package logstorage

import (
	"container/heap"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeTopOverTimeDefaultLimit is the default number of entries per time bucket returned by pipeTopOverTime.
const pipeTopOverTimeDefaultLimit = 10

// pipeTopOverTime processes '| top_over_time ...' and '| bottom_over_time ...' queries.
//
// It returns top or bottom N field sets for every time bucket.
type pipeTopOverTime struct {
	byFields []string

	limit    uint64
	limitStr string

	hitsFieldName string
	rankFieldName string

	timeField byStatsField

	isBottom bool
}

func (pt *pipeTopOverTime) String() string {
	name := "top_over_time"
	if pt.isBottom {
		name = "bottom_over_time"
	}

	s := fmt.Sprintf("%s step %s", name, pt.timeField.bucketSizeStr)
	if pt.timeField.bucketOffsetStr != "" {
		s += " offset " + pt.timeField.bucketOffsetStr
	}
	if pt.limit != pipeTopOverTimeDefaultLimit {
		s += " " + pt.limitStr
	}
	s += " by (" + fieldNamesString(pt.byFields) + ")"
	if pt.hitsFieldName != "hits" {
		s += " hits as " + quoteTokenIfNeeded(pt.hitsFieldName)
	}
	if pt.rankFieldName != "" {
		s += rankFieldNameString(pt.rankFieldName)
	}
	return s
}

func (pt *pipeTopOverTime) splitToRemoteAndLocal(_ int64) (pipe, []pipe) {
	// Execute locally. Remote execution would require shipping bucketed stats back anyway.
	return nil, []pipe{pt}
}

func (pt *pipeTopOverTime) canLiveTail() bool {
	return false
}

func (pt *pipeTopOverTime) canReturnLastNResults() bool {
	return false
}

func (pt *pipeTopOverTime) updateNeededFields(pf *prefixfilter.Filter) {
	pf.Reset()
	pf.AddAllowFilter("_time")
	pf.AddAllowFilters(pt.byFields)
}

func (pt *pipeTopOverTime) hasFilterInWithQuery() bool {
	return false
}

func (pt *pipeTopOverTime) initFilterInValues(_ *inValuesCache, _ getFieldValuesFunc, _ bool) (pipe, error) {
	return pt, nil
}

func (pt *pipeTopOverTime) visitSubqueries(_ func(q *Query)) {
	// nothing to do
}

func (pt *pipeTopOverTime) newPipeProcessor(concurrency int, stopCh <-chan struct{}, cancel func(), ppNext pipeProcessor) pipeProcessor {
	maxStateSize := int64(float64(memory.Allowed()) * 0.4)

	ptp := &pipeTopOverTimeProcessor{
		pt:     pt,
		stopCh: stopCh,
		cancel: cancel,
		ppNext: ppNext,

		maxStateSize: maxStateSize,
	}
	ptp.shards.Init = func(shard *pipeTopOverTimeProcessorShard) {
		shard.pt = pt
		shard.m.init(uint(concurrency), &shard.stateSizeBudget)
	}
	ptp.stateSizeBudget.Store(maxStateSize)

	return ptp
}

type pipeTopOverTimeProcessor struct {
	pt     *pipeTopOverTime
	stopCh <-chan struct{}
	cancel func()
	ppNext pipeProcessor

	shards atomicutil.Slice[pipeTopOverTimeProcessorShard]

	maxStateSize    int64
	stateSizeBudget atomic.Int64
}

type pipeTopOverTimeProcessorShard struct {
	pt *pipeTopOverTime

	m hitsMapAdaptive

	keyBuf       []byte
	columnValues [][]string
	timeValues   []string

	stateSizeBudget int
}

func (ptp *pipeTopOverTimeProcessor) writeBlock(workerID uint, br *blockResult) {
	if br.rowsLen == 0 {
		return
	}

	shard := ptp.shards.Get(workerID)

	for shard.stateSizeBudget < 0 {
		remaining := ptp.stateSizeBudget.Add(-stateSizeBudgetChunk)
		if remaining < 0 {
			if remaining+stateSizeBudgetChunk >= 0 {
				ptp.cancel()
			}
			return
		}
		shard.stateSizeBudget += stateSizeBudgetChunk
	}

	shard.writeBlock(br)
}

func (shard *pipeTopOverTimeProcessorShard) writeBlock(br *blockResult) {
	timeValues := br.getBucketedTimestampValues(&shard.pt.timeField)
	shard.timeValues = timeValues

	byFields := shard.pt.byFields
	columnValues := shard.columnValues[:0]
	for _, f := range byFields {
		c := br.getColumnByName(f)
		values := c.getValues(br)
		columnValues = append(columnValues, values)
	}
	shard.columnValues = columnValues

	keyBuf := shard.keyBuf
	hits := uint64(1)
	for rowIdx := 1; rowIdx < br.rowsLen; rowIdx++ {
		if timeValues[rowIdx-1] == timeValues[rowIdx] && isEqualPrevRow(columnValues, rowIdx) {
			hits++
			continue
		}
		keyBuf = shard.appendKey(keyBuf, timeValues[rowIdx-1], columnValues, rowIdx-1)
		shard.m.updateStateString(keyBuf, hits)
		hits = 1
	}
	keyBuf = shard.appendKey(keyBuf, timeValues[len(timeValues)-1], columnValues, br.rowsLen-1)
	shard.m.updateStateString(keyBuf, hits)
	shard.keyBuf = keyBuf
}

func (shard *pipeTopOverTimeProcessorShard) appendKey(dst []byte, bucket string, columnValues [][]string, rowIdx int) []byte {
	dst = dst[:0]
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(bucket))
	for _, values := range columnValues {
		dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(values[rowIdx]))
	}
	return dst
}

func (ptp *pipeTopOverTimeProcessor) flush() error {
	if n := ptp.stateSizeBudget.Load(); n <= 0 {
		return fmt.Errorf("cannot calculate [%s], since it requires more than %dMB of memory", ptp.pt.String(), ptp.maxStateSize/(1<<20))
	}

	buckets := ptp.mergeShardsParallel()
	if needStop(ptp.stopCh) {
		return nil
	}

	wctx := &pipeTopOverTimeWriteContext{
		ptp: ptp,
	}

	var rowFields []Field
	bucketNames := make([]string, 0, len(buckets))
	for bucket := range buckets {
		bucketNames = append(bucketNames, bucket)
	}
	sort.Strings(bucketNames)

	for _, bucket := range bucketNames {
		entries := buckets[bucket]
		if len(entries) == 0 {
			continue
		}
		if ptp.pt.isBottom {
			sort.Slice(entries, func(i, j int) bool {
				a := entries[i]
				b := entries[j]
				if a.hits == b.hits {
					return a.k < b.k
				}
				return a.hits < b.hits
			})
		} else {
			sort.Slice(entries, func(i, j int) bool {
				return entries[j].less(entries[i])
			})
		}

		for i, e := range entries {
			if needStop(ptp.stopCh) {
				return nil
			}

			rowFields = rowFields[:0]
			rowFields = append(rowFields, Field{
				Name:  "_time",
				Value: bucket,
			})

			keyBuf := bytesutil.ToUnsafeBytes(e.k)
			_, nSize := encoding.UnmarshalBytes(keyBuf)
			if nSize <= 0 {
				logger.Panicf("BUG: cannot unmarshal time bucket")
			}
			keyBuf = keyBuf[nSize:]

			for _, fieldName := range ptp.pt.byFields {
				value, nSize := encoding.UnmarshalBytes(keyBuf)
				if nSize <= 0 {
					logger.Panicf("BUG: cannot unmarshal field value")
				}
				keyBuf = keyBuf[nSize:]
				rowFields = append(rowFields, Field{
					Name:  fieldName,
					Value: bytesutil.ToUnsafeString(value),
				})
			}

			hitsStr := string(marshalUint64String(nil, e.hits))
			rowFields = append(rowFields, Field{
				Name:  ptp.pt.hitsFieldName,
				Value: hitsStr,
			})
			if ptp.pt.rankFieldName != "" {
				rowFields = append(rowFields, Field{
					Name:  ptp.pt.rankFieldName,
					Value: strconv.Itoa(i + 1),
				})
			}
			wctx.writeRow(rowFields)
		}
	}

	wctx.flush()

	return nil
}

func (ptp *pipeTopOverTimeProcessor) mergeShardsParallel() map[string][]*pipeTopEntry {
	limit := ptp.pt.limit
	if limit == 0 {
		return nil
	}

	shards := ptp.shards.All()
	if len(shards) == 0 {
		return nil
	}

	hmas := make([]*hitsMapAdaptive, 0, len(shards))
	for i := range shards {
		hma := &shards[i].m
		if hma.entriesCount() > 0 {
			hmas = append(hmas, hma)
		}
	}

	if len(hmas) == 0 {
		return nil
	}

	buckets := make(map[string]*bucketEntriesHeap)
	var bucketsLock sync.Mutex

	hitsMapMergeParallel(hmas, ptp.stopCh, func(hm *hitsMap) {
		if needStop(ptp.stopCh) {
			return
		}
		addBucketsFromHitsMap(buckets, &bucketsLock, hm, limit, ptp.pt.isBottom, ptp.stopCh)
	})
	if needStop(ptp.stopCh) {
		return nil
	}

	result := make(map[string][]*pipeTopEntry, len(buckets))
	for bucket, heap := range buckets {
		entries := heapToSortedEntries(heap, ptp.pt.isBottom)
		if len(entries) > 0 {
			result[bucket] = entries
		}
	}
	return result
}

type bucketEntriesHeap struct {
	isBottom bool
	topHeap  topEntriesHeap
	botHeap  bottomEntriesHeap
}

func (beh *bucketEntriesHeap) push(e *pipeTopEntry, limit uint64) {
	if limit == 0 {
		return
	}
	if !beh.isBottom {
		if uint64(len(beh.topHeap)) < limit {
			eCopy := *e
			heap.Push(&beh.topHeap, &eCopy)
			return
		}
		if !beh.topHeap[0].less(e) {
			return
		}
		eCopy := *e
		beh.topHeap[0] = &eCopy
		heap.Fix(&beh.topHeap, 0)
		return
	}

	if uint64(len(beh.botHeap)) < limit {
		eCopy := *e
		heap.Push(&beh.botHeap, &eCopy)
		return
	}
	root := beh.botHeap[0]
	if root.hits < e.hits {
		return
	}
	if root.hits == e.hits && root.k <= e.k {
		return
	}
	eCopy := *e
	beh.botHeap[0] = &eCopy
	heap.Fix(&beh.botHeap, 0)
}

func heapToSortedEntries(beh *bucketEntriesHeap, isBottom bool) []*pipeTopEntry {
	if beh == nil {
		return nil
	}
	if !isBottom {
		eh := beh.topHeap
		result := ([]*pipeTopEntry)(eh)
		for len(eh) > 0 {
			x := heap.Pop(&eh)
			result[len(eh)] = x.(*pipeTopEntry)
		}
		return result
	}

	eh := beh.botHeap
	result := ([]*pipeTopEntry)(eh)
	for len(eh) > 0 {
		x := heap.Pop(&eh)
		result[len(eh)] = x.(*pipeTopEntry)
	}
	return result
}

func addBucketsFromHitsMap(dst map[string]*bucketEntriesHeap, dstLock *sync.Mutex, hm *hitsMap, limit uint64, isBottom bool, stopCh <-chan struct{}) {
	for k, pHits := range hm.strings {
		if needStop(stopCh) {
			return
		}
		keyBuf := bytesutil.ToUnsafeBytes(k)
		bucketBytes, nSize := encoding.UnmarshalBytes(keyBuf) // first chunk is the bucket; skip it to reach grouped fields
		if nSize <= 0 {
			logger.Panicf("BUG: cannot unmarshal time bucket")
		}
		bucket := bytesutil.ToUnsafeString(bucketBytes)

		dstLock.Lock()
		beh := dst[bucket]
		if beh == nil {
			beh = &bucketEntriesHeap{
				isBottom: isBottom,
			}
			dst[bucket] = beh
		}
		dstLock.Unlock()

		e := &pipeTopEntry{
			k:    k,
			hits: *pHits,
		}
		beh.push(e, limit)
	}
}

type bottomEntriesHeap []*pipeTopEntry

func (h bottomEntriesHeap) Less(i, j int) bool {
	return h[i].lessBottom(h[j])
}
func (h bottomEntriesHeap) Swap(i, j int) {
	a := h
	a[i], a[j] = a[j], a[i]
}
func (h bottomEntriesHeap) Len() int {
	return len(h)
}
func (h *bottomEntriesHeap) Push(x any) {
	*h = append(*h, x.(*pipeTopEntry))
}
func (h *bottomEntriesHeap) Pop() any {
	a := *h
	x := a[len(a)-1]
	a[len(a)-1] = nil
	*h = a[:len(a)-1]
	return x
}

func (e *pipeTopEntry) lessBottom(r *pipeTopEntry) bool {
	if e.hits == r.hits {
		return e.k < r.k
	}
	return e.hits > r.hits
}

type pipeTopOverTimeWriteContext struct {
	ptp *pipeTopOverTimeProcessor
	rcs []resultColumn
	br  blockResult

	rowsCount int
	valuesLen int
}

func (wctx *pipeTopOverTimeWriteContext) ensureColumns() {
	if len(wctx.rcs) > 0 {
		return
	}
	rcs := wctx.rcs[:0]
	rcs = appendResultColumnWithName(rcs, "_time")
	for _, f := range wctx.ptp.pt.byFields {
		rcs = appendResultColumnWithName(rcs, f)
	}
	rcs = appendResultColumnWithName(rcs, wctx.ptp.pt.hitsFieldName)
	if wctx.ptp.pt.rankFieldName != "" {
		rcs = appendResultColumnWithName(rcs, wctx.ptp.pt.rankFieldName)
	}
	wctx.rcs = rcs
}

func (wctx *pipeTopOverTimeWriteContext) writeRow(rowFields []Field) {
	wctx.ensureColumns()

	rcs := wctx.rcs
	if len(rcs) != len(rowFields) {
		logger.Panicf("BUG: unexpected number of columns; got %d; want %d", len(rowFields), len(rcs))
	}
	for i := range rcs {
		rcs[i].addValue(rowFields[i].Value)
		wctx.valuesLen += len(rowFields[i].Value)
	}
	wctx.rowsCount++

	if wctx.valuesLen >= 64_000 {
		wctx.flush()
	}
}

func (wctx *pipeTopOverTimeWriteContext) flush() {
	rcs := wctx.rcs
	br := &wctx.br

	wctx.valuesLen = 0

	br.setResultColumns(rcs, wctx.rowsCount)
	wctx.rowsCount = 0
	wctx.ptp.ppNext.writeBlock(0, br)
	br.reset()
	for i := range rcs {
		rcs[i].resetValues()
	}
}

func parsePipeTopOverTime(lex *lexer) (pipe, error) {
	return parsePipeTopBottomOverTime(lex, false)
}

func parsePipeBottomOverTime(lex *lexer) (pipe, error) {
	return parsePipeTopBottomOverTime(lex, true)
}

func parsePipeTopBottomOverTime(lex *lexer, isBottom bool) (pipe, error) {
	name := "top_over_time"
	if isBottom {
		name = "bottom_over_time"
	}
	if !lex.isKeyword(name) {
		return nil, fmt.Errorf("expecting %q; got %q", name, lex.token)
	}
	lex.nextToken()

	if !lex.isKeyword("step") {
		return nil, fmt.Errorf("missing 'step' for %s", name)
	}
	lex.nextToken()

	stepStr, err := lex.nextCompoundToken()
	if err != nil {
		return nil, fmt.Errorf("cannot parse step: %w", err)
	}
	bf := byStatsField{
		name:          "_time",
		bucketSizeStr: stepStr,
	}
	if stepStr != "year" && stepStr != "month" {
		step, ok := tryParseBucketSize(stepStr)
		if !ok {
			return nil, fmt.Errorf("cannot parse step for %s: %q", name, stepStr)
		}
		bf.bucketSize = step
	}

	if lex.isKeyword("offset") {
		lex.nextToken()

		offsetStr, err := lex.nextCompoundToken()
		if err != nil {
			return nil, fmt.Errorf("cannot parse offset for %s: %w", name, err)
		}
		offset, ok := tryParseBucketOffset(offsetStr)
		if !ok {
			return nil, fmt.Errorf("cannot parse offset for %s: %q", name, offsetStr)
		}
		bf.bucketOffsetStr = offsetStr
		bf.bucketOffset = offset
	}

	limit := uint64(pipeTopOverTimeDefaultLimit)
	limitStr := ""
	if isNumberPrefix(lex.token) {
		limitF, s, err := parseNumber(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse N in %s: %w", name, err)
		}
		if limitF < 1 {
			return nil, fmt.Errorf("value N in '%s %s' must be integer bigger than 0", name, s)
		}
		limit = uint64(limitF)
		limitStr = s
	}

	if !lex.isKeyword("by") {
		return nil, fmt.Errorf("missing 'by' clause in %s", name)
	}
	lex.nextToken()

	var byFields []string
	if lex.isKeyword("(") {
		bfs, err := parseFieldNamesInParens(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse 'by(...)': %w", err)
		}
		byFields = bfs
	} else {
		bfs, err := parseCommaSeparatedFields(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse 'by ...': %w", err)
		}
		byFields = bfs
	}
	if len(byFields) == 0 {
		return nil, fmt.Errorf("expecting at least a single field in 'by(...)'")
	}
	for _, f := range byFields {
		if f == "_time" {
			return nil, fmt.Errorf("the field '_time' cannot be used in 'by(...)' for %s", name)
		}
	}

	pt := &pipeTopOverTime{
		byFields:      byFields,
		limit:         limit,
		limitStr:      limitStr,
		hitsFieldName: "hits",
		timeField:     bf,
		isBottom:      isBottom,
	}

	for {
		switch {
		case lex.isKeyword("hits"):
			lex.nextToken()
			if lex.isKeyword("as") {
				lex.nextToken()
			}
			s, err := lex.nextCompoundToken()
			if err != nil {
				return nil, fmt.Errorf("cannot parse 'hits' name: %w", err)
			}
			pt.hitsFieldName = s
		case lex.isKeyword("rank"):
			rankFieldName, err := parseRankFieldName(lex)
			if err != nil {
				return nil, fmt.Errorf("cannot parse rank field name in [%s]: %w", pt, err)
			}
			pt.rankFieldName = rankFieldName
			for slices.Contains(byFields, pt.rankFieldName) || pt.rankFieldName == "_time" || pt.rankFieldName == pt.hitsFieldName {
				pt.rankFieldName += "s"
			}
		default:
			for slices.Contains(byFields, pt.hitsFieldName) || pt.hitsFieldName == "_time" {
				pt.hitsFieldName += "s"
			}
			return pt, nil
		}
	}
}
