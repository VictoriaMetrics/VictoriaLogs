package logstorage

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeDeduplicate processes '| deduplicate ...' queries.
//
// See https://docs.victoriametrics.com/victorialogs/logsql/#deduplicate-pipe
type pipeDeduplicate struct {
	// byFields contains field names for detecting duplicate log entries.
	byFields []string
}

func (pd *pipeDeduplicate) String() string {
	s := "deduplicate"
	if len(pd.byFields) > 0 {
		s += " by (" + fieldNamesString(pd.byFields) + ")"
	}
	return s
}

func (pd *pipeDeduplicate) splitToRemoteAndLocal(_ int64) (pipe, []pipe) {
	return pd, []pipe{pd}
}

func (pd *pipeDeduplicate) canLiveTail() bool {
	return false
}

func (pd *pipeDeduplicate) canReturnLastNResults() bool {
	return false
}

func (pd *pipeDeduplicate) isFixedOutputFieldsOrder() bool {
	return false
}

func (pd *pipeDeduplicate) updateNeededFields(pf *prefixfilter.Filter) {
	if len(pd.byFields) == 0 {
		pf.AddAllowFilter("*")
	} else {
		pf.AddAllowFilters(pd.byFields)
	}
}

func (pd *pipeDeduplicate) hasFilterInWithQuery() bool {
	return false
}

func (pd *pipeDeduplicate) initFilterInValues(_ *inValuesCache, _ getFieldValuesFunc) (pipe, error) {
	return pd, nil
}

func (pd *pipeDeduplicate) visitSubqueries(_ func(q *Query)) {
	// nothing to do
}

func (pd *pipeDeduplicate) newPipeProcessor(concurrency int, _ <-chan struct{}, cancel func(), ppNext pipeProcessor) pipeProcessor {
	maxStateSize := int64(float64(memory.Allowed()) * 0.4)

	pdp := &pipeDeduplicateProcessor{
		pd:     pd,
		cancel: cancel,
		ppNext: ppNext,

		keySetShards: make([]pipeDeduplicateKeySetShard, concurrency),

		maxStateSize: maxStateSize,
	}
	pdp.shards.Init = func(shard *pipeDeduplicateProcessorShard) {
		shard.pd = pd
		shard.rowIdxss = make([][]int, concurrency)
	}
	pdp.stateSizeBudget.Store(maxStateSize)
	return pdp
}

type pipeDeduplicateProcessor struct {
	pd     *pipeDeduplicate
	cancel func()
	ppNext pipeProcessor

	shards atomicutil.Slice[pipeDeduplicateProcessorShard]

	// keySetShards hold keys of the already returned log entries.
	keySetShards []pipeDeduplicateKeySetShard

	stateSizeBudget atomic.Int64
	maxStateSize    int64
}

type pipeDeduplicateProcessorShard struct {
	// pd points to the parent pipeDeduplicate.
	pd *pipeDeduplicate

	cs           []*blockResultColumn
	columnValues [][]string

	keysBuf []byte
	keyEnds []int

	// rowIdxss holds row indexes grouped by target keySet shard.
	rowIdxss [][]int

	// a is used for reducing memory allocations when registering dedup keys.
	a chunkedAllocator

	bm bitmap
	br blockResult
}

type pipeDeduplicateKeySetShard struct {
	pipeDeduplicateKeySet

	// The padding prevents false sharing
	_ [atomicutil.CacheLineSize - unsafe.Sizeof(pipeDeduplicateKeySet{})%atomicutil.CacheLineSize]byte
}

type pipeDeduplicateKeySet struct {
	mu sync.Mutex

	keys map[string]struct{}
}

func (shard *pipeDeduplicateProcessorShard) updateKeys(br *blockResult) {
	byFields := shard.pd.byFields

	cs := shard.cs[:0]
	if len(byFields) > 0 {
		for _, f := range byFields {
			cs = append(cs, br.getColumnByName(f))
		}
	} else {
		// Sort columns by name, so the key doesn't depend on column order within the block.
		cs = append(cs, br.getColumns()...)
		sort.Slice(cs, func(i, j int) bool {
			return cs[i].name < cs[j].name
		})
	}
	shard.cs = cs

	columnValues := shard.columnValues[:0]
	for _, c := range cs {
		columnValues = append(columnValues, c.getValues(br))
	}
	shard.columnValues = columnValues

	// No by(...) fields means keys need names too, since blocks may carry different field sets.
	needFieldNames := len(byFields) == 0

	keysBuf := shard.keysBuf[:0]
	keyEnds := shard.keyEnds[:0]
	for i := range br.rowsLen {
		for j, values := range columnValues {
			v := values[i]
			if needFieldNames {
				if v == "" {
					// Skip empty values
					continue
				}
				keysBuf = encoding.MarshalBytes(keysBuf, bytesutil.ToUnsafeBytes(cs[j].name))
			}
			keysBuf = encoding.MarshalBytes(keysBuf, bytesutil.ToUnsafeBytes(v))
		}
		keyEnds = append(keyEnds, len(keysBuf))
	}
	shard.keysBuf = keysBuf
	shard.keyEnds = keyEnds
}

func (pdp *pipeDeduplicateProcessor) writeBlock(workerID uint, br *blockResult) {
	if br.rowsLen == 0 {
		return
	}

	shard := pdp.shards.Get(workerID)
	shard.updateKeys(br)

	bm := &shard.bm
	bm.init(br.rowsLen)

	if pdp.addKeys(shard, bm) <= 0 {
		// The state size is too big. Stop processing data in order to avoid OOM crash.
		pdp.cancel()
		return
	}

	if bm.areAllBitsSet() {
		// Fast path - there are no duplicate rows at br - send it to the next pipe as is.
		pdp.ppNext.writeBlock(workerID, br)
		return
	}
	if bm.isZero() {
		// All the rows at br are duplicates. Nothing to send.
		return
	}

	// Slow path - copy the non-duplicate rows from br to shard.br before sending them to the next pipe.
	shard.br.initFromFilterAllColumns(br, bm)
	pdp.ppNext.writeBlock(workerID, &shard.br)
}

func (pdp *pipeDeduplicateProcessor) addKeys(shard *pipeDeduplicateProcessorShard, bm *bitmap) int64 {
	keysBuf := shard.keysBuf
	keyEnds := shard.keyEnds

	rowIdxss := shard.rowIdxss
	for i := range rowIdxss {
		rowIdxss[i] = rowIdxss[i][:0]
	}
	if len(rowIdxss) == 1 {
		// Fast path - a single shard, so there is no need in calculating key hashes.
		rowIdxs := rowIdxss[0]
		for i := range keyEnds {
			rowIdxs = append(rowIdxs, i)
		}
		rowIdxss[0] = rowIdxs
	} else {
		keyStart := 0
		for i, keyEnd := range keyEnds {
			key := keysBuf[keyStart:keyEnd]
			keyStart = keyEnd

			idx := xxhash.Sum64(key) % uint64(len(rowIdxss))
			rowIdxss[idx] = append(rowIdxss[idx], i)
		}
	}

	stateSize := int64(0)
	for i := range rowIdxss {
		if rowIdxs := rowIdxss[i]; len(rowIdxs) > 0 {
			stateSize += pdp.keySetShards[i].addKeys(&shard.a, keysBuf, keyEnds, rowIdxs, bm)
		}
	}

	return pdp.stateSizeBudget.Add(-stateSize)
}

func (ks *pipeDeduplicateKeySet) addKeys(a *chunkedAllocator, keysBuf []byte, keyEnds []int, rowIdxs []int, bm *bitmap) int64 {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	if ks.keys == nil {
		ks.keys = make(map[string]struct{})
	}

	stateSize := int64(0)
	for _, rowIdx := range rowIdxs {
		keyStart := 0
		if rowIdx > 0 {
			keyStart = keyEnds[rowIdx-1]
		}
		key := keysBuf[keyStart:keyEnds[rowIdx]]

		if _, ok := ks.keys[bytesutil.ToUnsafeString(key)]; ok {
			continue
		}
		keyCopy := a.cloneBytesToString(key)
		ks.keys[keyCopy] = struct{}{}
		stateSize += int64(len(keyCopy)) + int64(unsafe.Sizeof(keyCopy))
		bm.setBit(rowIdx)
	}

	return stateSize
}

func (pdp *pipeDeduplicateProcessor) flush() error {
	if pdp.stateSizeBudget.Load() <= 0 {
		return fmt.Errorf("cannot calculate [%s], since it requires more than %dMB of memory", pdp.pd.String(), pdp.maxStateSize/(1<<20))
	}
	return nil
}

func parsePipeDeduplicate(lex *lexer) (pipe, error) {
	if !lex.isKeyword("deduplicate") {
		return nil, fmt.Errorf("expecting 'deduplicate'; got %q", lex.token)
	}
	lex.nextToken()

	needFields := false
	if lex.isKeyword("by") {
		lex.nextToken()
		needFields = true
	}

	var byFields []string
	if lex.isKeyword("(") {
		bfs, err := parseFieldNamesInParens(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse 'by(...)': %w", err)
		}
		if len(bfs) == 0 {
			return nil, fmt.Errorf("missing fields inside 'by(...)'")
		}
		byFields = bfs
	} else if !lex.isQueryPartTrailer() {
		bfs, err := parseCommaSeparatedFieldNames(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse 'by ...': %w", err)
		}
		byFields = bfs
	} else if needFields {
		return nil, fmt.Errorf("missing fields after 'by'")
	}

	pd := &pipeDeduplicate{
		byFields: byFields,
	}
	return pd, nil
}
