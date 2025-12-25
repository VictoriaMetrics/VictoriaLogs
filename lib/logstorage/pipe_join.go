package logstorage

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeJoin processes '| join ...' pipe.
//
// See https://docs.victoriametrics.com/victorialogs/logsql/#join-pipe
type pipeJoin struct {
	// byFields contains fields to use for join on q results
	byFields []string

	// q is a query for obtaining results for joining
	q *Query

	// The join is performed as INNER JOIN if isInner is set.
	// Otherwise the join is performed as LEFT JOIN.
	isInner bool

	// isLateral means the join query must be evaluated per row (or per key)
	// using values from the outer query. When enabled, pj.q isn't pre-executed,
	// but evaluated during processing. This is a checkpoint for the lateral join
	// implementation (see issue #635).
	isLateral bool
	// baseCtx is used as parent context for lateral subqueries.
	baseCtx context.Context

	// runSubQuery is set for lateral joins and allows executing the inner query
	// on demand during processing.
	runSubQuery func(ctx context.Context, q *Query, writeBlock writeBlockResultFunc) error

	// hasTimeFilter is true if q contains a time filter. Used to guard lateral subqueries.
	hasTimeFilter bool

	// limits for lateral execution
	maxSubqueries   int
	maxPerKey       int
	concurrency     int
	subQueryTimeout time.Duration

	// prefix is the prefix to add to log fields from q query
	prefix string

	// m contains results for joining. They are automatically initialized during query execution
	m map[string][][]Field
}

func (pj *pipeJoin) String() string {
	s := "join"
	if pj.isLateral {
		s += " lateral"
	}
	s = fmt.Sprintf("%s by (%s) (%s)", s, fieldNamesString(pj.byFields), pj.q.String())
	if pj.isInner {
		s += " inner"
	}
	if pj.prefix != "" {
		s += " prefix " + quoteTokenIfNeeded(pj.prefix)
	}
	if pj.isLateral {
		if opts := pj.lateralOptionsString(); opts != "" {
			s += " " + opts
		}
	}
	return s
}

func (pj *pipeJoin) lateralOptionsString() string {
	var parts []string
	if pj.maxSubqueries > 0 {
		parts = append(parts, fmt.Sprintf("max_subqueries=%d", pj.maxSubqueries))
	}
	if pj.maxPerKey > 0 {
		parts = append(parts, fmt.Sprintf("max_per_key=%d", pj.maxPerKey))
	}
	if pj.concurrency > 0 {
		parts = append(parts, fmt.Sprintf("concurrency=%d", pj.concurrency))
	}
	if pj.subQueryTimeout > 0 {
		parts = append(parts, fmt.Sprintf("timeout=%s", pj.subQueryTimeout))
	}
	if len(parts) == 0 {
		return ""
	}
	return "options(" + strings.Join(parts, ",") + ")"
}

func (pj *pipeJoin) splitToRemoteAndLocal(_ int64) (pipe, []pipe) {
	return nil, []pipe{pj}
}

func (pj *pipeJoin) canLiveTail() bool {
	return true
}

func (pj *pipeJoin) canReturnLastNResults() bool {
	return false
}

func (pj *pipeJoin) hasFilterInWithQuery() bool {
	// Do not check for in(...) filters at pj.q, since they are checked separately during pj.q execution.
	return false
}

func (pj *pipeJoin) initFilterInValues(_ *inValuesCache, _ getFieldValuesFunc, _ bool) (pipe, error) {
	// Do not init values for in(...) filters at pj.q, since they are initialized separately at initJoinMap.
	return pj, nil
}

func (pj *pipeJoin) visitSubqueries(visitFunc func(q *Query)) {
	pj.q.visitSubqueries(visitFunc)
}

func (pj *pipeJoin) initJoinMap(getJoinMapFunc getJoinMapFunc) (pipe, error) {
	if pj.isLateral {
		// Lateral join is evaluated during processing; skip precomputed map.
		pj.hasTimeFilter = hasTimeFilter(pj.q.f)
		return pj, nil
	}

	m, err := getJoinMapFunc(pj.q, pj.byFields, pj.prefix)
	if err != nil {
		return nil, fmt.Errorf("cannot execute query at pipe [%s]: %w", pj, err)
	}
	pjNew := *pj
	pjNew.m = m
	return &pjNew, nil
}

func (pj *pipeJoin) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilters(pj.byFields)
}

func (pj *pipeJoin) newPipeProcessor(_ int, stopCh <-chan struct{}, _ func(), ppNext pipeProcessor) pipeProcessor {
	return &pipeJoinProcessor{
		pj:     pj,
		stopCh: stopCh,
		ppNext: ppNext,
	}
}

type pipeJoinProcessor struct {
	pj     *pipeJoin
	stopCh <-chan struct{}
	ppNext pipeProcessor

	shards atomicutil.Slice[pipeJoinProcessorShard]

	// lateral cache & limits
	cacheMu   sync.Mutex
	cache     map[string][][]Field
	subqSem   chan struct{}
	subqCount int
}

type pipeJoinProcessorShard struct {
	wctx pipeUnpackWriteContext

	byValues     []string
	byValuesIdxs []int
	tmpBuf       []byte

	rowFieldMap map[string]string
}

func (pjp *pipeJoinProcessor) writeBlock(workerID uint, br *blockResult) {
	if br.rowsLen == 0 {
		return
	}

	if pjp.pj.isLateral {
		pjp.writeBlockLateral(workerID, br)
		return
	}

	pj := pjp.pj
	shard := pjp.shards.Get(workerID)
	shard.wctx.init(workerID, pjp.ppNext, true, true, br)

	shard.byValues = slicesutil.SetLength(shard.byValues, len(pj.byFields))
	byValues := shard.byValues

	cs := br.getColumns()
	shard.byValuesIdxs = slicesutil.SetLength(shard.byValuesIdxs, len(cs))
	byValuesIdxs := shard.byValuesIdxs
	for i := range cs {
		name := cs[i].name
		byValuesIdxs[i] = slices.Index(pj.byFields, name)

	}

	for rowIdx := 0; rowIdx < br.rowsLen; rowIdx++ {
		clear(byValues)
		for j := range cs {
			if cIdx := byValuesIdxs[j]; cIdx >= 0 {
				byValues[cIdx] = cs[j].getValueAtRow(br, rowIdx)
			}
		}

		shard.tmpBuf = marshalStrings(shard.tmpBuf[:0], byValues)
		matchingRows := pj.m[string(shard.tmpBuf)]

		if len(matchingRows) == 0 {
			if !pj.isInner {
				shard.wctx.writeRow(rowIdx, nil)
			}
			continue
		}
		for _, extraFields := range matchingRows {
			if needStop(pjp.stopCh) {
				return
			}
			shard.wctx.writeRow(rowIdx, extraFields)
		}
	}

	shard.wctx.flush()
	shard.wctx.reset()
}

func (pjp *pipeJoinProcessor) flush() error {
	return nil
}

// writeBlockLateral executes the join per key by running subqueries on demand.
func (pjp *pipeJoinProcessor) writeBlockLateral(workerID uint, br *blockResult) {
	pj := pjp.pj

	// init cache and semaphore once
	pjp.cacheMu.Lock()
	if pjp.cache == nil {
		pjp.cache = make(map[string][][]Field)
	}
	if pjp.subqSem == nil {
		conc := pj.concurrency
		if conc <= 0 {
			conc = 4
		}
		pjp.subqSem = make(chan struct{}, conc)
	}
	pjp.cacheMu.Unlock()

	shard := pjp.shards.Get(workerID)
	shard.wctx.init(workerID, pjp.ppNext, true, true, br)

	shard.byValues = slicesutil.SetLength(shard.byValues, len(pj.byFields))
	byValues := shard.byValues

	cs := br.getColumns()
	shard.byValuesIdxs = slicesutil.SetLength(shard.byValuesIdxs, len(cs))
	byValuesIdxs := shard.byValuesIdxs
	for i := range cs {
		name := cs[i].name
		byValuesIdxs[i] = slices.Index(pj.byFields, name)
	}

	for rowIdx := 0; rowIdx < br.rowsLen; rowIdx++ {
		clear(byValues)
		if shard.rowFieldMap == nil {
			shard.rowFieldMap = make(map[string]string)
		}
		for k := range shard.rowFieldMap {
			delete(shard.rowFieldMap, k)
		}
		for j := range cs {
			if cIdx := byValuesIdxs[j]; cIdx >= 0 {
				byValues[cIdx] = cs[j].getValueAtRow(br, rowIdx)
			}
			v := cs[j].getValueAtRow(br, rowIdx)
			if v != "" {
				shard.rowFieldMap[cs[j].name] = v
			}
		}

		matchingRows := pjp.getOrRunSubquery(byValues, shard.rowFieldMap)

		if len(matchingRows) == 0 {
			if !pj.isInner {
				shard.wctx.writeRow(rowIdx, nil)
			}
			continue
		}
		for _, extraFields := range matchingRows {
			if needStop(pjp.stopCh) {
				return
			}
			shard.wctx.writeRow(rowIdx, extraFields)
		}
	}

	shard.wctx.flush()
	shard.wctx.reset()
}

func (pjp *pipeJoinProcessor) getOrRunSubquery(byValues []string, outer map[string]string) [][]Field {
	pj := pjp.pj
	if pj.runSubQuery == nil {
		return nil
	}

	// lazy init for tests or direct calls
	pjp.cacheMu.Lock()
	if pjp.cache == nil {
		pjp.cache = make(map[string][][]Field)
	}
	if pjp.subqSem == nil {
		conc := pj.concurrency
		if conc <= 0 {
			conc = 4
		}
		pjp.subqSem = make(chan struct{}, conc)
	}
	pjp.cacheMu.Unlock()

	q, rendered, err := instantiateLateralQuery(pj.q, pj.byFields, byValues, outer)
	if err != nil || q == nil {
		return nil
	}
	if !hasTimeFilter(q.f) {
		// Guard: avoid unbounded scans for lateral subqueries without time filter.
		return nil
	}
	cacheKey := buildLateralCacheKey(rendered, byValues)

	// cache check by rendered query
	pjp.cacheMu.Lock()
	if rows, ok := pjp.cache[cacheKey]; ok {
		pjp.cacheMu.Unlock()
		return rows
	}
	pjp.cacheMu.Unlock()

	// limit total subqueries
	pjp.cacheMu.Lock()
	if pjp.pj.maxSubqueries > 0 && pjp.subqCount >= pjp.pj.maxSubqueries {
		pjp.cacheMu.Unlock()
		return nil
	}
	pjp.subqCount++
	pjp.cacheMu.Unlock()

	// concurrency gate
	pjp.subqSem <- struct{}{}
	defer func() { <-pjp.subqSem }()

	rows := pjp.runSubqueryForQuery(q, byValues)

	pjp.cacheMu.Lock()
	pjp.cache[cacheKey] = rows
	pjp.cacheMu.Unlock()

	return rows
}

func (pjp *pipeJoinProcessor) runSubqueryForQuery(q *Query, byValues []string) [][]Field {
	pj := pjp.pj
	if pj.runSubQuery == nil {
		return nil
	}

	// inject equality filters for byFields
	q.f = addAndFiltersJoin(q.f, pj.byFields, byValues)

	// limit rows per key
	limit := pj.maxPerKey
	if limit <= 0 {
		limit = 100
	}

	var results [][]Field
	var rowsSeen int
	writeBlock := func(_ uint, br *blockResult) {
		if br.rowsLen == 0 {
			return
		}
		cs := br.getColumns()
		for rowIdx := 0; rowIdx < br.rowsLen; rowIdx++ {
			if rowsSeen >= limit {
				return
			}
			rowsSeen++
			fields := make([]Field, 0, len(cs))
			for j := range cs {
				name := cs[j].name
				if pj.prefix != "" {
					name = pj.prefix + name
				} else {
					name = strings.Clone(name)
				}
				v := cs[j].getValueAtRow(br, rowIdx)
				if v == "" {
					continue
				}
				fields = append(fields, Field{
					Name:  name,
					Value: strings.Clone(v),
				})
			}
			results = append(results, fields)
		}
	}

	ctx := pj.baseCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if pj.subQueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pj.subQueryTimeout)
		defer cancel()
	}

	_ = pj.runSubQuery(ctx, q, writeBlock)

	return results
}

// addAndFiltersJoin builds an AND of existing filter and equality filters for byFields.
// Empty values are skipped.
func addAndFiltersJoin(base filter, byFields, byValues []string) filter {
	var fs []filter
	if base != nil {
		fs = append(fs, base)
	}
	for i, name := range byFields {
		if i >= len(byValues) {
			break
		}
		v := byValues[i]
		if v == "" {
			continue
		}
		fs = append(fs, &filterExact{
			fieldName: name,
			value:     v,
		})
	}
	return filtersAndJoin(fs)
}

// filtersAndJoin builds AND over provided filters, skipping nils.
func filtersAndJoin(fs []filter) filter {
	dst := fs[:0]
	for _, f := range fs {
		if f != nil {
			dst = append(dst, f)
		}
	}
	if len(dst) == 0 {
		return nil
	}
	if len(dst) == 1 {
		return dst[0]
	}
	return &filterAnd{filters: dst}
}

// hasTimeFilter checks whether f (possibly composite) contains time filter.
func hasTimeFilter(f filter) bool {
	switch t := f.(type) {
	case *filterTime:
		return true
	case *filterAnd:
		for _, ch := range t.filters {
			if hasTimeFilter(ch) {
				return true
			}
		}
	case *filterOr:
		for _, ch := range t.filters {
			if hasTimeFilter(ch) {
				return true
			}
		}
	case *filterNot:
		return hasTimeFilter(t.f)
	default:
		return false
	}
	return false
}

// instantiateLateralQuery renders q to string, replaces ${field} with value from byValues,
// and reparses it into a new Query. Falls back to nil on error.
func instantiateLateralQuery(q *Query, byFields []string, byValues []string, outer map[string]string) (*Query, string, error) {
	qs := q.String()

	// seed map with byFields
	if outer == nil {
		outer = make(map[string]string, len(byFields))
	}
	for i, name := range byFields {
		if i >= len(byValues) {
			break
		}
		if outer[name] == "" {
			outer[name] = byValues[i]
		}
	}

	rendered := renderPlaceholders(qs, outer)

	lex := newLexer(rendered, q.timestamp)
	qNew, err := parseQuery(lex)
	if err != nil {
		return nil, rendered, err
	}
	return qNew, rendered, nil
}

func buildLateralCacheKey(rendered string, byValues []string) string {
	var b strings.Builder
	// rough capacity hint
	b.Grow(len(rendered) + len(byValues)*8)
	b.WriteString(rendered)
	b.WriteByte('|')
	for _, v := range byValues {
		b.WriteString(strconv.Itoa(len(v)))
		b.WriteByte(':')
		b.WriteString(v)
		b.WriteByte(';')
	}
	return b.String()
}

// renderPlaceholders replaces ${field} with value from outer map, quoting as needed.
// Missing values are replaced with empty string.
func renderPlaceholders(s string, outer map[string]string) string {
	var b strings.Builder
	start := 0
	for {
		idx := strings.Index(s[start:], "${")
		if idx < 0 {
			break
		}
		idx += start
		b.WriteString(s[start:idx])
		end := strings.IndexByte(s[idx:], '}')
		if end < 0 {
			// no closing brace, write remainder
			b.WriteString(s[idx:])
			return b.String()
		}
		end += idx
		name := s[idx+2 : end]
		val := outer[name]
		b.WriteString(quoteTokenIfNeeded(val))
		start = end + 1
	}
	if start == 0 {
		return s
	}
	b.WriteString(s[start:])
	return b.String()
}

func parsePipeJoin(lex *lexer) (pipe, error) {
	if !lex.isKeyword("join") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "join")
	}
	lex.nextToken()

	isLateral := false
	if lex.isKeyword("lateral") {
		isLateral = true
		lex.nextToken()
	}

	// parse by (...)
	if lex.isKeyword("by", "on") {
		lex.nextToken()
	}

	byFields, err := parseFieldNamesInParens(lex)
	if err != nil {
		return nil, fmt.Errorf("cannot parse 'by(...)' at 'join': %w", err)
	}
	if len(byFields) == 0 {
		return nil, fmt.Errorf("'by(...)' at 'join' must contain at least a single field")
	}
	if slices.Contains(byFields, "*") {
		return nil, fmt.Errorf("join by '*' isn't supported")
	}

	// Parse join query
	q, err := parseQueryInParens(lex)
	if err != nil {
		return nil, fmt.Errorf("cannot parse join(...) query: %w", err)
	}

	pj := &pipeJoin{
		byFields:  byFields,
		q:         q,
		isLateral: isLateral,
	}

	parsedInner := false
	parsedPrefix := false
	parsedOptions := false
	for {
		switch {
		case !parsedInner && lex.isKeyword("inner"):
			parsedInner = true
			lex.nextToken()
			pj.isInner = true
			continue
		case !parsedPrefix && lex.isKeyword("prefix"):
			parsedPrefix = true
			lex.nextToken()
			prefix, err := lex.nextCompoundToken()
			if err != nil {
				return nil, fmt.Errorf("cannot read prefix for [%s]: %w", pj, err)
			}
			pj.prefix = prefix
			continue
		case !parsedOptions && lex.isKeyword("options"):
			parsedOptions = true
			lex.nextToken()
			if !lex.isKeyword("(") {
				return nil, fmt.Errorf("missing '(' after options at [%s]", pj)
			}
			lex.nextToken()
			for !lex.isKeyword(")") {
				switch {
				case lex.isKeyword("max_subqueries"):
					lex.nextToken()
					if lex.isKeyword("=") {
						lex.nextToken()
					}
					n, err := parseIntToken(lex)
					if err != nil {
						return nil, fmt.Errorf("cannot parse max_subqueries for [%s]: %w", pj, err)
					}
					pj.maxSubqueries = n
				case lex.isKeyword("max_per_key"):
					lex.nextToken()
					if lex.isKeyword("=") {
						lex.nextToken()
					}
					n, err := parseIntToken(lex)
					if err != nil {
						return nil, fmt.Errorf("cannot parse max_per_key for [%s]: %w", pj, err)
					}
					pj.maxPerKey = n
				case lex.isKeyword("concurrency"):
					lex.nextToken()
					if lex.isKeyword("=") {
						lex.nextToken()
					}
					n, err := parseIntToken(lex)
					if err != nil {
						return nil, fmt.Errorf("cannot parse concurrency for [%s]: %w", pj, err)
					}
					pj.concurrency = n
				case lex.isKeyword("timeout"):
					lex.nextToken()
					if lex.isKeyword("=") {
						lex.nextToken()
					}
					d, err := parseDurationToken(lex)
					if err != nil {
						return nil, fmt.Errorf("cannot parse timeout for [%s]: %w", pj, err)
					}
					pj.subQueryTimeout = d
				default:
					return nil, fmt.Errorf("unexpected token %q in options for [%s]", lex.token, pj)
				}
				if lex.isKeyword(",") {
					lex.nextToken()
				} else if !lex.isKeyword(")") {
					return nil, fmt.Errorf("unexpected token %q in options for [%s]", lex.token, pj)
				}
			}
			lex.nextToken()
			continue
		default:
			goto doneOptions
		}
	}
doneOptions:
	return pj, nil
}

func parseIntToken(lex *lexer) (int, error) {
	tok, err := lex.nextCompoundToken()
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(tok)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func parseDurationToken(lex *lexer) (time.Duration, error) {
	tok, err := lex.nextCompoundToken()
	if err != nil {
		return 0, err
	}
	d, ok := tryParseDuration(tok)
	if !ok {
		return 0, fmt.Errorf("cannot parse duration %q", tok)
	}
	return time.Duration(d), nil
}
