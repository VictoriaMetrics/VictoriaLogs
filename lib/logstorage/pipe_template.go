package logstorage

import (
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/drain"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeTemplate processes '| template ...' queries.
//
// See https://docs.victoriametrics.com/victorialogs/logsql/#template-pipe
type pipeTemplate struct {
	// field is the field to template.
	field string

	// hitsFieldName is the name of the field for returning the number of hits per each template.
	// If hitsFieldName is empty, then hits aren't returned.
	hitsFieldName string

	// limit is the maximum number of templates to return.
	limit uint64
}

func (pt *pipeTemplate) String() string {
	s := "template"
	if pt.field != "_msg" {
		s += " at " + quoteTokenIfNeeded(pt.field)
	}
	if pt.hitsFieldName != "" {
		s += " with hits"
	}
	if pt.limit > 0 {
		s += fmt.Sprintf(" limit %d", pt.limit)
	}
	return s
}

func (pt *pipeTemplate) splitToRemoteAndLocal(timestamp int64) (pipe, []pipe) {
	return nil, []pipe{pt}
}

func (pt *pipeTemplate) canLiveTail() bool {
	return false
}

func (pt *pipeTemplate) canReturnLastNResults() bool {
	return false
}

func (pt *pipeTemplate) isFixedOutputFieldsOrder() bool {
	return true
}

func (pt *pipeTemplate) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter(pt.field)
}

func (pt *pipeTemplate) hasFilterInWithQuery() bool {
	return false
}

func (pt *pipeTemplate) initFilterInValues(_ *inValuesCache, _ getFieldValuesFunc, _ bool) (pipe, error) {
	return pt, nil
}

func (pt *pipeTemplate) visitSubqueries(_ func(q *Query)) {
	// nothing to do
}

func (pt *pipeTemplate) newPipeProcessor(concurrency int, stopCh <-chan struct{}, cancel func(), ppNext pipeProcessor) pipeProcessor {
	maxStateSize := int64(float64(memory.Allowed()) * 0.4)

	ptp := &pipeTemplateProcessor{
		pt:     pt,
		stopCh: stopCh,
		cancel: cancel,
		ppNext: ppNext,

		maxStateSize: maxStateSize,
	}
	ptp.shards.Init = func(shard *pipeTemplateProcessorShard) {
		shard.pt = pt
		shard.d = drain.New(drain.DefaultConfig(), &shard.stateSizeBudget)
	}
	ptp.stateSizeBudget.Store(maxStateSize)

	return ptp
}

type pipeTemplateProcessor struct {
	pt     *pipeTemplate
	stopCh <-chan struct{}
	cancel func()
	ppNext pipeProcessor

	shards atomicutil.Slice[pipeTemplateProcessorShard]

	maxStateSize    int64
	stateSizeBudget atomic.Int64
}

type pipeTemplateProcessorShard struct {
	pt *pipeTemplate
	d  *drain.Drain
	a  arena

	stateSizeBudget int
}

func (ptp *pipeTemplateProcessor) writeBlock(workerID uint, br *blockResult) {
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

	c := br.getColumnByName(ptp.pt.field)
	if c.isConst {
		v := c.valuesEncoded[0]
		vNormalized := ptp.normalizeValue(shard, v)
		shard.d.TrainWithHits(vNormalized, uint64(br.rowsLen))
		return
	}

	switch c.valueType {
	case valueTypeDict:
		c.forEachDictValueWithHits(br, func(v string, hits uint64) {
			vNormalized := ptp.normalizeValue(shard, v)
			shard.d.TrainWithHits(vNormalized, hits)
		})
	case valueTypeUint8:
		values := c.getValuesEncoded(br)
		for _, v := range values {
			n := unmarshalUint8(v)
			vStr := fmt.Sprintf("%d", n)
			vNormalized := ptp.normalizeValue(shard, vStr)
			shard.d.TrainWithHits(vNormalized, 1)
		}
	case valueTypeUint16:
		values := c.getValuesEncoded(br)
		for _, v := range values {
			n := unmarshalUint16(v)
			vStr := fmt.Sprintf("%d", n)
			vNormalized := ptp.normalizeValue(shard, vStr)
			shard.d.TrainWithHits(vNormalized, 1)
		}
	case valueTypeUint32:
		values := c.getValuesEncoded(br)
		for _, v := range values {
			n := unmarshalUint32(v)
			vStr := fmt.Sprintf("%d", n)
			vNormalized := ptp.normalizeValue(shard, vStr)
			shard.d.TrainWithHits(vNormalized, 1)
		}
	case valueTypeUint64:
		values := c.getValuesEncoded(br)
		for _, v := range values {
			n := unmarshalUint64(v)
			vStr := fmt.Sprintf("%d", n)
			vNormalized := ptp.normalizeValue(shard, vStr)
			shard.d.TrainWithHits(vNormalized, 1)
		}
	case valueTypeInt64:
		values := c.getValuesEncoded(br)
		for _, v := range values {
			n := unmarshalInt64(v)
			vStr := fmt.Sprintf("%d", n)
			vNormalized := ptp.normalizeValue(shard, vStr)
			shard.d.TrainWithHits(vNormalized, 1)
		}
	default:
		values := c.getValues(br)
		for _, v := range values {
			vNormalized := ptp.normalizeValue(shard, v)
			shard.d.TrainWithHits(vNormalized, 1)
		}
	}
}

func (ptp *pipeTemplateProcessor) normalizeValue(shard *pipeTemplateProcessorShard, v string) string {
	shard.a.reset()
	bLen := len(shard.a.b)
	shard.a.b = appendCollapseNums(shard.a.b, v)
	shard.a.b = appendPrettifyCollapsedNums(shard.a.b[:bLen], shard.a.b[bLen:])
	return bytesutil.ToUnsafeString(shard.a.b[bLen:])
}

func (ptp *pipeTemplateProcessor) flush() error {
	shards := ptp.shards.All()
	if len(shards) == 0 {
		return nil
	}

	// Train drain model by merging results from shards
	d := drain.New(drain.DefaultConfig(), nil)
	for _, shard := range shards {
		if needStop(ptp.stopCh) {
			return nil
		}
		d.Merge(shard.d)
	}

	clusters := d.Clusters()

	// Sort clusters by hits descending
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Size() > clusters[j].Size()
	})

	if limit := ptp.pt.limit; limit > 0 && uint64(len(clusters)) > limit {
		clusters = clusters[:limit]
	}

	// Write results
	wctx := &pipeTemplateWriteContext{
		ptp:    ptp,
		ppNext: ptp.ppNext,
	}

	var rowFields []Field
	for _, c := range clusters {
		if needStop(ptp.stopCh) {
			return nil
		}
		rowFields = append(rowFields[:0], Field{
			Name:  ptp.pt.field,
			Value: c.Template(),
		})
		if ptp.pt.hitsFieldName != "" {
			rowFields = append(rowFields, Field{
				Name:  ptp.pt.hitsFieldName,
				Value: wctx.getUint64String(uint64(c.Size())),
			})
		}
		wctx.writeRow(rowFields)
	}
	wctx.flush()

	return nil
}

type pipeTemplateWriteContext struct {
	ptp      *pipeTemplateProcessor
	workerID uint
	ppNext   pipeProcessor
	rcs      []resultColumn
	br       blockResult
	a        arena

	rowsCount int
	valuesLen int
}

func (wctx *pipeTemplateWriteContext) getUint64String(n uint64) string {
	bLen := len(wctx.a.b)
	wctx.a.b = marshalUint64String(wctx.a.b, n)
	return bytesutil.ToUnsafeString(wctx.a.b[bLen:])
}

func (wctx *pipeTemplateWriteContext) writeRow(rowFields []Field) {
	rcs := wctx.rcs
	if len(rcs) != len(rowFields) {
		wctx.flush()
		rcs = wctx.rcs[:0]
		for _, f := range rowFields {
			rcs = appendResultColumnWithName(rcs, f.Name)
		}
		wctx.rcs = rcs
	} else {
		for i, f := range rowFields {
			if rcs[i].name != f.Name {
				wctx.flush()
				rcs = wctx.rcs[:0]
				for _, f := range rowFields {
					rcs = appendResultColumnWithName(rcs, f.Name)
				}
				wctx.rcs = rcs
			}
		}
	}

	for i, f := range rowFields {
		v := f.Value
		rcs[i].addValue(v)
		wctx.valuesLen += len(v)
	}
	wctx.rowsCount++
	if wctx.valuesLen >= 64_000 {
		wctx.flush()
	}
}

func (wctx *pipeTemplateWriteContext) flush() {
	if wctx.rowsCount == 0 {
		return
	}
	wctx.br.setResultColumns(wctx.rcs, wctx.rowsCount)
	wctx.valuesLen = 0
	wctx.rowsCount = 0
	wctx.ppNext.writeBlock(wctx.workerID, &wctx.br)
	wctx.br.reset()
	for i := range wctx.rcs {
		wctx.rcs[i].resetValues()
	}
	wctx.a.reset()
}

func parsePipeTemplate(lex *lexer) (pipe, error) {
	if !lex.isKeyword("template") {
		return nil, fmt.Errorf("expecting 'template'; got %q", lex.token)
	}
	lex.nextToken()

	field := "_msg"
	if lex.isKeyword("at") {
		lex.nextToken()
		f, err := parseFieldName(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse 'at' field: %w", err)
		}
		field = f
	} else if !lex.isKeyword("with", "hits", "limit", "|", "") {
		// support template(field) or template field
		if lex.isKeyword("(") {
			lex.nextToken()
			f, err := parseFieldName(lex)
			if err != nil {
				return nil, fmt.Errorf("cannot parse field inside 'template(...)': %w", err)
			}
			field = f
			if !lex.isKeyword(")") {
				return nil, fmt.Errorf("expecting ')' after 'template(%s'; got %q", field, lex.token)
			}
			lex.nextToken()
		} else {
			f, err := parseFieldName(lex)
			if err != nil {
				return nil, fmt.Errorf("cannot parse field name: %w", err)
			}
			field = f
		}
	}

	pt := &pipeTemplate{
		field: field,
	}

	if lex.isKeyword("with") {
		lex.nextToken()
		if !lex.isKeyword("hits") {
			return nil, fmt.Errorf("expecting 'hits' after 'with'; got %q", lex.token)
		}
	}
	if lex.isKeyword("hits") {
		lex.nextToken()
		pt.hitsFieldName = "hits"
		if pt.field == "hits" {
			pt.hitsFieldName = "hits_count"
		}
	}

	if lex.isKeyword("limit") {
		n, err := parseLimit(lex)
		if err != nil {
			return nil, err
		}
		pt.limit = n
	}

	return pt, nil
}
