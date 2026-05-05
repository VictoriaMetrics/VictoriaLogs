package logstorage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/valyala/fastjson"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeJSONArrayValues processes '| json_array_values ...' pipe.
//
// See https://docs.victoriametrics.com/victorialogs/logsql/#json_array_values-pipe
type pipeJSONArrayValues struct {
	// elementField is the field to extract from every object in the source JSON array.
	elementField string

	// fromField is the source field that holds a JSON array of objects.
	fromField string

	// resultField is the field to store the resulting JSON array of extracted values.
	resultField string
}

func (pv *pipeJSONArrayValues) String() string {
	s := "json_array_values " + quoteTokenIfNeeded(pv.elementField)
	if !isMsgFieldName(pv.fromField) {
		s += " from " + quoteTokenIfNeeded(pv.fromField)
	}
	if !isMsgFieldName(pv.resultField) {
		s += " as " + quoteTokenIfNeeded(pv.resultField)
	}
	return s
}

func (pv *pipeJSONArrayValues) splitToRemoteAndLocal(_ int64) (pipe, []pipe) {
	return pv, nil
}

func (pv *pipeJSONArrayValues) canLiveTail() bool {
	return true
}

func (pv *pipeJSONArrayValues) canReturnLastNResults() bool {
	return pv.resultField != "_time"
}

func (pv *pipeJSONArrayValues) isFixedOutputFieldsOrder() bool {
	return false
}

func (pv *pipeJSONArrayValues) updateNeededFields(pf *prefixfilter.Filter) {
	if pf.MatchString(pv.resultField) {
		pf.AddDenyFilter(pv.resultField)
		pf.AddAllowFilter(pv.fromField)
	}
}

func (pv *pipeJSONArrayValues) hasFilterInWithQuery() bool {
	return false
}

func (pv *pipeJSONArrayValues) initFilterInValues(_ *inValuesCache, _ getFieldValuesFunc) (pipe, error) {
	return pv, nil
}

func (pv *pipeJSONArrayValues) visitSubqueries(_ func(q *Query)) {
	// nothing to do
}

func (pv *pipeJSONArrayValues) newPipeProcessor(_ int, _ <-chan struct{}, _ func(), ppNext pipeProcessor) pipeProcessor {
	pvp := &pipeJSONArrayValuesProcessor{
		pv:     pv,
		ppNext: ppNext,
	}
	return pvp
}

type pipeJSONArrayValuesProcessor struct {
	pv     *pipeJSONArrayValues
	ppNext pipeProcessor

	shards atomicutil.Slice[pipeJSONArrayValuesProcessorShard]
}

type pipeJSONArrayValuesProcessorShard struct {
	a  arena
	rc resultColumn
}

func (pvp *pipeJSONArrayValuesProcessor) writeBlock(workerID uint, br *blockResult) {
	if br.rowsLen == 0 {
		return
	}

	shard := pvp.shards.Get(workerID)
	shard.rc.name = pvp.pv.resultField

	c := br.getColumnByName(pvp.pv.fromField)
	if c.isConst {
		// Fast path for const column
		v := c.valuesEncoded[0]
		vEncoded := shard.getEncodedJSONArrayValues(pvp.pv.elementField, v)
		shard.rc.addValue(vEncoded)
		br.addResultColumnConst(shard.rc)
	} else {
		// Slow path for other columns
		values := c.getValues(br)
		vEncoded := ""
		for rowIdx := range values {
			if rowIdx == 0 || values[rowIdx] != values[rowIdx-1] {
				vEncoded = shard.getEncodedJSONArrayValues(pvp.pv.elementField, values[rowIdx])
			}
			shard.rc.addValue(vEncoded)
		}
		br.addResultColumn(shard.rc)
	}

	pvp.ppNext.writeBlock(workerID, br)

	shard.reset()
}

func (shard *pipeJSONArrayValuesProcessorShard) reset() {
	shard.a.reset()
	shard.rc.reset()
}

func (pvp *pipeJSONArrayValuesProcessor) flush() error {
	return nil
}

func (shard *pipeJSONArrayValuesProcessorShard) getEncodedJSONArrayValues(elementField, src string) string {
	bLen := len(shard.a.b)
	shard.a.b = appendJSONArrayValues(shard.a.b, elementField, src)
	return bytesutil.ToUnsafeString(shard.a.b[bLen:])
}

// appendJSONArrayValues appends to dst a JSON array containing the values of elementField
// extracted from every object in the JSON array stored at src.
//
// If src isn't a JSON array, an empty JSON array `[]` is appended.
// Array elements that aren't JSON objects, or that don't contain elementField, are skipped.
func appendJSONArrayValues(dst []byte, elementField, src string) []byte {
	if src == "" || src[0] != '[' {
		return append(dst, '[', ']')
	}

	p := jspp.Get()
	defer jspp.Put(p)

	jsv, err := p.Parse(src)
	if err != nil {
		return append(dst, '[', ']')
	}
	jsa, err := jsv.Array()
	if err != nil {
		return append(dst, '[', ']')
	}

	dst = append(dst, '[')
	first := true
	for _, e := range jsa {
		if e.Type() != fastjson.TypeObject {
			continue
		}
		fv := e.Get(elementField)
		if fv == nil {
			continue
		}
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = fv.MarshalTo(dst)
	}
	dst = append(dst, ']')
	return dst
}

func parsePipeJSONArrayValues(lex *lexer) (pipe, error) {
	if !lex.isKeyword("json_array_values") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "json_array_values")
	}
	lex.nextToken()

	elementField, err := parseFieldName(lex)
	if err != nil {
		return nil, fmt.Errorf("cannot parse element field name for 'json_array_values' pipe: %w", err)
	}

	fromField := "_msg"
	if lex.isKeyword("from") {
		lex.nextToken()
		f, err := parseFieldName(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse 'from' field name for 'json_array_values %s': %w", quoteTokenIfNeeded(elementField), err)
		}
		fromField = f
	}

	resultField := "_msg"
	if lex.isKeyword("as") {
		lex.nextToken()
	}
	if !lex.isKeyword("|", ")", "") {
		f, err := parseFieldName(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse result field for 'json_array_values %s': %w", quoteTokenIfNeeded(elementField), err)
		}
		resultField = f
	}

	pv := &pipeJSONArrayValues{
		elementField: elementField,
		fromField:    fromField,
		resultField:  resultField,
	}

	return pv, nil
}
