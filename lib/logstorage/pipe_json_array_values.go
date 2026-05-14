package logstorage

import (
	"fmt"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/valyala/fastjson"
	"github.com/valyala/quicktemplate"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeJSONArrayValues processes '| json_array_values ...' pipe.
//
// See https://docs.victoriametrics.com/victorialogs/logsql/#json_array_values-pipe
type pipeJSONArrayValues struct {
	// fieldName is the field to extract from every JSON object in the array.
	fieldName string

	// fieldNameParts is fieldName split by '.', used for navigating nested JSON objects.
	fieldNameParts []string

	// fromField is the source field containing JSON array string.
	fromField string

	// resultField is the destination field.
	resultField string
}

func (pv *pipeJSONArrayValues) String() string {
	s := "json_array_values " + quoteTokenIfNeeded(pv.fieldName)
	if !isMsgFieldName(pv.fromField) {
		s += " from " + quoteTokenIfNeeded(pv.fromField)
	}
	if pv.resultField != pv.fromField {
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
	pvp.shards.Init = func(shard *pipeJSONArrayValuesProcessorShard) {
		shard.reset()
	}
	return pvp
}

type pipeJSONArrayValuesProcessor struct {
	pv     *pipeJSONArrayValues
	ppNext pipeProcessor

	shards atomicutil.Slice[pipeJSONArrayValuesProcessorShard]
}

func (pvp *pipeJSONArrayValuesProcessor) writeBlock(workerID uint, br *blockResult) {
	if br.rowsLen == 0 {
		return
	}

	shard := pvp.shards.Get(workerID)
	shard.rc.name = pvp.pv.resultField

	c := br.getColumnByName(pvp.pv.fromField)
	fieldName := pvp.pv.fieldName
	fieldNameParts := pvp.pv.fieldNameParts
	if c.isConst {
		// Fast path for const column
		v := c.valuesEncoded[0]
		r := shard.extractValues(v, fieldName, fieldNameParts)
		shard.rc.addValue(r)
		br.addResultColumnConst(shard.rc)
	} else {
		// Slow path for other columns
		values := c.getValues(br)
		vEncoded := ""
		for rowIdx := range values {
			if rowIdx == 0 || values[rowIdx] != values[rowIdx-1] {
				vEncoded = shard.extractValues(values[rowIdx], fieldName, fieldNameParts)
			}
			shard.rc.addValue(vEncoded)
		}
		br.addResultColumn(shard.rc)
	}

	pvp.ppNext.writeBlock(workerID, br)

	shard.reset()
}

type pipeJSONArrayValuesProcessorShard struct {
	a  arena
	rc resultColumn

	tmpBuf []byte
}

func (shard *pipeJSONArrayValuesProcessorShard) reset() {
	shard.a.reset()
	shard.rc.reset()
	shard.tmpBuf = shard.tmpBuf[:0]
}

func (shard *pipeJSONArrayValuesProcessorShard) extractValues(s, fieldName string, fieldNameParts []string) string {
	s = strings.TrimLeft(s, " \t\n\r")
	if s == "" || s[0] != '[' {
		return "[]"
	}

	p := jspp.Get()
	defer jspp.Put(p)

	jsv, err := p.Parse(s)
	if err != nil {
		return "[]"
	}
	jsa, err := jsv.Array()
	if err != nil {
		return "[]"
	}

	bLen := len(shard.a.b)
	shard.a.b = append(shard.a.b, '[')
	needComma := false
	for _, jso := range jsa {
		if jso.Type() != fastjson.TypeObject {
			continue
		}
		v := jso.Get(fieldName)
		if v == nil && len(fieldNameParts) > 1 {
			v = jso.Get(fieldNameParts...)
		}
		if v == nil || v.Type() == fastjson.TypeNull {
			continue
		}
		if needComma {
			shard.a.b = append(shard.a.b, ',')
		}
		if v.Type() == fastjson.TypeString {
			sb, _ := v.StringBytes()
			shard.a.b = quicktemplate.AppendJSONString(shard.a.b, bytesutil.ToUnsafeString(sb), true)
		} else {
			shard.tmpBuf = v.MarshalTo(shard.tmpBuf[:0])
			shard.a.b = quicktemplate.AppendJSONString(shard.a.b, bytesutil.ToUnsafeString(shard.tmpBuf), true)
		}
		needComma = true
	}

	shard.a.b = append(shard.a.b, ']')
	return bytesutil.ToUnsafeString(shard.a.b[bLen:])
}

func (pvp *pipeJSONArrayValuesProcessor) flush() error {
	return nil
}

func parsePipeJSONArrayValues(lex *lexer) (pipe, error) {
	if !lex.isKeyword("json_array_values") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "json_array_values")
	}
	lex.nextToken()

	fieldName, err := parseFieldNameWithOptionalParens(lex)
	if err != nil {
		return nil, fmt.Errorf("cannot parse field name after 'json_array_values': %w", err)
	}

	fromField := "_msg"
	if lex.isKeyword("from") {
		lex.nextToken()
		field, err := parseFieldNameWithOptionalParens(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse field name after 'json_array_values %s from': %w", quoteTokenIfNeeded(fieldName), err)
		}
		fromField = field
	}

	resultField := fromField
	if !lex.isKeyword(")", "|", "") {
		if lex.isKeyword("as") {
			lex.nextToken()
		}
		field, err := parseFieldNameWithOptionalParens(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse result field name for 'json_array_values' pipe: %w", err)
		}
		resultField = field
	}

	pv := &pipeJSONArrayValues{
		fieldName:      fieldName,
		fieldNameParts: strings.Split(fieldName, "."),
		fromField:      fromField,
		resultField:    resultField,
	}

	return pv, nil
}
