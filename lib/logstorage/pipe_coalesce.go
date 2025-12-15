package logstorage

import (
	"fmt"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeCoalesce implements '| coalesce (...) as ...' pipe.
//
// See https://docs.victoriametrics.com/victorialogs/logsql/#coalesce-pipe
type pipeCoalesce struct {
	srcFields    []string
	dstField     string
	defaultValue string
}

func (pc *pipeCoalesce) String() string {
	if len(pc.srcFields) == 0 {
		logger.Panicf("BUG: pipeCoalesce must contain at least one srcField")
	}

	s := "coalesce(" + strings.Join(pc.srcFields, ", ") + ")"
	s += " as " + quoteTokenIfNeeded(pc.dstField)
	if pc.defaultValue != "" {
		s += " default " + quoteTokenIfNeeded(pc.defaultValue)
	}
	return s
}

func (pc *pipeCoalesce) splitToRemoteAndLocal(_ int64) (pipe, []pipe) {
	return pc, nil
}

func (pc *pipeCoalesce) canLiveTail() bool {
	return true
}

func (pc *pipeCoalesce) canReturnLastNResults() bool {
	return pc.dstField != "_time"
}

func (pc *pipeCoalesce) updateNeededFields(pf *prefixfilter.Filter) {
	if pf.MatchString(pc.dstField) {
		pf.AddAllowFilters(pc.srcFields)
	}

	pf.AddDenyFilter(pc.dstField)
}

func (pc *pipeCoalesce) hasFilterInWithQuery() bool {
	return false
}

func (pc *pipeCoalesce) initFilterInValues(_ *inValuesCache, _ getFieldValuesFunc, _ bool) (pipe, error) {
	return pc, nil
}

func (pc *pipeCoalesce) visitSubqueries(_ func(q *Query)) {

}

func (pc *pipeCoalesce) newPipeProcessor(_ int, _ <-chan struct{}, _ func(), ppNext pipeProcessor) pipeProcessor {
	return &pipeCoalesceProcessor{
		pc:     pc,
		ppNext: ppNext,
	}
}

// pipeCoalesceProcessor processes the coalesce pipe
type pipeCoalesceProcessor struct {
	pc     *pipeCoalesce
	ppNext pipeProcessor

	shards atomicutil.Slice[pipeCoalesceProcessorShard]
}

type pipeCoalesceProcessorShard struct {
	rc resultColumn
}

func (pcp *pipeCoalesceProcessor) writeBlock(workerID uint, br *blockResult) {
	if br.rowsLen == 0 {
		return
	}

	shard := pcp.shards.Get(workerID)
	pc := pcp.pc

	shard.rc.name = pc.dstField

	srcColumns := make([]*blockResultColumn, len(pc.srcFields))
	for i, srcField := range pc.srcFields {
		srcColumns[i] = br.getColumnByName(srcField)
	}

	for rowIdx := 0; rowIdx < br.rowsLen; rowIdx++ {
		value := ""
		for _, srcColumn := range srcColumns {
			v := srcColumn.getValueAtRow(br, rowIdx)
			if v != "" {
				value = v
				break
			}
		}

		// If all source fields are empty, use default value
		if value == "" {
			value = pc.defaultValue
		}

		shard.rc.addValue(value)
	}

	br.addResultColumn(shard.rc)
	pcp.ppNext.writeBlock(workerID, br)

	shard.rc.reset()
}

func (pcp *pipeCoalesceProcessor) flush() error {
	return nil
}

// parsePipeCoalesce parses '| coalesce(field1, field2, field3) as result_field default "default value"'
func parsePipeCoalesce(lex *lexer) (pipe, error) {
	if !lex.isKeyword("coalesce") {
		return nil, fmt.Errorf("expecting 'coalesce'; got %q", lex.token)
	}
	lex.nextToken()

	if !lex.isKeyword("(") {
		return nil, fmt.Errorf("expecting '(' after 'coalesce'; got %q", lex.token)
	}

	// Parse comma-separated field names in parentheses
	var srcFields []string
	for {
		lex.nextToken()
		if lex.isKeyword(")") {
			lex.nextToken()
			break
		}
		if lex.isKeyword(",") {
			return nil, fmt.Errorf("unexpected ','")
		}

		fieldName, err := parseFieldName(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse field name: %w", err)
		}
		srcFields = append(srcFields, fieldName)

		if lex.isKeyword(")") {
			lex.nextToken()
			break
		}

		if !lex.isKeyword(",") {
			return nil, fmt.Errorf("unexpected token: %q; expecting ',' or ')'", lex.token)
		}
	}

	if len(srcFields) == 0 {
		return nil, fmt.Errorf("coalesce requires at least one field name")
	}

	// Parse 'as' keyword
	if !lex.isKeyword("as") {
		return nil, fmt.Errorf("expecting 'as' after coalesce(...); got %q", lex.token)
	}
	lex.nextToken()

	// Parse destination field name
	dstField, err := parseFieldName(lex)
	if err != nil {
		return nil, fmt.Errorf("cannot parse result field name: %w", err)
	}

	// Parse optional 'default' keyword and value
	var defaultValue string
	if lex.isKeyword("default") {
		lex.nextToken() // Skip the "default" keyword
		value, err := lex.nextCompoundToken()
		if err != nil {
			return nil, fmt.Errorf("cannot parse default value: %w", err)
		}
		defaultValue = value
	}

	pc := &pipeCoalesce{
		srcFields:    srcFields,
		dstField:     dstField,
		defaultValue: defaultValue,
	}
	return pc, nil
}
