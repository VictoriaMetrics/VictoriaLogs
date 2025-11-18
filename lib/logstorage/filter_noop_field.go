package logstorage

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

type filterNoopField struct {
	fieldName string
}

func (fa *filterNoopField) String() string {
	return quoteTokenIfNeeded(fa.fieldName) + ":**"
}

func (fa *filterNoopField) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter(fa.fieldName)
}

func (fn *filterNoopField) matchRow(fields []Field) bool {
	return true
}

func (fa *filterNoopField) applyToBlockResult(br *blockResult, bm *bitmap) {}

func (fa *filterNoopField) applyToBlockSearch(bs *blockSearch, bm *bitmap) {}
