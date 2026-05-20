package logstorage

import (
	"fmt"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
)

// QueryStats contains various query execution stats.
type QueryStats struct {
	// BytesReadColumnsHeaders is the total number of columns header bytes read from disk during the search.
	BytesReadColumnsHeaders uint64

	// BytesReadColumnsHeaderIndexes is the total number of columns header index bytes read from disk during the search.
	BytesReadColumnsHeaderIndexes uint64

	// BytesReadBloomFilters is the total number of bloom filter bytes read from disk during the search.
	BytesReadBloomFilters uint64

	// BytesReadValues is the total number of values bytes read from disk during the search.
	BytesReadValues uint64

	// BytesReadTimestamps is the total number of timestamps bytes read from disk during the search.
	BytesReadTimestamps uint64

	// BytesReadBlockHeaders is the total number of headers bytes read from disk during the search.
	BytesReadBlockHeaders uint64

	// BlocksProcessed is the number of data blocks processed during query execution.
	BlocksProcessed uint64

	// RowsProcessed is the number of log rows processed during query execution.
	RowsProcessed uint64

	// RowsFound is the number of rows found by the query.
	RowsFound uint64

	// ValuesRead is the number of log field values read during query exection.
	ValuesRead uint64

	// TimestampsRead is the number of timestamps read during query execution.
	TimestampsRead uint64

	// BytesProcessedUncompressedValues is the total number of uncompressed values bytes processed during the search.
	BytesProcessedUncompressedValues uint64

	QueryCompleteness QueryCompleteness
}

// GetBytesReadTotal returns the total number of bytes read, which is tracked by qs.
func (qs *QueryStats) GetBytesReadTotal() uint64 {
	return qs.BytesReadColumnsHeaders + qs.BytesReadColumnsHeaderIndexes + qs.BytesReadBloomFilters + qs.BytesReadValues + qs.BytesReadTimestamps + qs.BytesReadBlockHeaders
}

// UpdateAtomic add src to qs in an atomic manner.
func (qs *QueryStats) UpdateAtomic(src *QueryStats) {
	atomic.AddUint64(&qs.BytesReadColumnsHeaders, src.BytesReadColumnsHeaders)
	atomic.AddUint64(&qs.BytesReadColumnsHeaderIndexes, src.BytesReadColumnsHeaderIndexes)
	atomic.AddUint64(&qs.BytesReadBloomFilters, src.BytesReadBloomFilters)
	atomic.AddUint64(&qs.BytesReadValues, src.BytesReadValues)
	atomic.AddUint64(&qs.BytesReadTimestamps, src.BytesReadTimestamps)
	atomic.AddUint64(&qs.BytesReadBlockHeaders, src.BytesReadBlockHeaders)

	atomic.AddUint64(&qs.BlocksProcessed, src.BlocksProcessed)
	atomic.AddUint64(&qs.RowsProcessed, src.RowsProcessed)
	atomic.AddUint64(&qs.RowsFound, src.RowsFound)
	atomic.AddUint64(&qs.ValuesRead, src.ValuesRead)
	atomic.AddUint64(&qs.TimestampsRead, src.TimestampsRead)
	atomic.AddUint64(&qs.BytesProcessedUncompressedValues, src.BytesProcessedUncompressedValues)

	mergeQueryCompleteness(&qs.QueryCompleteness, &src.QueryCompleteness)
}

// QueryCompleteness indicates whether a query returned a full or partial result set.
// Partial result only happen if the `-search.allowPartialResponse` command-line flag or the `allow_partial_response` LogsQL option is set.
// See https://docs.victoriametrics.com/victorialogs/querying/#partial-responses
type QueryCompleteness uint32

const (
	// QueryCompletenessNotSet indicates that the value was not set.
	// This typically occurs in streaming queries where data is not buffered but flushed periodically without merging a final completeness status.
	// In this case, it behaves like QueryCompletenessUnknown, except that QueryCompletenessNotSet has a lower priority during a merge than other statuses.
	QueryCompletenessNotSet QueryCompleteness = 0

	// QueryCompletenessPartial means that at least one storage failed to process the query, and the response contains partially calculated data.
	QueryCompletenessPartial QueryCompleteness = 1

	// QueryCompletenessUnknown indicates a case where it is impossible to determine whether a query is complete or partial.
	// This occurs with streaming queries that are not buffered in memory.
	QueryCompletenessUnknown QueryCompleteness = 2

	// QueryCompletenessComplete means that the query was processed by every storage without errors, and the response is complete.
	QueryCompletenessComplete QueryCompleteness = 3
)

func (qs *QueryStats) MergeQueryCompleteness(complete bool) {
	v := QueryCompletenessComplete
	if !complete {
		v = QueryCompletenessPartial
	}
	mergeQueryCompleteness(&qs.QueryCompleteness, &v)
}

func mergeQueryCompleteness(dstPtr, srcPtr *QueryCompleteness) {
	src := QueryCompleteness(atomic.LoadUint32((*uint32)(srcPtr)))
	if src == QueryCompletenessNotSet {
		return
	}
	srcPriority := getQueryCompletenessPriority(src)

	for {
		dst := QueryCompleteness(atomic.LoadUint32((*uint32)(dstPtr)))
		dstPriority := getQueryCompletenessPriority(dst)
		if srcPriority <= dstPriority {
			// src priority is lower, e.g. Partial should not be overridden with Unknown.
			return
		}
		if atomic.CompareAndSwapUint32((*uint32)(dstPtr), uint32(dst), uint32(src)) {
			return
		}
	}
}

func getQueryCompletenessPriority(qc QueryCompleteness) uint8 {
	switch qc {
	case QueryCompletenessPartial:
		return 3
	case QueryCompletenessUnknown:
		return 2
	case QueryCompletenessComplete:
		return 1
	default:
		return 0
	}
}

// UpdateAtomicFromDataBlock adds query stats from db to qs.
func (qs *QueryStats) UpdateFromDataBlock(db *DataBlock) error {
	rowsCount := db.RowsCount()
	if rowsCount != 1 {
		return fmt.Errorf("unexpected number of rows in the query stats block; got %d; want 1", rowsCount)
	}

	var errGlobal error
	getUint64Entry := func(name string, required bool) uint64 {
		c := db.GetColumnByName(name)
		if c == nil {
			if !required {
				return 0
			}
			if errGlobal == nil {
				errGlobal = fmt.Errorf("cannot find field %q in query stats received from the remote storage", name)
			}
			return 0
		}
		v := c.Values[0]
		n, _ := tryParseUint64(v)
		return n
	}

	qs.BytesReadColumnsHeaders += getUint64Entry("BytesReadColumnsHeaders", true)
	qs.BytesReadColumnsHeaderIndexes += getUint64Entry("BytesReadColumnsHeaderIndexes", true)
	qs.BytesReadBloomFilters += getUint64Entry("BytesReadBloomFilters", true)
	qs.BytesReadValues += getUint64Entry("BytesReadValues", true)
	qs.BytesReadTimestamps += getUint64Entry("BytesReadTimestamps", true)
	qs.BytesReadBlockHeaders += getUint64Entry("BytesReadBlockHeaders", true)

	qs.BlocksProcessed += getUint64Entry("BlocksProcessed", true)
	qs.RowsProcessed += getUint64Entry("RowsProcessed", true)
	qs.RowsFound += getUint64Entry("RowsFound", true)
	qs.ValuesRead += getUint64Entry("ValuesRead", true)
	qs.TimestampsRead += getUint64Entry("TimestampsRead", true)
	qs.BytesProcessedUncompressedValues += getUint64Entry("BytesProcessedUncompressedValues", true)

	v := QueryCompleteness(getUint64Entry("QueryCompleteness", false))
	if v == QueryCompletenessNotSet {
		// We received NotSet, which means a backend flushed the data without waiting for the final status.
		// Override the value to Unknown to distinguish it from the default value (NotSet).
		v = QueryCompletenessUnknown
	}
	mergeQueryCompleteness(&qs.QueryCompleteness, &v)

	return errGlobal
}

// CreateDataBlock creates a DataBlock from qs.
func (qs *QueryStats) CreateDataBlock(queryDurationNsecs int64) *DataBlock {
	var cs []BlockColumn

	addUint64Entry := func(name string, value uint64) {
		cs = append(cs, BlockColumn{
			Name: name,
			Values: []string{
				string(marshalUint64String(nil, value)),
			},
		})
	}

	qs.addPublicEntries(addUint64Entry, queryDurationNsecs)

	// An internal field that should not be shown to the user.
	addUint64Entry("QueryCompleteness", uint64(qs.QueryCompleteness))

	return &DataBlock{
		columns: cs,
	}
}

func (qs *QueryStats) writeToPipeProcessor(pp pipeProcessor, queryDurationNsecs int64) {
	var rcs []resultColumn

	var buf []byte
	addUint64Entry := func(name string, value uint64) {
		rcs = append(rcs, resultColumn{})
		rc := &rcs[len(rcs)-1]
		rc.name = name
		bufLen := len(buf)
		buf = marshalUint64String(buf, value)
		v := bytesutil.ToUnsafeString(buf[bufLen:])
		rc.addValue(v)
	}

	qs.addPublicEntries(addUint64Entry, queryDurationNsecs)

	var br blockResult
	br.setResultColumns(rcs, 1)
	pp.writeBlock(0, &br)
}

func (qs *QueryStats) addPublicEntries(addUint64Entry func(name string, value uint64), queryDurationNsecs int64) {
	addUint64Entry("BytesReadColumnsHeaders", qs.BytesReadColumnsHeaders)
	addUint64Entry("BytesReadColumnsHeaderIndexes", qs.BytesReadColumnsHeaderIndexes)
	addUint64Entry("BytesReadBloomFilters", qs.BytesReadBloomFilters)
	addUint64Entry("BytesReadValues", qs.BytesReadValues)
	addUint64Entry("BytesReadTimestamps", qs.BytesReadTimestamps)
	addUint64Entry("BytesReadBlockHeaders", qs.BytesReadBlockHeaders)

	addUint64Entry("BytesReadTotal", qs.GetBytesReadTotal())

	addUint64Entry("BlocksProcessed", qs.BlocksProcessed)
	addUint64Entry("RowsProcessed", qs.RowsProcessed)
	addUint64Entry("RowsFound", qs.RowsFound)
	addUint64Entry("ValuesRead", qs.ValuesRead)
	addUint64Entry("TimestampsRead", qs.TimestampsRead)
	addUint64Entry("BytesProcessedUncompressedValues", qs.BytesProcessedUncompressedValues)

	addUint64Entry("QueryDurationNsecs", uint64(queryDurationNsecs))
}
