package vlstorage

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type timeSortedRangeOps struct {
	// When current range has too many matches: narrow to the half nearer the target end
	narrow func(start, end int64) (int64, int64)
	// When current range has too few matches: return the adjacent range away from target
	shift func(start, end int64) (int64, int64)
}

var descDirOps = timeSortedRangeOps{
	narrow: func(start, end int64) (int64, int64) {
		// The number of found rows on the [start ... end) time range exceeds 2*n,
		// so search for the rows on the adjusted time range [start+(end/2-start/2) ... end).
		return start + (end/2 - start/2), end
	},
	shift: func(start, end int64) (int64, int64) {
		d := end/2 - start/2
		return start - d, start
	},
}

var ascDirOps = timeSortedRangeOps{
	narrow: func(start, end int64) (int64, int64) {
		return start, end - (end/2 - start/2)
	},
	shift: func(start, end int64) (int64, int64) {
		d := end/2 - start/2
		return end, end + d
	},
}

func runOptimizedTimeSortedNResultsQuery(qctx *logstorage.QueryContext, offset, limit uint64, isDesc bool, writeBlock logstorage.WriteDataBlockFunc) error {
	rows, err := getTimeSortedNQueryResults(qctx, offset+limit, isDesc)
	if err != nil {
		return err
	}
	if offset >= uint64(len(rows)) {
		return nil
	}
	rows = rows[offset:]

	var db logstorage.DataBlock
	var columns []logstorage.BlockColumn
	var values []string
	for _, r := range rows {
		columns = slicesutil.SetLength(columns, len(r.fields))
		values = slicesutil.SetLength(values, len(r.fields))
		for j, f := range r.fields {
			values[j] = f.Value
			columns[j].Name = f.Name
			columns[j].Values = values[j : j+1]
		}
		db.SetColumns(columns)
		writeBlock(0, &db)
	}
	return nil
}

func getTimeSortedNQueryResults(qctx *logstorage.QueryContext, limit uint64, isDesc bool) ([]logRow, error) {
	timestamp := qctx.Query.GetTimestamp()

	q := qctx.Query.Clone(timestamp)
	q.AddPipeOffsetLimit(0, 2*limit)
	qctxLocal := qctx.WithQuery(q)
	rows, err := getQueryResults(qctxLocal)
	if err != nil {
		return nil, err
	}

	if uint64(len(rows)) < 2*limit {
		// Fast path - the requested time range contains up to 2*limit rows.
		rows = getTopNRows(rows, limit, isDesc)
		return rows, nil
	}

	ops := ascDirOps
	if isDesc {
		ops = descDirOps
	}

	// Slow path - use binary search for adjusting time range for selecting up to 2*limit rows.
	start, end := q.GetFilterTimeRange()
	if end < math.MaxInt64 {
		end++
	}
	start, end = ops.narrow(start, end)
	n := limit

	var rowsFound []logRow
	var lastNonEmptyRows []logRow

	for {
		q = qctx.Query.CloneWithTimeFilter(timestamp, start, end-1)
		q.AddPipeOffsetLimit(0, 2*n)
		qctxLocal := qctx.WithQuery(q)
		rows, err := getQueryResults(qctxLocal)
		if err != nil {
			return nil, err
		}

		if end/2-start/2 <= 0 {
			// The [start ... end) time range doesn't exceed a nanosecond, e.g. it cannot be adjusted more.
			// Return up to limit rows from the found rows and the last non-empty rows.
			rowsFound = append(rowsFound, lastNonEmptyRows...)
			rowsFound = append(rowsFound, rows...)
			rowsFound = getTopNRows(rowsFound, limit, isDesc)
			return rowsFound, nil
		}

		if uint64(len(rows)) >= 2*n {
			// The number of found rows on the current time range exceeds 2*n,
			// so search for the rows on the adjusted narrowed time range by a half.
			if !logstorage.CanApplyTimeSortedNResultsOptimization(start, end) {
				// It is faster obtaining the top N logs via a direct sort+limit on such a small time range instead of using binary search.
				rows, err := getLogRowsTopN(qctx, start, end, isDesc, n)
				if err != nil {
					return nil, err
				}
				rowsFound = append(rowsFound, rows...)
				rowsFound = getTopNRows(rowsFound, limit, isDesc)
				return rowsFound, nil
			}
			start, end = ops.narrow(start, end)
			lastNonEmptyRows = rows
			continue
		}
		if uint64(len(rowsFound)+len(rows)) >= limit {
			// The found rows contain the needed limit rows in the target time direction.
			rowsFound = append(rowsFound, rows...)
			rowsFound = getTopNRows(rowsFound, limit, isDesc)
			return rowsFound, nil
		}

		// The number of found rows is below the limit. This means the current time range
		// doesn't cover the needed logs, so it must be extended.
		// Append the found rows to rowsFound, adjust n, so it doesn't take into account already found rows
		// and shift the time range via ops.shift to the adjacent non-overlapping range.
		rowsFound = append(rowsFound, rows...)
		n -= uint64(len(rows))

		start, end = ops.shift(start, end)
	}
}

func getLogRowsTopN(qctx *logstorage.QueryContext, start, end int64, isDesc bool, n uint64) ([]logRow, error) {
	timestamp := qctx.Query.GetTimestamp()
	q := qctx.Query.CloneWithTimeFilter(timestamp, start, end)
	q.AddPipeSortByTime(isDesc)
	q.AddPipeOffsetLimit(0, n)
	qctxLocal := qctx.WithQuery(q)
	return getQueryResults(qctxLocal)
}

func getQueryResults(qctx *logstorage.QueryContext) ([]logRow, error) {
	var rowsLock sync.Mutex
	var rows []logRow

	var errLocal error
	var errLocalLock sync.Mutex

	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsLocal, err := getLogRowsFromDataBlock(db)
		if err != nil {
			errLocalLock.Lock()
			errLocal = err
			errLocalLock.Unlock()
		}

		rowsLock.Lock()
		rows = append(rows, rowsLocal...)
		rowsLock.Unlock()
	}

	err := RunQuery(qctx, writeBlock)
	if errLocal != nil {
		return nil, errLocal
	}

	return rows, err
}

func getLogRowsFromDataBlock(db *logstorage.DataBlock) ([]logRow, error) {
	timestamps, ok := db.GetTimestamps(nil)
	if !ok {
		return nil, fmt.Errorf("missing _time field in the query results")
	}

	// There is no need to sort columns here, since they will be sorted by the caller.
	columns := db.GetColumns(false)

	columnNames := make([]string, len(columns))
	var timestampsColumn logstorage.BlockColumn
	for i, c := range columns {
		if c.Name == "_time" {
			timestampsColumn = c
		}
		columnNames[i] = strings.Clone(c.Name)
	}

	lrs := make([]logRow, 0, len(timestamps))
	fieldsBuf := make([]logstorage.Field, 0, len(columns)*len(timestamps))

	for i, timestamp := range timestamps {
		fieldsBufLen := len(fieldsBuf)

		// The _time column must go first, since the query results are sorted by _time.
		fieldsBuf = append(fieldsBuf, logstorage.Field{
			Name:  "_time",
			Value: strings.Clone(timestampsColumn.Values[i]),
		})

		for j, c := range columns {
			if c.Name == "_time" {
				continue
			}
			fieldsBuf = append(fieldsBuf, logstorage.Field{
				Name:  columnNames[j],
				Value: strings.Clone(c.Values[i]),
			})
		}
		lrs = append(lrs, logRow{
			timestamp: timestamp,
			fields:    fieldsBuf[fieldsBufLen:],
		})
	}

	return lrs, nil
}

type logRow struct {
	timestamp int64
	fields    []logstorage.Field
}

func getTopNRows(rows []logRow, limit uint64, isDesc bool) []logRow {
	if isDesc {
		sortLogRowsDesc(rows)
	} else {
		sortLogRowsAsc(rows)
	}

	if uint64(len(rows)) > limit {
		rows = rows[:limit]
	}
	return rows
}

func sortLogRowsDesc(rows []logRow) {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].timestamp > rows[j].timestamp
	})
}

func sortLogRowsAsc(rows []logRow) {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].timestamp < rows[j].timestamp
	})
}
