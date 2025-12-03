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

func runOptimizedLastNResultsQuery(qctx *logstorage.QueryContext, offset, limit uint64, writeBlock logstorage.WriteDataBlockFunc) error {
	rows, err := getLastNQueryResults(qctx, offset+limit)
	if err != nil {
		return err
	}
	if uint64(len(rows)) > offset {
		rows = rows[offset:]
	}

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
		db.Columns = columns
		writeBlock(0, &db)
	}
	return nil
}

func getLastNQueryResults(qctx *logstorage.QueryContext, limit uint64) ([]logRow, error) {
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
		rows = getLastNRows(rows, limit)
		return rows, nil
	}

	// Slow path - use binary search for adjusting time range for selecting up to 2*limit rows.
	start, end := q.GetFilterTimeRange()
	if end < math.MaxInt64 {
		end++
	}
	start += end/2 - start/2
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
			rowsFound = getLastNRows(rowsFound, limit)
			return rowsFound, nil
		}

		if uint64(len(rows)) >= 2*n {
			// The number of found rows on the [start ... end) time range exceeds 2*n,
			// so search for the rows on the adjusted time range [start+(end/2-start/2) ... end).
			if !logstorage.CanApplyLastNResultsOptimization(start, end) {
				// It is faster obtaining the last N logs as is on such a small time range instead of using binary search.
				rows, err := getLogRowsLastN(qctx, start, end, n)
				if err != nil {
					return nil, err
				}
				rowsFound = append(rowsFound, rows...)
				rowsFound = getLastNRows(rowsFound, limit)
				return rowsFound, nil
			}
			start += end/2 - start/2
			lastNonEmptyRows = rows
			continue
		}
		if uint64(len(rowsFound)+len(rows)) >= limit {
			// The found rows contains the needed limit rows with the biggest timestamps.
			rowsFound = append(rowsFound, rows...)
			rowsFound = getLastNRows(rowsFound, limit)
			return rowsFound, nil
		}

		// The number of found rows is below the limit. This means the [start ... end) time range
		// doesn't cover the needed logs, so it must be extended.
		// Append the found rows to rowsFound, adjust n, so it doesn't take into account already found rows
		// and adjust the time range to search logs at [start-(end/2-start/2) ... start).
		rowsFound = append(rowsFound, rows...)
		n -= uint64(len(rows))

		d := end/2 - start/2
		end = start
		start -= d
	}
}

func getLogRowsLastN(qctx *logstorage.QueryContext, start, end int64, n uint64) ([]logRow, error) {
	timestamp := qctx.Query.GetTimestamp()
	q := qctx.Query.CloneWithTimeFilter(timestamp, start, end)
	q.AddPipeSortByTimeDesc()
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

	columnNames := make([]string, len(db.Columns))
	for i, c := range db.Columns {
		columnNames[i] = strings.Clone(c.Name)
	}

	lrs := make([]logRow, 0, len(timestamps))
	fieldsBuf := make([]logstorage.Field, 0, len(columnNames)*len(timestamps))

	for i, timestamp := range timestamps {
		fieldsBufLen := len(fieldsBuf)
		for j, c := range db.Columns {
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

func getLastNRows(rows []logRow, limit uint64) []logRow {
	sortLogRows(rows)
	if uint64(len(rows)) > limit {
		rows = rows[:limit]
	}
	return rows
}

func sortLogRows(rows []logRow) {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].timestamp > rows[j].timestamp
	})
}
