package vlstorage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func runOptimizedLastNResultsQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64, writeBlock logstorage.WriteDataBlockFunc) error {
	rows, err := getLastNQueryResults(ctx, tenantIDs, q, limit)
	if err != nil {
		return err
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

func getLastNQueryResults(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logRow, error) {
	qOrig := q
	timestamp := qOrig.GetTimestamp()

	q = qOrig.Clone(timestamp)
	q.AddPipeLimit(2 * limit)
	rows, err := getQueryResults(ctx, tenantIDs, q)
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
	d := end/2 - start/2
	start += d
	n := limit

	var rowsFound []logRow

	for {
		q = qOrig.CloneWithTimeFilter(timestamp, start, end)
		q.AddPipeLimit(2 * n)
		rows, err := getQueryResults(ctx, tenantIDs, q)
		if err != nil {
			return nil, err
		}

		if d == 0 || start >= end {
			// The [start ... end] time range equals to one nanosecond, e.g. it cannot be adjusted more. Return up to limit rows.
			rowsFound = append(rowsFound, rows...)
			rowsFound = getLastNRows(rowsFound, limit)
			return rowsFound, nil
		}

		dLastBit := d & 1
		d /= 2

		if uint64(len(rows)) >= 2*n {
			// The number of found rows on the [start ... end] time range exceeds 2*n,
			// so reduce the time range to further to [start+d ... end].
			start += d
			continue
		}
		if uint64(len(rows)) >= n {
			// The number of found rows is in the range [n ... 2*n).
			// This means that found rows contains the needed limit rows with the biggest timestamps.
			rowsFound = append(rowsFound, rows...)
			rowsFound = getLastNRows(rowsFound, limit)
			return rowsFound, nil
		}

		// The number of found rows on [start ... end] time range is below the limit.
		// This means the time range doesn't cover the needed logs, so it must be extended.
		// Append the found rows to rowsFound, adjust the limit, so it doesn't take into account already found rows
		// and adjust the time range to search logs to [start-d ... start).
		rowsFound = append(rowsFound, rows...)
		n -= uint64(len(rows))

		end = start - 1
		start -= d + dLastBit
	}
}

func getQueryResults(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logRow, error) {
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

	err := RunQuery(ctx, tenantIDs, q, writeBlock)
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
		rows = rows[uint64(len(rows))-limit:]
	}
	return rows
}

func sortLogRows(rows []logRow) {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].timestamp < rows[j].timestamp
	})
}
