package vlstorage

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestRunOptimizedTimeSortedNResultsPreservesFixedFieldsOrder(t *testing.T) {
	storageDir := t.TempDir()
	s := logstorage.MustOpenStorage(storageDir, &logstorage.StorageConfig{
		Retention: 100 * 365 * 24 * time.Hour,
	})
	defer s.MustClose()

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	baseTime := time.Date(2025, 6, 1, 14, 30, 19, 0, time.UTC)
	for i := range 7 {
		ts := baseTime.Add(time.Duration(i) * 24 * time.Hour).UnixNano()
		lr.MustAdd(logstorage.TenantID{}, ts, []logstorage.Field{
			{Name: "_msg", Value: fmt.Sprintf("message-%d", i)},
		}, 0)
	}
	s.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	s.DebugFlush()

	prevStorage := localStorage
	localStorage = s
	defer func() {
		localStorage = prevStorage
	}()

	runQuery := func(qStr string) ([]string, [][]string) {
		t.Helper()
		q, err := logstorage.ParseQueryAtTimestamp(qStr, baseTime.Add(10*24*time.Hour).UnixNano())
		if err != nil {
			t.Fatalf("cannot parse query %q: %s", qStr, err)
		}
		qctx := logstorage.NewQueryContext(t.Context(), &logstorage.QueryStats{}, []logstorage.TenantID{{}}, q, false, nil)

		var colNames []string
		var gotRows [][]string
		err = RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			cols := db.GetColumns(false)
			if len(colNames) == 0 {
				for _, c := range cols {
					colNames = append(colNames, c.Name)
				}
			}
			for rowIdx := range db.RowsCount() {
				row := make([]string, len(cols))
				for colIdx, c := range cols {
					row[colIdx] = c.Values[rowIdx]
				}
				gotRows = append(gotRows, row)
			}
		})
		if err != nil {
			t.Fatalf("RunQuery(%q) error: %s", qStr, err)
		}
		return colNames, gotRows
	}

	wantMsgTimeDesc := [][]string{
		{"message-6", "2025-06-07T14:30:19Z"},
		{"message-5", "2025-06-06T14:30:19Z"},
		{"message-4", "2025-06-05T14:30:19Z"},
		{"message-3", "2025-06-04T14:30:19Z"},
		{"message-2", "2025-06-03T14:30:19Z"},
	}

	cols, rows := runQuery("* | sort by (_time desc) limit 5 | fields _msg, _time")
	if !reflect.DeepEqual(cols, []string{"_msg", "_time"}) {
		t.Fatalf("unexpected columns order; got %v; want [_msg _time]", cols)
	}
	if !reflect.DeepEqual(rows, wantMsgTimeDesc) {
		t.Fatalf("unexpected rows; got %v; want %v", rows, wantMsgTimeDesc)
	}

	cols, rows = runQuery("* | sort by (_time desc) offset 2 limit 3 | fields _msg, _time")
	if !reflect.DeepEqual(cols, []string{"_msg", "_time"}) {
		t.Fatalf("unexpected columns order for offset+limit; got %v; want [_msg _time]", cols)
	}
	if !reflect.DeepEqual(rows, wantMsgTimeDesc[2:]) {
		t.Fatalf("unexpected rows for offset+limit; got %v; want %v", rows, wantMsgTimeDesc[2:])
	}

	cols, rows = runQuery("* | fields _msg, _time | sort by (_time desc) limit 5")
	if !reflect.DeepEqual(cols, []string{"_time", "_msg"}) {
		t.Fatalf("unexpected columns order when fields precedes sort; got %v; want [_time _msg]", cols)
	}
	wantTimeMsgDesc := [][]string{
		{"2025-06-07T14:30:19Z", "message-6"},
		{"2025-06-06T14:30:19Z", "message-5"},
		{"2025-06-05T14:30:19Z", "message-4"},
		{"2025-06-04T14:30:19Z", "message-3"},
		{"2025-06-03T14:30:19Z", "message-2"},
	}
	if !reflect.DeepEqual(rows, wantTimeMsgDesc) {
		t.Fatalf("unexpected rows when fields precedes sort; got %v; want %v", rows, wantTimeMsgDesc)
	}

	cols, rows = runQuery("* | sort by (_time desc) limit 2 | fields _msg, _time")
	if !reflect.DeepEqual(cols, []string{"_msg", "_time"}) {
		t.Fatalf("unexpected columns order for binary-search path; got %v; want [_msg _time]", cols)
	}
	if !reflect.DeepEqual(rows, wantMsgTimeDesc[:2]) {
		t.Fatalf("unexpected rows for binary-search path; got %v; want %v", rows, wantMsgTimeDesc[:2])
	}

	cols, _ = runQuery("* | sort by (_time desc) limit 5")
	if len(cols) == 0 || cols[0] != "_time" {
		t.Fatalf("expected _time first when output field order is not fixed; got %v", cols)
	}
}
