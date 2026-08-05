package logstorage

import (
	"fmt"
	"testing"
)

func BenchmarkRemoteStorageRunQuery(b *testing.B) {
	for _, rowsCount := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("rows%d", rowsCount), func(b *testing.B) {
			benchmarkRemoteStorageRunQuery(b, rowsCount)
		})
	}
}

func benchmarkRemoteStorageRunQuery(b *testing.B, rowsCount int) {
	rqs := buildRemoteQueryStorage(b, rowsCount)

	q, err := ParseQuery(fmt.Sprintf("_time:[%d,%d]", rqs.startNs, rqs.endNs))
	if err != nil {
		b.Fatalf("ParseQuery: %v", err)
	}

	b.SetBytes(int64(rowsCount))
	b.ResetTimer()
	for b.Loop() {
		var got int
		qctx := newTestQueryContext(rqs.tenantIDs, q)
		if err := rqs.s.RunQuery(qctx, func(_ uint, db *DataBlock) {
			got += db.RowsCount()
		}); err != nil {
			b.Fatalf("RunQuery: %v", err)
		}
		if got != rowsCount {
			b.Fatalf("unexpected rows: got %d; want %d", got, rowsCount)
		}
	}
}
