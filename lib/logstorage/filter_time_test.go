package logstorage

import (
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestFilterTime(t *testing.T) {
	t.Parallel()

	timestamps := []int64{
		1,
		9,
		123,
		456,
		789,
	}

	// match
	ft := newFilterTime(-10, 1, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0})

	ft = newFilterTime(-10, 10, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0, 1})

	ft = newFilterTime(1, 1, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0})

	ft = newFilterTime(2, 456, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{1, 2, 3})

	ft = newFilterTime(2, 457, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{1, 2, 3})

	ft = newFilterTime(120, 788, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{2, 3})

	ft = newFilterTime(120, 789, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{2, 3, 4})

	ft = newFilterTime(120, 10000, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{2, 3, 4})

	ft = newFilterTime(789, 1000, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{4})

	// mismatch
	ft = newFilterTime(-1000, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, nil)

	ft = newFilterTime(790, 1000, "")
	testFilterMatchForTimestamps(t, timestamps, ft, nil)
}

func testFilterMatchForTimestamps(t *testing.T, timestamps []int64, f filter, expectedRowIdxs []int) {
	t.Helper()

	// Create the test storage
	storagePath := t.Name()
	cfg := &StorageConfig{
		Retention: 100 * 365 * time.Duration(nsecsPerDay),
	}
	s := MustOpenStorage(storagePath, cfg)

	// Generate rows
	getValue := func(rowIdx int) string {
		return fmt.Sprintf("some value for row %d", rowIdx)
	}
	tenantID := TenantID{
		AccountID: 123,
		ProjectID: 456,
	}
	generateRowsFromTimestamps(s, tenantID, timestamps, getValue)

	expectedResults := make([]string, len(expectedRowIdxs))
	expectedTimestamps := make([]int64, len(expectedRowIdxs))
	for i, idx := range expectedRowIdxs {
		expectedResults[i] = getValue(idx)
		expectedTimestamps[i] = timestamps[idx]
	}

	testFilterMatchForStorage(t, s, tenantID, f, "_msg", expectedResults, expectedTimestamps)

	// Close and delete the test storage
	s.MustClose()
	fs.MustRemoveDir(storagePath)
}

func generateRowsFromTimestamps(s *Storage, tenantID TenantID, timestamps []int64, getValue func(rowIdx int) string) {
	lr := GetLogRows(nil, nil, nil, nil, "")
	var fields []Field
	for i, timestamp := range timestamps {
		fields = append(fields[:0], Field{
			Name:  "_msg",
			Value: getValue(i),
		})
		lr.mustAdd(tenantID, timestamp, fields)
	}
	s.MustAddRows(lr)
	PutLogRows(lr)
	s.DebugFlush()
}
