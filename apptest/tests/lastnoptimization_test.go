package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleLastnOptimization verifies that last N optimization works correctly.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/802#issuecomment-3584878274
func TestVlsingleLastnOptimization(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ingestRecords := []string{
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:00Z"}`,
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:00Z"}`,
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:00Z"}`,
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:00Z"}`,
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:00Z"}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{})
	sut.ForceFlush(t)

	f := func(start, end string) {
		t.Helper()

		for limit := 1; limit <= 2*len(ingestRecords); limit++ {
			var logLines []string

			wantLinesCount := min(limit, len(ingestRecords))
			for i := range wantLinesCount {
				logLines = append(logLines, ingestRecords[i])
			}
			wantResponse := &apptest.LogsQLQueryResponse{
				LogLines: logLines,
			}

			selectQueryArgs := apptest.QueryOpts{
				Start: start,
				End:   end,
				Limit: fmt.Sprintf("%d", limit),
			}
			got := sut.LogsQLQuery(t, "* | keep _msg, _time", selectQueryArgs)
			assertLogsQLResponseEqual(t, got, wantResponse)
		}
	}

	// verify the case when the logs are located at the start of the selected time range
	f("2025-01-01T01:00:00Z", "2025-01-01T01:00:03Z")

	// verify the case when the logs are located in the middle of the selected time range
	f("2024-12-31T23:59:59Z", "2025-01-01T01:00:03Z")

	// verify the case when the logs are located at the end of the selected time range
	f("2024-12-31T23:59:59Z", "2025-01-01T01:00:00.000000001Z")

	// verify the case when the logs are outside the selected time range
	selectQueryArgs := apptest.QueryOpts{
		Start: "2024-12-31T23:59:59Z",
		End:   "2025-01-01T01:00:00Z",
		Limit: "3",
	}
	got := sut.LogsQLQuery(t, "* | count() x", selectQueryArgs)
	wantResponse := &apptest.LogsQLQueryResponse{
		LogLines: []string{
			`{"x":"0"}`,
		},
	}
	assertLogsQLResponseEqual(t, got, wantResponse)

	selectQueryArgs = apptest.QueryOpts{
		Start: "2025-01-01T01:00:00.000000001Z",
		End:   "2025-01-01T01:00:03Z",
		Limit: "3",
	}
	got = sut.LogsQLQuery(t, "* | count() x", selectQueryArgs)
	wantResponse = &apptest.LogsQLQueryResponse{
		LogLines: []string{
			`{"x":"0"}`,
		},
	}
	assertLogsQLResponseEqual(t, got, wantResponse)
}

// TestVlsingleLastnOptimizationIdenticalTimestamps verifies that last N optimization returns all the needed logs
// when many of them share the timestamp at the start of the selected time range or at the point where binary search splits it.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/pull/1882
func TestVlsingleLastnOptimizationIdenticalTimestamps(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ingestLogs := func(timestamp time.Time, count int) {
		t.Helper()

		var ingestRecords []string
		for range count {
			ingestRecords = append(ingestRecords, fmt.Sprintf(`{"_msg":"foo","_time":%q}`, timestamp.Format(time.RFC3339Nano)))
		}
		sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{})
		sut.ForceFlush(t)
	}

	f := func(start, end time.Time, limit int, wantTimestamps []time.Time) {
		t.Helper()

		var logLines []string
		for _, timestamp := range wantTimestamps {
			logLines = append(logLines, fmt.Sprintf(`{"_time":%q}`, timestamp.Format(time.RFC3339Nano)))
		}
		wantResponse := &apptest.LogsQLQueryResponse{
			LogLines: logLines,
		}

		selectQueryArgs := apptest.QueryOpts{
			Start: start.Format(time.RFC3339Nano),
			End:   end.Format(time.RFC3339Nano),
			Limit: fmt.Sprintf("%d", limit),
		}
		got := sut.LogsQLQuery(t, "* | keep _time", selectQueryArgs)
		assertLogsQLResponseEqual(t, got, wantResponse)
	}

	// Every case uses its own day, so the cases don't interfere with each other.

	// verify the case when some of the needed logs are located at the start of the selected time range.
	// The time range starts at an odd nanosecond, since binary search may skip the first nanosecond of such a time range.
	rangeStart := time.Date(2025, 1, 1, 1, 0, 0, 1, time.UTC)
	rangeEnd := rangeStart.Add(time.Minute)
	newer := rangeEnd.Add(-time.Second)
	ingestLogs(rangeStart, 10)
	ingestLogs(newer, 1)
	f(rangeStart, rangeEnd, 3, []time.Time{newer, rangeStart, rangeStart})

	// verify the case when some of the needed logs are located at the point where binary search splits the selected time range
	splitPoint := time.Date(2025, 1, 2, 1, 0, 0, 0, time.UTC)
	rangeStart = splitPoint.Add(-30 * time.Second)
	rangeEnd = splitPoint.Add(30 * time.Second)
	newer = splitPoint.Add(time.Second)
	ingestLogs(splitPoint, 10)
	ingestLogs(newer, 2)
	f(rangeStart, rangeEnd, 6, []time.Time{newer, newer, splitPoint, splitPoint, splitPoint, splitPoint})
}
