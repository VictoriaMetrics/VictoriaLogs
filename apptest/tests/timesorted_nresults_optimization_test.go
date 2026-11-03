package tests

import (
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleTimeSortedNResultsOptimization verifies that time-sorted N results optimization works correctly.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/802#issuecomment-3584878274
func TestVlsingleTimeSortedNResultsOptimization(t *testing.T) {
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

// TestVlsingleTimeSortedNResultsOptimizationIdenticalTimestamps verifies that time-sorted N results optimization returns all the needed logs
// when many of them share the timestamp at the start of the selected time range or at the point where binary search splits it.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/pull/1882
func TestVlsingleTimeSortedNResultsOptimizationIdenticalTimestamps(t *testing.T) {
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

	f := func(start, end time.Time, limit int, sortDirection string, wantTimestamps []time.Time) {
		t.Helper()

		var logLines []string
		for _, timestamp := range wantTimestamps {
			logLines = append(logLines, fmt.Sprintf(`{"_time":%q}`, timestamp.Format(time.RFC3339Nano)))
		}
		wantResponse := &apptest.LogsQLQueryResponse{
			LogLines: logLines,
		}

		selectQueryArgs := apptest.QueryOpts{
			Start:         start.Format(time.RFC3339Nano),
			End:           end.Format(time.RFC3339Nano),
			Limit:         fmt.Sprintf("%d", limit),
			SortDirection: sortDirection,
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
	f(rangeStart, rangeEnd, 3, "", []time.Time{newer, rangeStart, rangeStart})

	// verify the case when some of the needed logs are located at the point where binary search splits the selected time range
	splitPoint := time.Date(2025, 1, 2, 1, 0, 0, 0, time.UTC)
	rangeStart = splitPoint.Add(-30 * time.Second)
	rangeEnd = splitPoint.Add(30 * time.Second)
	newer = splitPoint.Add(time.Second)
	ingestLogs(splitPoint, 10)
	ingestLogs(newer, 2)
	f(rangeStart, rangeEnd, 6, "", []time.Time{newer, newer, splitPoint, splitPoint, splitPoint, splitPoint})

	// verify the case when some of the needed logs are located at the end of the selected time range in asc order.
	// The time range ends at an even nanosecond, since binary search may skip the last nanosecond of such a time range.
	rangeEnd = time.Date(2025, 1, 3, 1, 1, 0, 0, time.UTC)
	rangeStart = rangeEnd.Add(-time.Minute)
	lastNsec := rangeEnd.Add(-time.Nanosecond)
	older := rangeStart.Add(time.Second)
	ingestLogs(lastNsec, 10)
	ingestLogs(older, 1)
	f(rangeStart, rangeEnd, 3, "asc", []time.Time{older, lastNsec, lastNsec})

	// verify the case when some of the needed logs are located right before the point where binary search splits the selected time range in asc order
	splitPoint = time.Date(2025, 1, 4, 1, 0, 0, 0, time.UTC)
	rangeStart = splitPoint.Add(-30 * time.Second)
	rangeEnd = splitPoint.Add(30 * time.Second)
	beforeSplit := splitPoint.Add(-time.Nanosecond)
	older = splitPoint.Add(-time.Second)
	ingestLogs(beforeSplit, 10)
	ingestLogs(older, 2)
	f(rangeStart, rangeEnd, 6, "asc", []time.Time{older, older, beforeSplit, beforeSplit, beforeSplit, beforeSplit})
}

// TestVlsingleTimeSortedNResultsOptimizationSortDirection verifies that both sort_direction=desc
// (default) and sort_direction=asc return rows in the expected _time order.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/924
func TestVlsingleTimeSortedNResultsOptimizationSortDirection(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ingestRecords := []string{
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:01Z"}`,
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:02Z"}`,
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:03Z"}`,
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:04Z"}`,
		`{"_msg":"Hello, VictoriaLogs!", "_time":"2025-01-01T01:00:05Z"}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{})
	sut.ForceFlush(t)

	recordsDesc := slices.Clone(ingestRecords)
	slices.Reverse(recordsDesc)

	f := func(start, end string) {
		t.Helper()

		check := func(sortDirection string, offset, limit int, want []string) {
			t.Helper()

			selectQueryArgs := apptest.QueryOpts{
				Start:         start,
				End:           end,
				Limit:         fmt.Sprintf("%d", limit),
				Offset:        fmt.Sprintf("%d", offset),
				SortDirection: sortDirection,
			}
			got := sut.LogsQLQuery(t, "* | keep _msg, _time", selectQueryArgs)
			assertLogsQLResponseOrdered(t, got, &apptest.LogsQLQueryResponse{LogLines: want})
		}

		for limit := 1; limit <= len(ingestRecords); limit++ {
			check("", 0, limit, recordsDesc[:limit])
			check("asc", 0, limit, ingestRecords[:limit])
		}
		check("", 1, 2, recordsDesc[1:3])
		check("asc", 1, 2, ingestRecords[1:3])
	}

	// Records at the start of the selected time range.
	f("2025-01-01T01:00:01Z", "2025-01-01T02:00:00Z")

	// Records at the end of the selected time range.
	f("2024-12-31T23:59:59Z", "2025-01-01T01:00:05.000000001Z")

	// Records exactly filling the selected time range.
	f("2025-01-01T01:00:01Z", "2025-01-01T01:00:05.000000001Z")

	// Records in the middle of a huge time range, which exercises binary-search narrow/shift depth.
	f("2020-01-01T00:00:00Z", "2030-01-01T00:00:00Z")
}

// TestVlsingleTimeSortedNResultsOptimizationSortDirectionValidation verifies that sort_direction is rejected if it is invalid or is passed without limit.
func TestVlsingleTimeSortedNResultsOptimizationSortDirectionValidation(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	f := func(opts apptest.QueryOpts) {
		t.Helper()

		response, statusCode := sut.LogsQLQueryRaw(t, "*", opts)
		if statusCode != http.StatusBadRequest {
			t.Fatalf("unexpected status code; got %d; want %d; response body\n%s", statusCode, http.StatusBadRequest, response)
		}
	}

	// invalid value
	f(apptest.QueryOpts{
		Limit:         "1",
		SortDirection: "bad",
	})

	// without limit
	f(apptest.QueryOpts{
		SortDirection: "asc",
	})
}
