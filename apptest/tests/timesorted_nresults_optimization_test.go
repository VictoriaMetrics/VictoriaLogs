package tests

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleTimeSortedNResultsOptimization verifies that time-sorted N results optimization works correctly.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/802#issuecomment-3584878274
func TestVlsingleTimeSortedNResultsOptimization(t *testing.T) {
	fs.MustRemoveDir(t.Name())
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

			selectQueryArgs = apptest.QueryOpts{
				Start:         start,
				End:           end,
				Limit:         fmt.Sprintf("%d", limit),
				SortDirection: "asc",
			}
			got = sut.LogsQLQuery(t, "* | keep _msg, _time", selectQueryArgs)
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

// TestVlsingleTimeSortedNResultsOptimizationSortDirection verifies that both sort_direction=desc
// (default) and sort_direction=asc return rows in the expected _time order.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/924
func TestVlsingleTimeSortedNResultsOptimizationSortDirection(t *testing.T) {
	fs.MustRemoveDir(t.Name())
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

	f := func(start, end string) {
		t.Helper()

		for limit := 1; limit <= len(ingestRecords); limit++ {
			// Default (desc): the newest `limit` rows, newest-first.
			wantDescLines := make([]string, 0, limit)
			for i := len(ingestRecords) - 1; i >= len(ingestRecords)-limit; i-- {
				wantDescLines = append(wantDescLines, ingestRecords[i])
			}
			wantDescResponse := &apptest.LogsQLQueryResponse{LogLines: wantDescLines}

			selectQueryArgs := apptest.QueryOpts{
				Start: start,
				End:   end,
				Limit: fmt.Sprintf("%d", limit),
			}
			got := sut.LogsQLQuery(t, "* | keep _msg, _time", selectQueryArgs)
			assertLogsQLResponseOrdered(t, got, wantDescResponse)

			// sort_direction=asc: the oldest `limit` rows, oldest-first.
			wantAscLines := make([]string, 0, limit)
			for i := 0; i < limit; i++ {
				wantAscLines = append(wantAscLines, ingestRecords[i])
			}
			wantAscResponse := &apptest.LogsQLQueryResponse{LogLines: wantAscLines}

			selectQueryArgs = apptest.QueryOpts{
				Start:         start,
				End:           end,
				Limit:         fmt.Sprintf("%d", limit),
				SortDirection: "asc",
			}
			got = sut.LogsQLQuery(t, "* | keep _msg, _time", selectQueryArgs)
			assertLogsQLResponseOrdered(t, got, wantAscResponse)
		}
	}

	// Records at the start of the selected time range.
	f("2025-01-01T01:00:01Z", "2025-01-01T02:00:00Z")

	// Records at the end of the selected time range.
	f("2024-12-31T23:59:59Z", "2025-01-01T01:00:05.000000001Z")

	// Records exactly filling the selected time range.
	f("2025-01-01T01:00:01Z", "2025-01-01T01:00:05.000000001Z")

	// Records in the middle of a huge time range — exercises binary-search narrow/shift depth.
	f("2020-01-01T00:00:00Z", "2030-01-01T00:00:00Z")
}
