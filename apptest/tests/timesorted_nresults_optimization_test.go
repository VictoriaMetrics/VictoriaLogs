package tests

import (
	"fmt"
	"net/http"
	"strings"
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

		selectQueryArgs := apptest.QueryOpts{
			Start:  start,
			End:    end,
			Limit:  "2",
			Offset: "1",
		}
		got := sut.LogsQLQuery(t, "* | keep _msg, _time", selectQueryArgs)
		wantDescResponse := &apptest.LogsQLQueryResponse{
			LogLines: []string{
				ingestRecords[3],
				ingestRecords[2],
			},
		}
		assertLogsQLResponseOrdered(t, got, wantDescResponse)

		selectQueryArgs = apptest.QueryOpts{
			Start:         start,
			End:           end,
			Limit:         "2",
			Offset:        "1",
			SortDirection: "asc",
		}
		got = sut.LogsQLQuery(t, "* | keep _msg, _time", selectQueryArgs)
		wantAscResponse := &apptest.LogsQLQueryResponse{
			LogLines: []string{
				ingestRecords[1],
				ingestRecords[2],
			},
		}
		assertLogsQLResponseOrdered(t, got, wantAscResponse)
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

func TestVlsingleTimeSortedNResultsOptimizationSortDirectionValidation(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	f := func(name string, opts apptest.QueryOpts) {
		t.Helper()

		t.Run(name, func(t *testing.T) {
			response, statusCode := sut.LogsQLQueryRaw(t, "*", opts)
			if statusCode != http.StatusBadRequest {
				t.Fatalf("unexpected status code; got %d; want %d; response body\n%s", statusCode, http.StatusBadRequest, response)
			}
			errMsg := `unexpected sort_direction="bad"; expecting 'asc', 'desc' or ''`
			if !strings.Contains(response, errMsg) {
				t.Fatalf("unexpected response body\ngot\n%s\nwant to contain\n%s", response, errMsg)
			}
		})
	}

	f("with_limit", apptest.QueryOpts{
		Limit:         "1",
		SortDirection: "bad",
	})
	f("without_limit", apptest.QueryOpts{
		SortDirection: "bad",
	})
}
