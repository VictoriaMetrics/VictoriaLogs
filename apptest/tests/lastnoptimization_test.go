package tests

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleLastnOptimization verifies that last N optimization works correctly.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/802#issuecomment-3584878274
func TestVlsingleLastnOptimization(t *testing.T) {
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

func TestVlsingleLastnOptimizationWithUnpackedInvalidTime(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	sut.JSONLineWrite(t, []string{
		`{"_msg":"{\"_time\":\"not-a-timestamp\",\"msg\":\"bad\"}","_time":"2025-01-01T01:00:00Z"}`,
	}, apptest.IngestOpts{})
	sut.ForceFlush(t)

	query := `* | unpack_json | keep _msg, _time | sort by (_time desc) limit 1`

	got := sut.LogsQLQuery(t, query, apptest.QueryOpts{
		Start: "2025-01-01T01:00:00Z",
		End:   "2025-01-01T01:00:00.000000001Z",
	})
	wantResponse := &apptest.LogsQLQueryResponse{
		LogLines: []string{
			`{"_msg":"{\"_time\":\"not-a-timestamp\",\"msg\":\"bad\"}","_time":"not-a-timestamp"}`,
		},
	}
	assertLogsQLResponseEqual(t, got, wantResponse)

	res, statusCode := sut.LogsQLQueryRaw(t, query, apptest.QueryOpts{
		Start: "2025-01-01T01:00:00Z",
		End:   "2025-01-01T01:00:03Z",
	})
	if statusCode != 200 {
		t.Fatalf("unexpected response status code for optimized query: %d; response\n%s", statusCode, res)
	}
	got = apptest.NewLogsQLQueryResponse(t, res)
	assertLogsQLResponseEqual(t, got, wantResponse)
}
