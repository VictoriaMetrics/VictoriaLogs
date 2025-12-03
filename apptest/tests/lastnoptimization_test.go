package tests

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlssingleLastnOptimization verifies that last N optimization works correctly.
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

	for limit := 1; limit <= 2*len(ingestRecords); limit++ {
		var logLines []string

		wantLinesCount := limit
		if wantLinesCount > len(ingestRecords) {
			wantLinesCount = len(ingestRecords)
		}
		for i := 0; i < wantLinesCount; i++ {
			logLines = append(logLines, ingestRecords[i])
		}
		wantResponse := &apptest.LogsQLQueryResponse{
			LogLines: logLines,
		}

		selectQueryArgs := apptest.QueryOpts{
			Start: "2025-01-01T01:00:00Z",
			End:   "2025-01-01T01:00:03Z",
			Limit: fmt.Sprintf("%d", limit),
		}
		got := sut.LogsQLQuery(t, "* | keep _msg, _time", selectQueryArgs)
		assertLogsQLResponseEqual(t, got, wantResponse)
	}
}
