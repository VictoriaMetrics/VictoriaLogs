package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestVlagentFileCollector(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	logsDir := t.TempDir()
	logPath := filepath.Join(logsDir, "0.log")
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("failed to get hostname: %s", err)
	}

	// Create log file before vlagent starts to avoid waiting for -fileCollector.refreshInterval (10s by default).
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create log file: %s", err)
	}
	_ = f.Close()

	sut := tc.MustStartVlsingle("vlsingle", []string{
		"-storageDataPath=" + t.TempDir(),
	})
	remoteWriteURL := fmt.Sprintf("http://%s/insert/jsonline", sut.HTTPAddr())
	_ = tc.MustStartVlagent("vlagent", []string{remoteWriteURL}, []string{
		"-fileCollector.glob=" + filepath.Join(logsDir, "*.log"),
		`-fileCollector.extraFields={"foo":"bar"}`,
		"-tmpDataPath=" + t.TempDir(),
		"-remoteWrite.format=jsonline",
	})

	// Append the first batch of logs and ensure they were processed.
	appendToFile(t, logPath, []string{
		`{"_msg":"insert file", "time":"2025-06-05T14:30:19.088007Z"}`,
		`{"_msg":"insert file", "time":"2025-06-05T14:30:19.088007Z"}`,
	})
	wantLogLines := []string{
		fmt.Sprintf(`{"_msg":"insert file","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar","file":%q,"hostname":%q}`, logPath, hostname),
		fmt.Sprintf(`{"_msg":"insert file","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar","file":%q,"hostname":%q}`, logPath, hostname),
	}
	assertLogsQLResponseEventually(tc, func() *apptest.LogsQLQueryResponse {
		sut.ForceFlush(t)
		return sut.LogsQLQuery(t, "insert file", apptest.QueryOpts{})
	}, &apptest.LogsQLQueryResponse{LogLines: wantLogLines})

	// Append the second batch of logs and ensure they were processed.
	appendToFile(t, logPath, []string{
		`{"_msg":"insert file2", "time":"2025-06-05T14:30:19.088007Z"}`,
		`{"_msg":"insert file2", "time":"2025-06-05T14:30:19.088007Z"}`,
	})
	wantLogLines = []string{
		fmt.Sprintf(`{"_msg":"insert file2","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar","file":%q,"hostname":%q}`, logPath, hostname),
		fmt.Sprintf(`{"_msg":"insert file2","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar","file":%q,"hostname":%q}`, logPath, hostname),
	}
	assertLogsQLResponseEventually(tc, func() *apptest.LogsQLQueryResponse {
		sut.ForceFlush(t)
		return sut.LogsQLQuery(t, "insert file2", apptest.QueryOpts{})
	}, &apptest.LogsQLQueryResponse{LogLines: wantLogLines})
}

func appendToFile(t *testing.T, filePath string, lines []string) {
	t.Helper()
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %s", err)
	}
	if _, err := f.WriteString(strings.Join(lines, "\n") + "\n"); err != nil {
		t.Fatalf("failed to write to file: %s", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("failed to sync file: %s", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %s", err)
	}
}
