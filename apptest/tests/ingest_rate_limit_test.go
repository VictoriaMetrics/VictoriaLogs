package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestVlsingleIngestionRateLimit(t *testing.T) {
	f := func(t *testing.T, flags []string) {
		t.Helper()

		fs.MustRemoveDir(t.Name())
		tc := apptest.NewTestCase(t)
		defer tc.Stop()

		sut := tc.MustStartVlsingle("vlsingle", flags)
		sut.JSONLineWrite(t, jsonLines(3), apptest.IngestOpts{})

		startTime := time.Now()
		sut.JSONLineWrite(t, jsonLines(1), apptest.IngestOpts{})
		if d := time.Since(startTime); d < time.Second {
			t.Fatalf("the data ingestion took %s; it must be throttled for at least 1s", d)
		}
	}
	t.Run("maxLogsPerSecond", func(t *testing.T) {
		f(t, []string{"-insert.maxLogsPerSecond=1"})
	})

	t.Run("maxBytesPerSecond", func(t *testing.T) {
		f(t, []string{"-insert.maxBytesPerSecond=50"})
	})
}

func jsonLines(linesCount int) []string {
	lines := make([]string, linesCount)
	for i := range lines {
		lines[i] = fmt.Sprintf(`{"_msg":"rate limit test %d"}`, i)
	}
	return lines
}
