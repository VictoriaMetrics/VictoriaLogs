package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleIngestionRateLimit verifies that the data ingestion is throttled
// according to -insert.maxLogsPerSecond and -insert.maxBytesPerSecond command-line flags.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/887
func TestVlsingleIngestionRateLimit(t *testing.T) {
	f := func(t *testing.T, flags []string) {
		t.Helper()

		fs.MustRemoveDir(t.Name())
		tc := apptest.NewTestCase(t)
		defer tc.Stop()

		sut := tc.MustStartVlsingle("vlsingle", flags)

		// The first data ingestion isn't throttled - it just puts the rate limiter budget into debt,
		// since the budget is checked before it is decreased.
		sut.JSONLineWrite(t, jsonLines(3), apptest.IngestOpts{})

		// The next data ingestion must wait until the rate limiter budget is replenished.
		startTime := time.Now()
		sut.JSONLineWrite(t, jsonLines(1), apptest.IngestOpts{})
		if d := time.Since(startTime); d < time.Second {
			t.Fatalf("the data ingestion took %s; it must be throttled for at least 1s", d)
		}
	}

	// the limit on the number of ingested log entries per second
	t.Run("maxLogsPerSecond", func(t *testing.T) {
		f(t, []string{"-insert.maxLogsPerSecond=1"})
	})

	// the limit on the number of ingested bytes per second
	t.Run("maxBytesPerSecond", func(t *testing.T) {
		f(t, []string{"-insert.maxBytesPerSecond=50"})
	})
}

// TestVlsingleIngestionRateLimitDisabled verifies that the data ingestion isn't throttled
// if -insert.maxLogsPerSecond and -insert.maxBytesPerSecond aren't set.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/887
func TestVlsingleIngestionRateLimitDisabled(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	startTime := time.Now()
	sut.JSONLineWrite(t, jsonLines(1000), apptest.IngestOpts{})
	if d := time.Since(startTime); d > 10*time.Second {
		t.Fatalf("the data ingestion took %s; it must not be throttled", d)
	}
}

// jsonLines returns linesCount log entries in the json line format.
func jsonLines(linesCount int) []string {
	lines := make([]string, linesCount)
	for i := range lines {
		lines[i] = fmt.Sprintf(`{"_msg":"rate limit test %d"}`, i)
	}
	return lines
}
