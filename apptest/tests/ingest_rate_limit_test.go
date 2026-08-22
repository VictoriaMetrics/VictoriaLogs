package tests

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleIngestionRateLimit verifies that the ingestion rate limits
// set via -insert.maxLogsPerSecond and -insert.maxBytesPerSecond are applied
// to the data ingestion requests sent over HTTP.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/887
func TestVlsingleIngestionRateLimit(t *testing.T) {
	f := func(t *testing.T, flags []string, linesCount int) {
		t.Helper()

		fs.MustRemoveDir(t.Name())
		tc := apptest.NewTestCase(t)
		defer tc.Stop()

		sut := tc.MustStartVlsingle("vlsingle", flags)
		url := fmt.Sprintf("http://%s/insert/jsonline", sut.HTTPAddr())

		// Ingest more data than the configured per-second limit allows.
		// The request itself must succeed, since the limit is checked before the request is processed.
		statusCode, _ := mustPostJSONLine(t, url, jsonLines(linesCount))
		if statusCode != http.StatusOK {
			t.Fatalf("unexpected status code for the initial data ingestion request; got %d; want %d", statusCode, http.StatusOK)
		}

		// The next request must be rejected with 429 and the Retry-After header,
		// since the configured per-second limit is already exceeded.
		statusCode, retryAfter := mustPostJSONLine(t, url, jsonLines(1))
		if statusCode != http.StatusTooManyRequests {
			t.Fatalf("unexpected status code for the throttled data ingestion request; got %d; want %d", statusCode, http.StatusTooManyRequests)
		}
		if retryAfter == "" {
			t.Fatalf("missing Retry-After header in the response with the %d status code", http.StatusTooManyRequests)
		}
	}

	// the limit on the number of ingested log entries per second
	t.Run("maxLogsPerSecond", func(t *testing.T) {
		f(t, []string{"-insert.maxLogsPerSecond=1"}, 100)
	})

	// the limit on the number of ingested bytes per second
	t.Run("maxBytesPerSecond", func(t *testing.T) {
		f(t, []string{"-insert.maxBytesPerSecond=10"}, 100)
	})

	// both limits at once
	t.Run("bothLimits", func(t *testing.T) {
		f(t, []string{"-insert.maxLogsPerSecond=1", "-insert.maxBytesPerSecond=10"}, 100)
	})
}

// TestVlsingleIngestionRateLimitDisabled verifies that the data ingestion isn't limited
// if -insert.maxLogsPerSecond and -insert.maxBytesPerSecond aren't set.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/887
func TestVlsingleIngestionRateLimitDisabled(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()
	url := fmt.Sprintf("http://%s/insert/jsonline", sut.HTTPAddr())

	for i := range 10 {
		statusCode, _ := mustPostJSONLine(t, url, jsonLines(100))
		if statusCode != http.StatusOK {
			t.Fatalf("unexpected status code for the data ingestion request #%d; got %d; want %d", i, statusCode, http.StatusOK)
		}
	}
}

// jsonLines returns linesCount log entries in the json line format.
func jsonLines(linesCount int) string {
	lines := make([]string, linesCount)
	for i := range lines {
		lines[i] = fmt.Sprintf(`{"_msg":"rate limit test %d"}`, i)
	}
	return strings.Join(lines, "\n")
}

// mustPostJSONLine sends data to the given url and returns the response status code
// together with the Retry-After response header.
func mustPostJSONLine(t *testing.T, url, data string) (int, string) {
	t.Helper()

	// Send application/json content type, since the default content type used by http.Post
	// makes net/http consume the request body when the request query args are parsed.
	resp, err := http.Post(url, "application/json", strings.NewReader(data))
	if err != nil {
		t.Fatalf("cannot send data to %s: %s", url, err)
	}
	defer resp.Body.Close()

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("cannot read response body from %s: %s", url, err)
	}
	return resp.StatusCode, resp.Header.Get("Retry-After")
}
