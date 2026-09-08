package apptest

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Vlagent holds the state of a vlagent app and provides vlagent-specific functions
type Vlagent struct {
	*app
	*ServesMetrics

	remoteStoragesCount int
	httpListenAddr      string
}

// MustStartVlagent starts an instance of vlagent with the given flags.
// It also sets the default flags and populates the app instance state with runtime
// values extracted from the application log (such as httpListenAddr)
func MustStartVlagent(t *testing.T, instance string, remoteWriteURLs []string, flags []string, cli *Client) *Vlagent {
	t.Helper()

	extractREs := []*regexp.Regexp{
		httpListenAddrRE,
	}

	flags = setDefaultFlags(flags, map[string]string{
		"-httpListenAddr":            "127.0.0.1:0",
		"-remoteWrite.url":           strings.Join(remoteWriteURLs, ","),
		"-remoteWrite.tmpDataPath":   t.TempDir(),
		"-remoteWrite.flushInterval": "10ms",
		"-remoteWrite.showURL":       "true",
	})
	app, extracts := mustStartApp(t, instance, "../../bin/vlagent-race", flags, extractREs)

	return &Vlagent{
		app:                 app,
		remoteStoragesCount: len(remoteWriteURLs),
		ServesMetrics: &ServesMetrics{
			metricsURL: fmt.Sprintf("http://%s/metrics", extracts[0]),
			cli:        cli,
		},
		httpListenAddr: extracts[0],
	}
}

// JSONLineWrite is a test helper function that inserts a
// collection of records in json line format by sending a HTTP
// POST request to /insert/jsonline vlagent endpoint.
//
// See https://docs.victoriametrics.com/victorialogs/data-ingestion/#json-stream-api
func (app *Vlagent) JSONLineWrite(t *testing.T, records []string, opts IngestOpts) {
	t.Helper()

	data := []byte(strings.Join(records, "\n"))

	url := fmt.Sprintf("http://%s/insert/jsonline", app.httpListenAddr)
	uv := opts.asURLValues()
	uvs := uv.Encode()
	if len(uvs) > 0 {
		url += "?" + uvs
	}
	app.sendBlocking(t, len(records), func() {
		_, statusCode := app.cli.PostWithTenant(t, opts.AccountID, opts.ProjectID, url, "text/plain", data)
		if statusCode != http.StatusOK {
			t.Fatalf("unexpected status code: got %d, want %d", statusCode, http.StatusOK)
		}
	})
}

// sendBlocking executes send and waits until the data is added to every remote
// write queue.
//
// vlagent does not send the data immediately. It first puts the data into a
// buffer. Then a background goroutine takes the data from the buffer and sends it
// to the vmstorage. This happens every 1s by default.
//
// Waiting is implemented by retrieving the value of `vlagent_remotewrite_block_size_rows_sum`
// metric and checking whether it is equal or greater than the wanted value.
// This doesn't guarantee that remote storage has received the data.
//
// Unreliable if the records are inserted concurrently.
func (app *Vlagent) sendBlocking(t *testing.T, numRecordsToSend int, send func()) {
	t.Helper()

	// Take the current counter value before calling send(), since it may be updated
	// concurrently with the request execution. See TestVlagentRemoteWriteReplication
	// flakiness in CI when the counter is read after send().
	rowsPushed := app.remoteWriteRowsPushed(t)

	send()

	const (
		retries = 50
		period  = 100 * time.Millisecond
	)
	// take in account data replication
	wantRowsSentCount := rowsPushed + numRecordsToSend*app.remoteStoragesCount
	for range retries {
		if app.remoteWriteRowsPushed(t) >= wantRowsSentCount {
			return
		}
		time.Sleep(period)
	}
	t.Fatalf("timed out while waiting for inserted rows to be sent to remote storage")
}

func (app *Vlagent) remoteWriteRowsPushed(t *testing.T) int {
	total := 0.0
	for _, v := range app.GetMetricsByPrefix(t, "vlagent_remotewrite_block_size_rows_sum") {
		total += v
	}
	return int(total)
}
