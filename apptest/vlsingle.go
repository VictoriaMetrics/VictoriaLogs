package apptest

import (
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Vlsingle holds the state of single-node VictoriaLogs.
type Vlsingle struct {
	node *vlnode

	storageDataPath string
}

// MustStartVlsingle starts an instance of vlsingle with the given flags. It also
// sets the default flags and populates the app instance state with runtime
// values extracted from the application log (such as httpListenAddr).
//
// Stop must be called when the returned Vlsingle is no longer needed.
func MustStartVlsingle(t *testing.T, instance string, flags []string, cli *Client) *Vlsingle {
	t.Helper()

	storageDataPath := fmt.Sprintf("%s/%s-%d", os.TempDir(), instance, time.Now().UnixNano())
	flags = setDefaultFlags(flags, map[string]string{
		"-storageDataPath": storageDataPath,
		"-retentionPeriod": "100y",
	})
	node, extracts := mustStartVlnode(t, instance, flags, cli, []*regexp.Regexp{
		logsStorageDataPathRE,
	})

	return &Vlsingle{
		node: node,

		storageDataPath: extracts[0],
	}
}

// Stop stops app.
func (app *Vlsingle) Stop() {
	app.node.Stop()
}

type vlnode struct {
	*app
	*ServesMetrics

	httpListenAddr string
}

func mustStartVlnode(t *testing.T, instance string, flags []string, cli *Client, extraExtractREs []*regexp.Regexp) (*vlnode, []string) {
	t.Helper()

	extractREs := []*regexp.Regexp{
		httpListenAddrRE,
	}
	extractREs = append(extractREs, extraExtractREs...)

	flags = setDefaultFlags(flags, map[string]string{
		"-httpListenAddr": "127.0.0.1:0",
	})

	app, extracts := mustStartApp(t, instance, "../../bin/victoria-logs", flags, extractREs)

	node := &vlnode{
		app: app,
		ServesMetrics: &ServesMetrics{
			metricsURL: fmt.Sprintf("http://%s/metrics", extracts[0]),
			cli:        cli,
		},
		httpListenAddr: extracts[0],
	}
	return node, extracts[1:]
}

// ForceFlush is a test helper function that forces the flushing of inserted
// data, so it becomes available for searching immediately.
func (app *Vlsingle) ForceFlush(t *testing.T) {
	t.Helper()

	url := fmt.Sprintf("http://%s/internal/force_flush", app.node.httpListenAddr)
	_, statusCode := app.node.cli.Get(t, url)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected status code when querying %s: got %d, want %d", url, statusCode, http.StatusOK)
	}
}

// JSONLineWrite is a test helper function that inserts a
// collection of records in json line format by sending a HTTP
// POST request to /insert/jsonline vlsingle endpoint.
//
// See https://docs.victoriametrics.com/victorialogs/data-ingestion/#json-stream-api
func (app *Vlsingle) JSONLineWrite(t *testing.T, records []string, opts IngestOpts) {
	t.Helper()

	data := []byte(strings.Join(records, "\n"))

	url := fmt.Sprintf("http://%s/insert/jsonline", app.node.httpListenAddr)
	uv := opts.asURLValues()
	uvs := uv.Encode()
	if len(uvs) > 0 {
		url += "?" + uvs
	}

	_, statusCode := app.node.cli.Post(t, url, "text/plain", data)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected status code: got %d, want %d", statusCode, http.StatusOK)
	}
}

// NativeWrite is a test helper function that sends a collection of records
// to /insert/native API.
//
// See https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vlinsert/internalinsert/internalinsert.go
func (app *Vlsingle) NativeWrite(t *testing.T, records []logstorage.InsertRow, opts QueryOpts) {
	t.Helper()
	var data []byte
	for _, record := range records {
		data = record.Marshal(data)
	}
	dstURL := fmt.Sprintf("http://%s/insert/native", app.node.httpListenAddr)
	uv := opts.asURLValues()
	uv.Add("version", "v1")
	dstURL += "?" + uv.Encode()

	app.node.cli.Post(t, dstURL, "application/octet-stream", data)
}

// LogsQLQuery is a test helper function that performs
// PromQL/MetricsQL range query by sending a HTTP POST request to
// /select/logsql/query endpoint.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-logs
func (app *Vlsingle) LogsQLQuery(t *testing.T, query string, opts QueryOpts) *LogsQLQueryResponse {
	t.Helper()

	values := opts.asURLValues()
	values.Add("query", query)

	url := fmt.Sprintf("http://%s/select/logsql/query", app.node.httpListenAddr)
	res, _ := app.node.cli.PostForm(t, url, values)
	return NewLogsQLQueryResponse(t, res)
}

// StatsQueryRaw is a test helper function that performs
// a POST to /select/logsql/stats_query and returns raw body and status code.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-log-stats
func (app *Vlsingle) StatsQueryRaw(t *testing.T, query string, opts StatsQueryOpts) (string, int) {
	t.Helper()

	values := opts.asURLValues()
	values.Add("query", query)

	url := fmt.Sprintf("http://%s/select/logsql/stats_query", app.node.httpListenAddr)
	return app.node.cli.PostForm(t, url, values)
}

// StatsQueryRangeRaw is a test helper function that performs
// a POST to /select/logsql/stats_query_range and returns raw body and status code.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-log-range-stats
func (app *Vlsingle) StatsQueryRangeRaw(t *testing.T, query string, opts StatsQueryRangeOpts) (string, int) {
	t.Helper()

	values := opts.asURLValues()
	values.Add("query", query)

	url := fmt.Sprintf("http://%s/select/logsql/stats_query_range", app.node.httpListenAddr)
	return app.node.cli.PostForm(t, url, values)
}

// HTTPAddr returns the address at which the vmstorage process is listening
// for http connections.
func (app *Vlsingle) HTTPAddr() string {
	return app.node.httpListenAddr
}

// String returns the string representation of the vlsingle app state.
func (app *Vlsingle) String() string {
	return fmt.Sprintf("{app: %s storageDataPath: %q httpListenAddr: %q}", app.node.app, app.storageDataPath, app.node.httpListenAddr)
}
