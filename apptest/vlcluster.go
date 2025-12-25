package apptest

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// Vlcluster holds the state of a VictoriaLogs cluster.
type Vlcluster struct {
	storageNodes []*Vlsingle
	insertNode   *vlnode
	selectNode   *vlnode
}

// MustStartVlcluster starts VictoriaLogs cluster with the given storageFlags for storage nodes.
// It also sets the default flags and populates the app instance state with runtime
// values extracted from the application log (such as httpListenAddr).
//
// Stop must be called on the returned Vlcluster when it is no longer needed.
func MustStartVlcluster(t *testing.T, instance string, storageFlags []string, cli *Client) *Vlcluster {
	t.Helper()

	// Start storage nodes
	storageNodeAddrs := make([]string, 3)
	storageNodes := make([]*Vlsingle, 3)
	for i := 0; i < 3; i++ {
		storageName := fmt.Sprintf("%s-storage-%d", instance, i)
		storageNodes[i] = MustStartVlsingle(t, storageName, storageFlags, cli)
		storageNodeAddrs[i] = storageNodes[i].node.httpListenAddr
	}
	storageNodeFlag := fmt.Sprintf("-storageNode=%s", strings.Join(storageNodeAddrs, ","))

	// Start insert node
	insertName := instance + "-insert"
	insertFlags := []string{
		storageNodeFlag,
		"-select.disable=true",
	}
	insertNode, _ := mustStartVlnode(t, insertName, insertFlags, cli, nil)

	// Start select node
	selectName := instance + "-select"
	selectFlags := []string{
		storageNodeFlag,
		"-insert.disable=true",
	}
	selectNode, _ := mustStartVlnode(t, selectName, selectFlags, cli, nil)

	return &Vlcluster{
		storageNodes: storageNodes,
		insertNode:   insertNode,
		selectNode:   selectNode,
	}
}

// Stop stops app.
func (app *Vlcluster) Stop() {
	for _, node := range app.storageNodes {
		node.Stop()
	}
	app.insertNode.Stop()
	app.selectNode.Stop()
}

// ForceFlush is a test helper function that forces the flushing of inserted
// data, so it becomes available for searching immediately.
func (app *Vlcluster) ForceFlush(t *testing.T) {
	t.Helper()

	url := fmt.Sprintf("http://%s/internal/force_flush", app.insertNode.httpListenAddr)

	_, statusCode := app.insertNode.cli.Get(t, url)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected status code when querying %s: got %d; want %d", url, statusCode, http.StatusOK)
	}
}

// JSONLineWrite is a test helper function that inserts a
// collection of records in json line format by sending a HTTP
// POST request to /insert/jsonline vlsingle endpoint.
//
// See https://docs.victoriametrics.com/victorialogs/data-ingestion/#json-stream-api
func (app *Vlcluster) JSONLineWrite(t *testing.T, records []string, opts IngestOpts) {
	t.Helper()

	data := []byte(strings.Join(records, "\n"))

	url := fmt.Sprintf("http://%s/insert/jsonline", app.insertNode.httpListenAddr)
	uv := opts.asURLValues()
	uvs := uv.Encode()
	if len(uvs) > 0 {
		url += "?" + uvs
	}

	_, statusCode := app.insertNode.cli.Post(t, url, "text/plain", data)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected status code when sending data to %s: got %d, want %d", url, statusCode, http.StatusOK)
	}
}

// LogsQLQuery is a test helper function that performs
// PromQL/MetricsQL range query by sending a HTTP POST request to
// /select/logsql/query endpoint.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-logs
func (app *Vlcluster) LogsQLQuery(t *testing.T, query string, opts QueryOpts) *LogsQLQueryResponse {
	t.Helper()

	values := opts.asURLValues()
	values.Add("query", query)

	url := fmt.Sprintf("http://%s/select/logsql/query", app.selectNode.httpListenAddr)
	res, statusCode := app.selectNode.cli.PostForm(t, url, values)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected status code from %s: %d; want %d", url, statusCode, http.StatusOK)
	}
	return NewLogsQLQueryResponse(t, res)
}

// Facets sends the given query to /select/logsql/facets and returns the response.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-facets
func (app *Vlcluster) Facets(t *testing.T, query string, opts FacetsOpts) string {
	t.Helper()

	values := opts.asURLValues()
	values.Add("query", query)

	url := fmt.Sprintf("http://%s/select/logsql/facets", app.selectNode.httpListenAddr)
	res, statusCode := app.selectNode.cli.PostForm(t, url, values)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected status code from %s: %d; want %d", url, statusCode, http.StatusOK)
	}
	return res
}

// String returns the string representation of the app state.
func (app *Vlcluster) String() string {
	return "Vlcluster"
}
