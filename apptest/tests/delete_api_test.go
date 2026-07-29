package tests

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleDeleteAPIRequiresPOST verifies that the data-destructive deletion endpoints
// reject non-POST requests without deleting logs.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1635
func TestVlsingleDeleteAPIRequiresPOST(t *testing.T) {
	fs.MustRemoveDir(t.Name())

	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartVlsingle("vlsingle", []string{
		"-delete.enable=true",
		"-internaldelete.enable=true",
	})

	sut.JSONLineWrite(t, []string{
		`{"_msg":"delete me","_time":"2025-01-01T01:00:00Z","app":"del"}`,
	}, apptest.IngestOpts{})
	sut.ForceFlush(t)

	args := url.Values{}
	args.Set("filter", "app:del")
	args.Set("version", "v2")
	args.Set("task_id", "test-1635")
	args.Set("timestamp", "9999999999999999")
	args.Set("tenant_ids", `[{"AccountID":0,"ProjectID":0}]`)

	httpCli := &http.Client{}
	f := func(method, path string) {
		t.Helper()

		reqURL := "http://" + sut.HTTPAddr() + path + "?" + args.Encode()
		req, err := http.NewRequest(method, reqURL, nil)
		if err != nil {
			t.Fatalf("cannot create %s request to %s: %s", method, path, err)
		}
		resp, err := httpCli.Do(req)
		if err != nil {
			t.Fatalf("cannot send %s request to %s: %s", method, path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("unexpected status code for %s %s: got %d; want %d", method, path, resp.StatusCode, http.StatusMethodNotAllowed)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		f(method, "/delete/run_task")
		f(method, "/internal/delete/run_task")
	}

	// The rejected requests must not delete anything.
	if logLines := sut.LogsQLQuery(t, "app:del", apptest.QueryOpts{}).LogLines; len(logLines) != 1 {
		t.Fatalf("unexpected number of log lines after the rejected requests: got %d; want 1", len(logLines))
	}
}
