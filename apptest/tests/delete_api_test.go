package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleDeleteAPIRequiresPOST verifies that the log deletion endpoints require the POST method.
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
	cli := tc.Client()
	baseURL := "http://" + sut.HTTPAddr()

	f := func(method, path string, wantStatus int) {
		t.Helper()
		if body, statusCode := cli.Do(t, method, baseURL+path, "", nil); statusCode != wantStatus {
			t.Fatalf("unexpected status code for %s %s: got %d; want %d; body\n%s", method, path, statusCode, wantStatus, body)
		}
	}

	// Non-POST requests must be rejected with 405 on both the public and the internal endpoints.
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		f(method, "/delete/run_task", http.StatusMethodNotAllowed)
		f(method, "/internal/delete/run_task", http.StatusMethodNotAllowed)
	}

	// A POST request must be accepted.
	f(http.MethodPost, "/delete/run_task?filter=app:del", http.StatusOK)
}
