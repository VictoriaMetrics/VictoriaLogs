package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleInternalSelectAPIRequiresPOST verifies that the internal select and delete endpoints require the POST method.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1635
func TestVlsingleInternalSelectAPIRequiresPOST(t *testing.T) {
	fs.MustRemoveDir(t.Name())

	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartVlsingle("vlsingle", []string{
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

	// Non-POST requests must be rejected with 405.
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		f(method, "/internal/select/query", http.StatusMethodNotAllowed)
		f(method, "/internal/delete/run_task", http.StatusMethodNotAllowed)
	}

	// A POST request must not be rejected with 405.
	if _, statusCode := cli.Do(t, http.MethodPost, baseURL+"/internal/select/query", "", nil); statusCode == http.StatusMethodNotAllowed {
		t.Fatalf("unexpected 405 for POST /internal/select/query; POST must be allowed by the method check")
	}
}
