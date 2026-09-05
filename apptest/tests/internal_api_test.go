package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleInternalEndpointsRequirePOST verifies that the internal endpoints require
// the POST method in order to prevent GET-based SSRF attacks.
//
// See the related https://github.com/VictoriaMetrics/VictoriaLogs/issues/1635
func TestVlsingleInternalEndpointsRequirePOST(t *testing.T) {
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

	// Non-POST requests must be rejected with 405 both for the RPC endpoints (insert/select/delete)
	// and for the human-facing storage endpoints.
	paths := []string{
		"/internal/insert",
		"/internal/select/query",
		"/internal/delete/run_task",
		"/internal/force_merge",
	}
	for _, path := range paths {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodPatch} {
			f(method, path, http.StatusMethodNotAllowed)
		}
	}

	// A POST request must pass the method check.
	if _, statusCode := cli.Do(t, http.MethodPost, baseURL+"/internal/select/query", "", nil); statusCode == http.StatusMethodNotAllowed {
		t.Fatalf("unexpected 405 for POST /internal/select/query; POST must be allowed by the method check")
	}
}
