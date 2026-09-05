package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleDeleteAPIRequiresPOST verifies that the log deletion endpoint requires the POST method.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1635
func TestVlsingleDeleteAPIRequiresPOST(t *testing.T) {
	fs.MustRemoveDir(t.Name())

	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartVlsingle("vlsingle", []string{
		"-delete.enable=true",
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
		f(method, "/delete/run_task", http.StatusMethodNotAllowed)
	}

	// A POST request must be accepted.
	f(http.MethodPost, "/delete/run_task?filter=app:del", http.StatusOK)
}

// TestVlsingleDeleteAPIAuthKey verifies that the -deleteAuthKey command-line flag protects the /delete/* endpoints.
func TestVlsingleDeleteAPIAuthKey(t *testing.T) {
	fs.MustRemoveDir(t.Name())

	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartVlsingle("vlsingle", []string{
		"-delete.enable=true",
		"-deleteAuthKey=top-secret",
	})
	cli := tc.Client()
	baseURL := "http://" + sut.HTTPAddr()

	f := func(method, path string, wantStatus int) {
		t.Helper()
		if body, statusCode := cli.Do(t, method, baseURL+path, "", nil); statusCode != wantStatus {
			t.Fatalf("unexpected status code for %s %s: got %d; want %d; body\n%s", method, path, statusCode, wantStatus, body)
		}
	}

	// Requests without the matching authKey must be rejected with 401, including the read-only /delete/active_tasks.
	f(http.MethodPost, "/delete/run_task?filter=app:del", http.StatusUnauthorized)
	f(http.MethodPost, "/delete/run_task?filter=app:del&authKey=wrong", http.StatusUnauthorized)
	f(http.MethodGet, "/delete/active_tasks?authKey=wrong", http.StatusUnauthorized)

	// Requests with the matching authKey must be accepted.
	f(http.MethodPost, "/delete/run_task?filter=app:del&authKey=top-secret", http.StatusOK)
}
