package tests

import (
	"encoding/json"
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

	// Non-POST requests must be rejected with 405 on both the public and the internal endpoints.
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		for _, path := range []string{"/delete/run_task", "/internal/delete/run_task"} {
			if _, statusCode := cli.Do(t, method, baseURL+path, "", nil); statusCode != http.StatusMethodNotAllowed {
				t.Fatalf("unexpected status code for %s %s: got %d; want %d", method, path, statusCode, http.StatusMethodNotAllowed)
			}
		}
	}

	// The rejected requests must not have created any deletion task.
	if n := activeDeleteTasks(t, cli, baseURL); n != 0 {
		t.Fatalf("unexpected number of active deletion tasks after the rejected requests: got %d; want 0", n)
	}

	// A POST request must be accepted.
	if body, statusCode := cli.Post(t, baseURL+"/delete/run_task?filter=app:del", "", nil); statusCode != http.StatusOK {
		t.Fatalf("unexpected status code for POST /delete/run_task: got %d; want %d; body\n%s", statusCode, http.StatusOK, body)
	}
}

// activeDeleteTasks returns the number of active deletion tasks reported by /delete/active_tasks.
func activeDeleteTasks(t *testing.T, cli *apptest.Client, baseURL string) int {
	t.Helper()

	body, statusCode := cli.Get(t, baseURL+"/delete/active_tasks")
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected status code for /delete/active_tasks: got %d; want %d; body\n%s", statusCode, http.StatusOK, body)
	}
	var tasks []any
	if err := json.Unmarshal([]byte(body), &tasks); err != nil {
		t.Fatalf("cannot parse /delete/active_tasks response %q: %s", body, err)
	}
	return len(tasks)
}
