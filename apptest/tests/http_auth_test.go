package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleAuthKeyOverridesBasicAuth verifies that the -*AuthKey command-line flags override -httpAuth.*.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1764
func TestVlsingleAuthKeyOverridesBasicAuth(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	cli := tc.Client()
	f := func(method, reqURL string, wantStatus int) {
		t.Helper()
		if body, statusCode := cli.Do(t, method, reqURL, "", nil); statusCode != wantStatus {
			t.Fatalf("unexpected status code for %s %s: got %d; want %d; body\n%s", method, reqURL, statusCode, wantStatus, body)
		}
	}

	// Every path below returns 200 to an empty POST request.
	paths := []struct {
		path    string
		authKey string
	}{
		{"/delete/active_tasks", "delete-key"},
		{"/internal/force_merge", "merge-key"},
		{"/internal/force_flush", "flush-key"},
		{"/internal/log_new_streams", "streams-key"},
		{"/internal/partition/list", "partition-key"},
	}

	sut := tc.MustStartVlsingle("vlsingle-authkey", []string{
		"-httpAuth.username=user",
		"-httpAuth.password=pass",
		"-delete.enable=true",
		"-deleteAuthKey=delete-key",
		"-forceMergeAuthKey=merge-key",
		"-forceFlushAuthKey=flush-key",
		"-logNewStreamsAuthKey=streams-key",
		"-partitionManageAuthKey=partition-key",
	})
	baseURL := "http://" + sut.HTTPAddr()
	// net/http sends the credentials from the URL as HTTP Basic Auth.
	basicAuthURL := "http://user:pass@" + sut.HTTPAddr()

	// Requests must be accepted with the matching authKey alone and rejected otherwise.
	for _, p := range paths {
		f(http.MethodPost, baseURL+p.path, http.StatusUnauthorized)
		f(http.MethodGet, baseURL+p.path, http.StatusUnauthorized)
		f(http.MethodPost, basicAuthURL+p.path, http.StatusUnauthorized)
		f(http.MethodPost, baseURL+p.path+"?authKey=wrong", http.StatusUnauthorized)
		f(http.MethodPost, baseURL+p.path+"?authKey="+p.authKey, http.StatusOK)
	}

	// The paths without -*AuthKey must require the -httpAuth.* credentials.
	f(http.MethodPost, baseURL+"/select/logsql/query?query=*", http.StatusUnauthorized)
	f(http.MethodPost, basicAuthURL+"/select/logsql/query?query=*", http.StatusOK)
	f(http.MethodPost, baseURL+"/select/vmalert/-/reload", http.StatusUnauthorized)

	// The paths must fall back to -httpAuth.* when the corresponding -*AuthKey isn't set.
	sut = tc.MustStartVlsingle("vlsingle-basicauth", []string{
		"-httpAuth.username=user",
		"-httpAuth.password=pass",
		"-delete.enable=true",
	})
	baseURL = "http://" + sut.HTTPAddr()
	basicAuthURL = "http://user:pass@" + sut.HTTPAddr()
	for _, p := range paths {
		f(http.MethodPost, baseURL+p.path, http.StatusUnauthorized)
		f(http.MethodPost, basicAuthURL+p.path, http.StatusOK)
	}
}
