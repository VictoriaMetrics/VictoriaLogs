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
	f := func(reqURL string, wantStatus int) {
		t.Helper()
		if body, statusCode := cli.Do(t, http.MethodPost, reqURL, "", nil); statusCode != wantStatus {
			t.Fatalf("unexpected status code for POST %s: got %d; want %d; body\n%s", reqURL, statusCode, wantStatus, body)
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
		f(baseURL+p.path, http.StatusUnauthorized)
		f(basicAuthURL+p.path, http.StatusUnauthorized)
		f(baseURL+p.path+"?authKey=wrong", http.StatusUnauthorized)
		f(baseURL+p.path+"?authKey="+p.authKey, http.StatusOK)
	}

	// The remaining paths must still require the -httpAuth.* credentials,
	// including the paths ending with /reload, which used to skip HTTP Basic Auth.
	f(baseURL+"/select/logsql/query?query=*", http.StatusUnauthorized)
	f(basicAuthURL+"/select/logsql/query?query=*", http.StatusOK)
	f(baseURL+"/select/vmalert/-/reload", http.StatusUnauthorized)

	// The paths must fall back to -httpAuth.* when the corresponding -*AuthKey isn't set.
	sut = tc.MustStartVlsingle("vlsingle-basicauth", []string{
		"-httpAuth.username=user",
		"-httpAuth.password=pass",
		"-delete.enable=true",
	})
	baseURL = "http://" + sut.HTTPAddr()
	basicAuthURL = "http://user:pass@" + sut.HTTPAddr()
	for _, p := range paths {
		f(baseURL+p.path, http.StatusUnauthorized)
		f(basicAuthURL+p.path, http.StatusOK)
	}
}
