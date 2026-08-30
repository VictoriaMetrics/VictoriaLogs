package tests

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netselect"
	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleInternalEndpointsRequirePOST verifies that the internal endpoints require
// the POST method in order to prevent GET-based SSRF attacks.
//
// See the related https://github.com/VictoriaMetrics/VictoriaLogs/issues/1635
func TestVlsingleInternalEndpointsRequirePOST(t *testing.T) {
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

	// Non-POST requests must be rejected with 405 both for the RPC endpoints (insert/select/delete)
	// and for the human-facing storage endpoints. The deprecated /internal/* RPC paths
	// must be rejected in the same way as the /internal/rpc/* paths.
	paths := []string{
		"/internal/rpc/insert",
		"/internal/rpc/select/query",
		"/internal/rpc/delete/run_task",
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
	if _, statusCode := cli.Do(t, http.MethodPost, baseURL+"/internal/rpc/select/query", "", nil); statusCode == http.StatusMethodNotAllowed {
		t.Fatalf("unexpected 405 for POST /internal/rpc/select/query; POST must be allowed by the method check")
	}
}

// TestVlsingleDeprecatedInternalRPCPaths verifies that the internal RPC endpoints are served
// both at the /internal/rpc/* paths and at the deprecated /internal/* paths used by the previous release.
//
// This test must be removed together with the support for the deprecated /internal/* RPC paths.
func TestVlsingleDeprecatedInternalRPCPaths(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	// A request to the deprecated /internal/* path must reach the same handler as the
	// corresponding /internal/rpc/* path and return the same 200 response.
	// The insert request carries an empty body, so it adds no rows and just exercises routing.
	insertValues := url.Values{
		"version": {netinsert.ProtocolVersion},
	}
	selectValues := url.Values{
		"version":                {netselect.QueryProtocolVersion},
		"tenant_ids":             {`[{"account_id":0,"project_id":0}]`},
		"query":                  {`* | fields _msg`},
		"timestamp":              {"0"},
		"disable_compression":    {"true"},
		"allow_partial_response": {"false"},
		"hidden_fields_filters":  {"[]"},
	}

	f := func(path string, values url.Values) {
		t.Helper()
		t.Run(path, func(t *testing.T) {
			u := "http://" + sut.HTTPAddr() + path
			response, statusCode := tc.Client().PostForm(t, u, values)
			if statusCode != http.StatusOK {
				t.Fatalf("unexpected status code; got %d; want %d; response %q", statusCode, http.StatusOK, response)
			}
		})
	}

	f("/internal/insert", insertValues) // the deprecated path
	f("/internal/rpc/insert", insertValues)
	f("/internal/select/query", selectValues) // the deprecated path
	f("/internal/rpc/select/query", selectValues)
}
