package tests

import (
	"net/http"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleMultitenantQuery verifies /select/multitenant/logsql/query at single-node VictoriaLogs.
//
// See https://docs.victoriametrics.com/victorialogs/#multitenant-querying
func TestVlsingleMultitenantQuery(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartVlsingle("vlsingle", []string{"-multitenantselect.enable=true"})

	testMultitenantQuery(t, sut)
}

// TestVlclusterMultitenantQuery verifies /select/multitenant/logsql/query at VictoriaLogs cluster.
//
// See https://docs.victoriametrics.com/victorialogs/#multitenant-querying
func TestVlclusterMultitenantQuery(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartVlcluster("vlcluster", nil, []string{"-multitenantselect.enable=true"})

	testMultitenantQuery(t, sut)
}

// TestVlsingleMultitenantQueryDisabled verifies that /select/multitenant/logsql/* endpoints are disabled by default.
func TestVlsingleMultitenantQueryDisabled(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	res, statusCode := sut.LogsQLQueryRaw(t, "vl_account_id:1", apptest.QueryOpts{
		MultiTenant: true,
	})
	if statusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status code; got %d; want %d; response: %s", statusCode, http.StatusBadRequest, res)
	}
	if !strings.Contains(res, "-multitenantselect.enable") {
		t.Fatalf("unexpected response; it must mention -multitenantselect.enable; response: %s", res)
	}
}

type multitenantQueryApp interface {
	JSONLineWrite(t *testing.T, records []string, opts apptest.IngestOpts)
	ForceFlush(t *testing.T)
	LogsQLQuery(t *testing.T, query string, opts apptest.QueryOpts) *apptest.LogsQLQueryResponse
	LogsQLQueryRaw(t *testing.T, query string, opts apptest.QueryOpts) (string, int)
}

func testMultitenantQuery(t *testing.T, sut multitenantQueryApp) {
	t.Helper()

	ingest := func(accountID, projectID string, records []string) {
		t.Helper()
		sut.JSONLineWrite(t, records, apptest.IngestOpts{
			AccountID: accountID,
			ProjectID: projectID,
		})
	}
	ingest("1", "11", []string{
		`{"_msg":"a1","user":"alice","_time":"2025-01-01T01:00:00Z"}`,
		`{"_msg":"a2","user":"bob","_time":"2025-01-01T01:00:01Z"}`,
	})
	ingest("1", "12", []string{
		`{"_msg":"b1","user":"carol","_time":"2025-01-01T01:00:02Z"}`,
	})
	ingest("2", "21", []string{
		`{"_msg":"c1","user":"alice","_time":"2025-01-01T01:00:03Z"}`,
	})
	sut.ForceFlush(t)

	opts := apptest.QueryOpts{
		MultiTenant: true,
	}

	f := func(query string, responseExpected []string) {
		t.Helper()

		got := sut.LogsQLQuery(t, query+" | keep _msg, vl_account_id, vl_project_id", opts)
		wantResponse := &apptest.LogsQLQueryResponse{
			LogLines: responseExpected,
		}
		assertLogsQLResponseEqual(t, got, wantResponse)
	}

	// all the projects for the given account
	f(`vl_account_id:1`, []string{
		`{"_msg":"a1","vl_account_id":"1","vl_project_id":"11"}`,
		`{"_msg":"a2","vl_account_id":"1","vl_project_id":"11"}`,
		`{"_msg":"b1","vl_account_id":"1","vl_project_id":"12"}`,
	})

	// a single tenant
	f(`vl_account_id:1 vl_project_id:12`, []string{
		`{"_msg":"b1","vl_account_id":"1","vl_project_id":"12"}`,
	})

	// multiple accounts
	f(`vl_account_id:in(1,2) user:alice`, []string{
		`{"_msg":"a1","vl_account_id":"1","vl_project_id":"11"}`,
		`{"_msg":"c1","vl_account_id":"2","vl_project_id":"21"}`,
	})

	// the given project across all the accounts
	f(`vl_project_id:21`, []string{
		`{"_msg":"c1","vl_account_id":"2","vl_project_id":"21"}`,
	})

	// the subquery selects tenants on its own
	f(`vl_account_id:1 user:in(vl_account_id:2 | fields user)`, []string{
		`{"_msg":"a1","vl_account_id":"1","vl_project_id":"11"}`,
	})

	fError := func(query string, opts apptest.QueryOpts, errExpected string) {
		t.Helper()

		res, statusCode := sut.LogsQLQueryRaw(t, query, opts)
		if statusCode != http.StatusBadRequest {
			t.Fatalf("unexpected status code for query %q; got %d; want %d; response: %s", query, statusCode, http.StatusBadRequest, res)
		}
		if !strings.Contains(res, errExpected) {
			t.Fatalf("unexpected response for query %q; got %q; want it to contain %q", query, res, errExpected)
		}
	}

	// missing tenant filter in the query and in the subquery
	fError(`*`, opts, "missing vl_account_id or vl_project_id filter")
	fError(`vl_account_id:1 or user:alice`, opts, "missing vl_account_id or vl_project_id filter")
	fError(`vl_account_id:1 user:in(* | fields user)`, opts, "missing vl_account_id or vl_project_id filter")

	// AccountID and ProjectID headers aren't allowed
	fError(`vl_account_id:1`, apptest.QueryOpts{
		MultiTenant: true,
		AccountID:   "1",
		ProjectID:   "11",
	}, "AccountID and ProjectID request headers must not be set")
}
