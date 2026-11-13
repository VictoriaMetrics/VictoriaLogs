package vlselect

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestParseTenantAliases(t *testing.T) {
	f := func(a []string, resultExpected []tenantAlias) {
		t.Helper()

		result, err := parseTenantAliases(a)
		if err != nil {
			t.Fatalf("unexpected error for parseTenantAliases(%q): %s", a, err)
		}
		if !reflect.DeepEqual(result, resultExpected) {
			t.Fatalf("unexpected result for parseTenantAliases(%q)\ngot\n%v\nwant\n%v", a, result, resultExpected)
		}
	}

	f(nil, []tenantAlias{})
	f([]string{"0:0=k8s"}, []tenantAlias{{AccountID: 0, ProjectID: 0, Alias: "k8s"}})

	// the result is sorted by tenant id, so it doesn't depend on the order of the flag values
	f([]string{"0:1=nginx-access", "0:0=k8s"}, []tenantAlias{
		{AccountID: 0, ProjectID: 0, Alias: "k8s"},
		{AccountID: 0, ProjectID: 1, Alias: "nginx-access"},
	})
	f([]string{"2:0=b", "1:5=a"}, []tenantAlias{
		{AccountID: 1, ProjectID: 5, Alias: "a"},
		{AccountID: 2, ProjectID: 0, Alias: "b"},
	})

	// accountID without projectID means projectID=0
	f([]string{"7=logs"}, []tenantAlias{{AccountID: 7, ProjectID: 0, Alias: "logs"}})

	// surrounding whitespace is ignored, aliases may contain spaces and `=`
	f([]string{" 0:1 = nginx access "}, []tenantAlias{{AccountID: 0, ProjectID: 1, Alias: "nginx access"}})
	f([]string{"0:2=a=b"}, []tenantAlias{{AccountID: 0, ProjectID: 2, Alias: "a=b"}})

	// the last alias wins for duplicate tenant ids
	f([]string{"0:0=first", "0:0=second"}, []tenantAlias{{AccountID: 0, ProjectID: 0, Alias: "second"}})
}

func TestParseTenantAliasesFailure(t *testing.T) {
	f := func(a []string) {
		t.Helper()

		result, err := parseTenantAliases(a)
		if err == nil {
			t.Fatalf("expecting an error for parseTenantAliases(%q); got %v", a, result)
		}
	}

	// no `=` delimiter and no alias
	f([]string{"0:0"})
	f([]string{"0:0="})
	f([]string{"0:0=   "})

	// ParseTenantID maps an empty accountID or projectID to 0 without an error, so
	// these would otherwise alias a tenant nobody named: "" and ":" -> 0:0, ":1" -> 0:1
	f([]string{"=k8s"})
	f([]string{":=k8s"})
	f([]string{":1=nginx-access"})
	f([]string{"0:=k8s"})

	// still rejected by ParseTenantID itself
	f([]string{"abc=k8s"})
	f([]string{"0:abc=k8s"})
	f([]string{"0:1:2=k8s"})
}

func TestWriteTenantAliases(t *testing.T) {
	aliases, err := parseTenantAliases([]string{"0:0=k8s", "0:1=nginx-access", "3:4=traces"})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	f := func(headers map[string]string, responseExpected string) {
		t.Helper()

		r := httptest.NewRequest(http.MethodGet, "/select/tenant_aliases", nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()

		writeTenantAliases(w, r, aliases)

		if w.Code != http.StatusOK {
			t.Fatalf("unexpected status code for headers %v; got %d; want %d", headers, w.Code, http.StatusOK)
		}
		response := w.Body.String()
		if response != responseExpected {
			t.Fatalf("unexpected response for headers %v\ngot\n%s\nwant\n%s", headers, response, responseExpected)
		}
	}

	// requests without tenant headers see all the aliases
	f(nil, `[{"account_id":0,"project_id":0,"alias":"k8s"},{"account_id":0,"project_id":1,"alias":"nginx-access"},{"account_id":3,"project_id":4,"alias":"traces"}]`)

	// requests restricted to a single tenant see the alias for this tenant only
	f(map[string]string{"AccountID": "0", "ProjectID": "1"}, `[{"account_id":0,"project_id":1,"alias":"nginx-access"}]`)
	f(map[string]string{"AccountID": "3", "ProjectID": "4"}, `[{"account_id":3,"project_id":4,"alias":"traces"}]`)

	// a missing ProjectID header means projectID=0
	f(map[string]string{"AccountID": "0"}, `[{"account_id":0,"project_id":0,"alias":"k8s"}]`)
	f(map[string]string{"ProjectID": "1"}, `[{"account_id":0,"project_id":1,"alias":"nginx-access"}]`)

	// tenants without an alias get an empty response instead of `null`
	f(map[string]string{"AccountID": "9", "ProjectID": "9"}, `[]`)
}

func TestWriteTenantAliasesEmpty(t *testing.T) {
	aliases, err := parseTenantAliases(nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	r := httptest.NewRequest(http.MethodGet, "/select/tenant_aliases", nil)
	w := httptest.NewRecorder()

	writeTenantAliases(w, r, aliases)

	response := w.Body.String()
	if response != `[]` {
		t.Fatalf("unexpected response for empty -tenantAliases\ngot\n%s\nwant\n[]", response)
	}
}
