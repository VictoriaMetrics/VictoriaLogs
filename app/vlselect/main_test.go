package vlselect

import (
	"reflect"
	"testing"
)

func TestParseTenantAliases(t *testing.T) {
	f := func(a []string, resultExpected map[string]string) {
		t.Helper()

		result, err := parseTenantAliases(a)
		if err != nil {
			t.Fatalf("unexpected error for parseTenantAliases(%q): %s", a, err)
		}
		if !reflect.DeepEqual(result, resultExpected) {
			t.Fatalf("unexpected result for parseTenantAliases(%q)\ngot\n%v\nwant\n%v", a, result, resultExpected)
		}
	}

	f(nil, map[string]string{})
	f([]string{"0:0=k8s"}, map[string]string{"0:0": "k8s"})
	f([]string{"0:0=k8s", "0:1=nginx-access"}, map[string]string{"0:0": "k8s", "0:1": "nginx-access"})

	// accountID without projectID means projectID=0
	f([]string{"7=logs"}, map[string]string{"7:0": "logs"})

	// surrounding whitespace is ignored, aliases may contain spaces and `=`
	f([]string{" 0:1 = nginx access "}, map[string]string{"0:1": "nginx access"})
	f([]string{"0:2=a=b"}, map[string]string{"0:2": "a=b"})

	// the last alias wins for duplicate tenant ids
	f([]string{"0:0=first", "0:0=second"}, map[string]string{"0:0": "second"})
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
}
