package logsql

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
)

func TestParseExtraFilters_Success(t *testing.T) {
	f := func(s, resultExpected string) {
		t.Helper()

		f, err := parseExtraFilters(s)
		if err != nil {
			t.Fatalf("unexpected error in parseExtraFilters: %s", err)
		}
		result := f.String()
		if result != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}

	f("", "")

	// JSON string
	f(`{"foo":"bar"}`, `foo:=bar`)
	f(`{"foo":["bar","baz"]}`, `foo:in(bar,baz)`)
	f(`{"z":"=b ","c":["d","e,"],"a":[],"_msg":"x"}`, `z:="=b " c:in(d,"e,") =x`)

	// LogsQL filter
	f(`foobar`, `foobar`)
	f(`foo:bar`, `foo:bar`)
	f(`foo:(bar or baz) error _time:5m {"foo"=bar,baz="z"}`, `{foo="bar",baz="z"} (foo:bar or foo:baz) error _time:5m`)
}

func TestParseExtraFilters_Failure(t *testing.T) {
	f := func(s string) {
		t.Helper()

		_, err := parseExtraFilters(s)
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
	}

	// Invalid JSON
	f(`{"foo"}`)
	f(`[1,2]`)
	f(`{"foo":[1]}`)

	// Invalid LogsQL filter
	f(`foo:(bar`)

	// excess pipe
	f(`foo | count()`)
}

func TestParseExtraStreamFilters_Success(t *testing.T) {
	f := func(s, resultExpected string) {
		t.Helper()

		f, err := parseExtraStreamFilters(s)
		if err != nil {
			t.Fatalf("unexpected error in parseExtraStreamFilters: %s", err)
		}
		result := f.String()
		if result != resultExpected {
			t.Fatalf("unexpected result;\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}

	f("", "")

	// JSON string
	f(`{"foo":"bar"}`, `{foo="bar"}`)
	f(`{"foo":["bar","baz"]}`, `{foo=~"bar|baz"}`)
	f(`{"z":"b","c":["d","e|\""],"a":[],"_msg":"x"}`, `{z="b",c=~"d|e\\|\"",_msg="x"}`)

	// LogsQL filter
	f(`foobar`, `foobar`)
	f(`foo:bar`, `foo:bar`)
	f(`foo:(bar or baz) error _time:5m {"foo"=bar,baz="z"}`, `{foo="bar",baz="z"} (foo:bar or foo:baz) error _time:5m`)
}

func TestParseExtraStreamFilters_Failure(t *testing.T) {
	f := func(s string) {
		t.Helper()

		_, err := parseExtraStreamFilters(s)
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
	}

	// Invalid JSON
	f(`{"foo"}`)
	f(`[1,2]`)
	f(`{"foo":[1]}`)

	// Invalid LogsQL filter
	f(`foo:(bar`)

	// excess pipe
	f(`foo | count()`)
}

func TestGetStringSliceFromRequest(t *testing.T) {
	f := func(query, body string, resultExpected []string) {
		t.Helper()

		r := httptest.NewRequest(http.MethodPost, "/select/logsql/query?"+query, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if err := r.ParseForm(); err != nil {
			t.Fatalf("cannot parse form: %s", err)
		}

		result, err := getStringSliceFromRequest(r, "hidden_fields_filters")
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if !reflect.DeepEqual(result, resultExpected) {
			t.Fatalf("unexpected result\ngot\n%q\nwant\n%q", result, resultExpected)
		}
	}

	f("", "", nil)
	f("hidden_fields_filters=foo,bar*", "", []string{"foo", "bar*"})
	f("", `hidden_fields_filters=["foo","bar*"]`, []string{"foo", "bar*"})

	// The args from the body cannot override the args from the query string.
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1848
	f("hidden_fields_filters=secret", "hidden_fields_filters=", []string{"secret"})
	f("hidden_fields_filters=secret", "hidden%5Ffields%5Ffilters=", []string{"secret"})
	f("hidden_fields_filters=secret", "hidden_fields_filters=foo", []string{"foo", "secret"})
}

func TestIssue1896(t *testing.T) {
	// Issue #1896: support concurrency and parallel_readers limits via query args and headers
	f := func(queryURL string, headers map[string]string, expectedConcurrency, expectedParallelReaders int) {
		t.Helper()

		r := httptest.NewRequest(http.MethodGet, "/select/logsql/query?"+queryURL, nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("cannot parse form: %s", err)
		}
		ca, err := parseCommonArgs(r)
		if err != nil {
			t.Fatalf("unexpected error from parseCommonArgs: %s", err)
		}
		if n := ca.q.GetConcurrency(); n != expectedConcurrency {
			t.Fatalf("unexpected concurrency; got %d; want %d", n, expectedConcurrency)
		}
		if n := ca.q.GetParallelReaders(0); n != expectedParallelReaders {
			t.Fatalf("unexpected parallel_readers; got %d; want %d", n, expectedParallelReaders)
		}
	}

	maxCPUs := cgroup.AvailableCPUs()
	expectedConcurrency := func(want int) int {
		if want > maxCPUs {
			return maxCPUs
		}
		return want
	}

	// 1. URL parameter concurrency and parallel_readers
	f("query=*&concurrency=2&parallel_readers=3", nil, expectedConcurrency(2), 3)

	// 2. HTTP headers VL-Concurrency and VL-Parallel-Readers
	f("query=*", map[string]string{
		"VL-Concurrency":      "2",
		"VL-Parallel-Readers": "3",
	}, expectedConcurrency(2), 3)

	// 3. HTTP headers VL-Max-Concurrency clamps query options
	f("query=options(concurrency=10,parallel_readers=10)+*", map[string]string{
		"VL-Max-Concurrency":      "1",
		"VL-Max-Parallel-Readers": "1",
	}, 1, 1)

	// 4. URL parameter max_concurrency clamps query options
	f("query=options(concurrency=10,parallel_readers=10)+*&max_concurrency=2&max_parallel_readers=2", nil, expectedConcurrency(2), 2)

	// 5. Test error on invalid or non-positive value
	fErr := func(queryURL string) {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, "/select/logsql/query?"+queryURL, nil)
		if err := r.ParseForm(); err != nil {
			t.Fatalf("cannot parse form: %s", err)
		}
		if _, err := parseCommonArgs(r); err == nil {
			t.Fatalf("expecting error for query %q", queryURL)
		}
	}
	fErr("query=*&concurrency=0")
	fErr("query=*&concurrency=-1")
	fErr("query=*&concurrency=abc")
	fErr("query=*&max_concurrency=0")
	fErr("query=*&parallel_readers=0")
	fErr("query=*&max_parallel_readers=0")
}


