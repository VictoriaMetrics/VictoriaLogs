package logsql

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
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

func TestIssueQueryStatsHeaders(t *testing.T) {
	ca := &commonArgs{
		tenantIDs: []logstorage.TenantID{{AccountID: 1, ProjectID: 2}},
	}
	ca.qs.RowsProcessed = 100
	ca.qs.RowsFound = 42
	ca.qs.BlocksProcessed = 5
	ca.qs.BytesReadValues = 1000
	ca.qs.BytesReadTimestamps = 500

	h := make(http.Header)
	ca.writeResponseHeaders(h, time.Now())

	if got, want := h.Get("VL-Rows-Processed"), "100"; got != want {
		t.Fatalf("unexpected VL-Rows-Processed: got %q, want %q", got, want)
	}
	if got, want := h.Get("VL-Rows-Found"), "42"; got != want {
		t.Fatalf("unexpected VL-Rows-Found: got %q, want %q", got, want)
	}
	if got, want := h.Get("VL-Blocks-Processed"), "5"; got != want {
		t.Fatalf("unexpected VL-Blocks-Processed: got %q, want %q", got, want)
	}
	if got, want := h.Get("VL-Bytes-Read"), "1500"; got != want {
		t.Fatalf("unexpected VL-Bytes-Read: got %q, want %q", got, want)
	}

	exposeHeaders := h.Get("Access-Control-Expose-Headers")
	for _, expectedHeader := range []string{"Vl-Rows-Processed", "Vl-Rows-Found", "Vl-Blocks-Processed", "Vl-Bytes-Read"} {
		if !strings.Contains(exposeHeaders, expectedHeader) {
			t.Fatalf("Access-Control-Expose-Headers %q missing %q", exposeHeaders, expectedHeader)
		}
	}
}

func TestIssue1907(t *testing.T) {
	// Issue #1907: For streaming requests with trailers, initial response headers must not include stats
	ca := &commonArgs{
		tenantIDs: []logstorage.TenantID{{AccountID: 1, ProjectID: 2}},
	}
	ca.qs.RowsProcessed = 100

	hStreaming := make(http.Header)
	hStreaming.Set("Trailer", "VL-Rows-Processed, VL-Rows-Found, VL-Blocks-Processed, VL-Bytes-Read")
	ca.writeResponseHeaders(hStreaming, time.Now())

	if got := hStreaming.Get("VL-Rows-Processed"); got != "" {
		t.Fatalf("streaming request must not set stats in initial headers; got %q", got)
	}

	ca.writeResponseStats(hStreaming)
	if got, want := hStreaming.Get("VL-Rows-Processed"), "100"; got != want {
		t.Fatalf("unexpected trailer value: got %q, want %q", got, want)
	}
}

