package logsql

import (
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestValidateRangeQuery(t *testing.T) {
	f := func(qStr string, step int64, expectErr bool) {
		t.Helper()

		q, err := logstorage.ParseQueryAtTimestamp(qStr, 1_000_000_000_000)
		if err != nil {
			t.Fatalf("cannot parse query: %s", err)
		}

		err = validateRangeQuery(q, step)
		if !expectErr {
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			return
		}
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
	}

	// Invalid step.
	f("*", 0, true)

	// Missing bounded time range.
	f("*", 1, true)

	// Valid ranges within the limit.
	f("_time:1ns", 1, false)
	f("_time:30ms", 1_000, false)
	f("_time:5m", int64(time.Second), false)

	// Too many points.
	f("_time:30001ns", 1, true)
	f("_time:24h", 1, true)
	f("_time:300001ns", 10, true)
}

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
