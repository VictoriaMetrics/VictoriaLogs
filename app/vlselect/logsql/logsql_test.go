package logsql

import (
	"net/http"
	"testing"

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

func TestAPITimeFilterPrecision(t *testing.T) {
	f := func(endTimeStr string, expectedMaxTime int64) {
		t.Helper()

		q, err := logstorage.ParseQuery("*")
		if err != nil {
			t.Fatalf("unexpected error parsing base query: %s", err)
		}

		endTime, endStr, ok, err := getTimeNsec(&http.Request{
			Form: map[string][]string{
				"end": {endTimeStr},
			},
		}, "end")
		if err != nil {
			t.Fatalf("unexpected error parsing end time %s: %s", endTimeStr, err)
		}
		if !ok {
			t.Fatalf("end time was not parsed from %s", endTimeStr)
		}

		q.AddTimeFilterWithEndStr(0, endTime, endStr)

		_, maxTime := q.GetFilterTimeRange()
		if maxTime != expectedMaxTime {
			t.Fatalf("unexpected maxTime for endStr=%s; got %d; want %d", endTimeStr, maxTime, expectedMaxTime)
		}
	}

	// Test Unix timestamp precision based on digit count (the core issue)
	f("1755104700", 1755104700999999999)          // 10 digits = seconds, expand to end of second
	f("1755104700000", 1755104700000999999)       // 13 digits = milliseconds, expand to end of millisecond
	f("1755104700123", 1755104700123999999)       // 13 digits = milliseconds (non-zero)
	f("1755104700000000", 1755104700000000999)    // 16 digits = microseconds
	f("1755104700000000000", 1755104700000000000) // 19 digits = nanoseconds

	// Test RFC3339 timestamp precision
	f("2025-08-13T17:05:00Z", 1755104700999999999)           // Second precision
	f("2025-08-13T17:05:00.000Z", 1755104700000999999)       // Millisecond precision with trailing zeros
	f("2025-08-13T17:05:00.500Z", 1755104700500999999)       // Millisecond precision
	f("2025-08-13T17:05:00.123456Z", 1755104700123456999)    // Microsecond precision
	f("2025-08-13T17:05:00.123456789Z", 1755104700123456789) // Nanosecond precision
}
