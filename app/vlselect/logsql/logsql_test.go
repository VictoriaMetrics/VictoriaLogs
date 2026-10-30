package logsql

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
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

func TestTailProcessorWriteBlock_Success(t *testing.T) {
	// Test that tailProcessor.writeBlock() accumulates rows up to the limit
	f := func(rowsPerBlock []int) {
		t.Helper()

		tp := newTailProcessor(func() {}, false)
		rowsExpected := 0
		for _, n := range rowsPerBlock {
			tp.writeBlock(0, newTestDataBlock(n))
			rowsExpected += n
		}

		rows, err := tp.getTailRows()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if len(rows) != rowsExpected {
			t.Fatalf("unexpected number of rows; got %d; want %d", len(rows), rowsExpected)
		}
	}

	f([]int{1})
	f([]int{maxRowsPerTailQuery})
	f([]int{maxRowsPerTailQuery / 2, maxRowsPerTailQuery / 2})

	// Test that the limit is applied per query
	tp := newTailProcessor(func() {}, false)
	for range 2 {
		tp.writeBlock(0, newTestDataBlock(maxRowsPerTailQuery))
		if _, err := tp.getTailRows(); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
}

func TestTailProcessorWriteBlock_Failure(t *testing.T) {
	// Test that tailProcessor.writeBlock() cancels the query when rows exceed the limit
	f := func(rowsPerBlock []int) {
		t.Helper()

		canceled := false
		tp := newTailProcessor(func() { canceled = true }, false)
		for _, n := range rowsPerBlock {
			tp.writeBlock(0, newTestDataBlock(n))
		}

		if _, err := tp.getTailRows(); err == nil {
			t.Fatalf("expecting non-nil error")
		}
		if !canceled {
			t.Fatalf("expecting canceled query")
		}
	}

	// A single block exceeds the limit
	f([]int{maxRowsPerTailQuery + 1})

	// Multiple blocks exceed the limit
	f([]int{maxRowsPerTailQuery, 1})
}

func newTestDataBlock(rowsCount int) *logstorage.DataBlock {
	timestamps := make([]string, rowsCount)
	for i := range timestamps {
		timestamps[i] = "2025-01-01T00:00:00Z"
	}

	var db logstorage.DataBlock
	db.SetColumns([]logstorage.BlockColumn{
		{
			Name:   "_time",
			Values: timestamps,
		},
	})
	return &db
}
