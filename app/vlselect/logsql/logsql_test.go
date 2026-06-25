package logsql

import (
	"slices"
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

func TestTailProcessorGetTailRows(t *testing.T) {
	tp := newTailProcessor(func() {}, false)

	const streamA = "test-stream-a"
	const streamB = "test-stream-b"
	const ts = int64(1e9)

	row := func(timestamp int64, msg string) logRow {
		return logRow{
			timestamp: timestamp,
			fields:    []logstorage.Field{{Name: "_msg", Value: msg}},
		}
	}

	f := func(streamID string, input []logRow, wantMsgs ...string) {
		t.Helper()
		tp.perStreamRows[streamID] = input
		got, err := tp.getTailRows()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		var gotMsgs []string
		for _, r := range got {
			for _, field := range r {
				if field.Name == "_msg" {
					gotMsgs = append(gotMsgs, field.Value)
				}
			}
		}
		if !slices.Equal(gotMsgs, wantMsgs) {
			t.Fatalf("got msgs %v; want %v", gotMsgs, wantMsgs)
		}
	}

	// First time: row A is emitted.
	f(streamA, []logRow{row(ts, "A")}, "A")

	// Same timestamp, new content: A is deduped, B is emitted.
	f(streamA, []logRow{row(ts, "A"), row(ts, "B")}, "B")

	// Both seen now: nothing emitted.
	f(streamA, []logRow{row(ts, "A"), row(ts, "B")})

	// Empty input: nothing emitted.
	f(streamA, nil)

	// Multiple new timestamps, unsorted input: emitted in chronological order.
	f(streamA, []logRow{row(ts+2, "D"), row(ts+1, "C")}, "C", "D")

	// Boundary is now ts+2 with D seen: A is dropped (older), D is dropped (duplicate).
	f(streamA, []logRow{row(ts, "A"), row(ts+2, "D")})

	// Per-stream state: streamB emits A even though streamA already deduped it.
	f(streamB, []logRow{row(ts, "A")}, "A")

	// streamA is unaffected by streamB and still drops its boundary row D.
	f(streamA, []logRow{row(ts+2, "D")})
}
