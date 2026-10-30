package logstorage

import (
	"testing"
)

func TestTopkLess(t *testing.T) {
	parseTimestamp := func(s string) int64 {
		t.Helper()

		timestamp, ok := TryParseTimestampRFC3339Nano(s)
		if !ok {
			t.Fatalf("cannot parse timestamp %q", s)
		}
		return timestamp
	}

	ps := &pipeSort{
		byFields: []*bySortField{
			{name: "_time"},
		},
	}
	psDesc := &pipeSort{
		byFields: []*bySortField{
			{name: "_time"},
		},
		isDesc: true,
	}

	stringRow := func(s string) *pipeTopkRow {
		return &pipeTopkRow{
			byColumns:       []string{s},
			byColumnsIsTime: []bool{false},
		}
	}
	timeRow := func(s string) *pipeTopkRow {
		return &pipeTopkRow{
			byColumns:       []string{""},
			byColumnsIsTime: []bool{true},
			timestamp:       parseTimestamp(s),
		}
	}
	f := func(ps *pipeSort, a, b *pipeTopkRow, resultExpected bool) {
		t.Helper()

		result := topkLess(ps, a, b)
		if result != resultExpected {
			t.Fatalf("unexpected result for topkLess(%#v, %#v); got %v; want %v", a, b, result, resultExpected)
		}
	}

	// string time vs string time
	f(ps, stringRow("2026-04-25T10:00:54Z"), stringRow("2026-04-25T10:01:54Z"), true)
	f(ps, stringRow("2026-04-25T10:01:54Z"), stringRow("2026-04-25T10:00:54Z"), false)

	// real time vs real time
	f(ps, timeRow("2026-04-25T10:00:54Z"), timeRow("2026-04-25T10:01:54Z"), true)
	f(ps, timeRow("2026-04-25T10:01:54Z"), timeRow("2026-04-25T10:00:54Z"), false)

	// string time is smaller than real time
	f(ps, stringRow("2026-04-25T10:00:54Z"), timeRow("2026-04-25T10:01:54Z"), true)
	f(ps, timeRow("2026-04-25T10:01:54Z"), stringRow("2026-04-25T10:00:54Z"), false)

	// real time is smaller than string time
	f(ps, timeRow("2026-04-25T10:00:54Z"), stringRow("2026-04-25T10:01:54Z"), true)
	f(ps, stringRow("2026-04-25T10:01:54Z"), timeRow("2026-04-25T10:00:54Z"), false)

	// string time vs real time with descending sort
	f(psDesc, stringRow("2026-04-25T10:00:54Z"), timeRow("2026-04-25T10:01:54Z"), false)
	f(psDesc, timeRow("2026-04-25T10:01:54Z"), stringRow("2026-04-25T10:00:54Z"), true)
}

func TestLessString(t *testing.T) {
	f := func(a, b string, resultExpected bool) {
		t.Helper()

		result := lessString(a, b)
		if result != resultExpected {
			t.Fatalf("unexpected result for lessString(%q, %q); got %v; want %v", a, b, result, resultExpected)
		}
	}

	f("", "", false)
	f("a", "", false)
	f("", "a", true)
	f("foo", "bar", false)
	f("bar", "foo", true)
	f("foo", "foo", false)
	f("foo1", "foo", false)
	f("foo", "foo1", true)

	// integers
	f("123", "9", false)
	f("9", "123", true)
	f("-123", "9", true)
	f("9", "-123", false)

	// floating point numbers
	f("1e3", "5", false)
	f("5", "1e3", true)

	// timestamps
	f("2025-01-15T10:20:30.1", "2025-01-15T10:20:30.09", false)
	f("2025-01-15T10:20:30.09", "2025-01-15T10:20:30.1", true)

	// versions
	f("v1.23.4", "v1.23.10", true)
	f("v1.23.10", "v1.23.4", false)

	// durations
	f("1h", "5s", false)
	f("5s", "1h", true)

	// bytes
	f("1MB", "5KB", false)
	f("5KB", "1MB", true)

	f("1.5M", "5.1K", false)
	f("5.1K", "1.5M", true)
	f("1.5M", "1.5M", false)
}
