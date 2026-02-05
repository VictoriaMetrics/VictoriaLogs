package logstorage

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestMatchIPv6Range(t *testing.T) {
	t.Parallel()

	f := func(s, minValue, maxValue string, resultExpected bool) {
		t.Helper()
		minIP := mustParseIPv6(minValue)
		maxIP := mustParseIPv6(maxValue)
		result := matchIPv6Range(s, minIP, maxIP)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
	}

	// Invalid IP
	f("", "::1", "::2", false)
	f("123", "::1", "::2", false)
	f("1.2.3.4", "::1", "::2", false)

	// range mismatch
	f("::1", "::2", "::3", false)
	f("2001:db8::1", "2001:db8::2", "2001:db8::3", false)

	// range match
	f("::1", "::1", "::1", true)
	f("::1", "::0", "::2", true)
	f("2001:db8::1", "2001:db8::", "2001:db8::ffff", true)
}

func TestFilterIPv6Range(t *testing.T) {
	t.Parallel()

	t.Run("const-column", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"::1",
					"::1",
					"::1",
				},
			},
		}

		// match
		fr := &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::0"),
			maxValue:  mustParseIPv6("::2"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 2})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::1"),
			maxValue:  mustParseIPv6("::1"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 2})

		// mismatch
		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::2"),
			maxValue:  mustParseIPv6("::3"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = &filterIPv6Range{
			fieldName: "non-existing-column",
			minValue:  mustParseIPv6("::0"),
			maxValue:  mustParseIPv6("::ffff"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::2"),
			maxValue:  mustParseIPv6("::0"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	t.Run("dict", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"",
					"::1",
					"Abc",
					"2001:db8::1",
					"10.4",
					"foo ::1",
					"::1 bar",
					"0.0.0.2",
				},
			},
		}

		// match
		fr := &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::0"),
			maxValue:  mustParseIPv6("::2"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{1})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::0"),
			maxValue:  mustParseIPv6("fff::2"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{1, 7})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("0.0.0.0"),
			maxValue:  mustParseIPv6("0.0.0.2"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{7})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("2001:db8::"),
			maxValue:  mustParseIPv6("2001:db8::ffff"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3})

		// mismatch
		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::3"),
			maxValue:  mustParseIPv6("::4"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	t.Run("strings", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"A FOO",
					"a 10",
					"::1",
					"20",
					"15.5",
					"-5",
					"a fooBaR",
					"a ::1 dfff",
					"a ТЕСТЙЦУК НГКШ ",
					"a !!,23.(!1)",
					"2001:db8::1",
				},
			},
		}

		// match
		fr := &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::0"),
			maxValue:  mustParseIPv6("::2"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{2})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("2001:db8::"),
			maxValue:  mustParseIPv6("2001:db8::ffff"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{10})

		// mismatch
		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::3"),
			maxValue:  mustParseIPv6("::4"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	t.Run("ipv4", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"1.2.3.4",
					"0.0.0.0",
					"127.0.0.1",
					"254.255.255.255",
					"127.0.0.1",
					"127.0.0.1",
					"127.0.4.2",
					"127.0.0.1",
					"12.0.127.6",
					"55.55.12.55",
					"66.66.66.66",
					"7.7.7.7",
				},
			},
		}

		// match
		fr := &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("0.0.0.0"),
			maxValue:  mustParseIPv6("8.0.0.0"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 11})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::ffff:0:0"),
			maxValue:  mustParseIPv6("::ffff:800:0"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 11})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("::"),
			maxValue:  mustParseIPv6("ffff::"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		// mismatch
		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("128.0.0.0"),
			maxValue:  mustParseIPv6("144.0.0.0"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("255.0.0.0"),
			maxValue:  mustParseIPv6("255.255.255.255"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("8.0.0.0"),
			maxValue:  mustParseIPv6("0.0.0.0"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  mustParseIPv6("2001:db8::"),
			maxValue:  mustParseIPv6("2001:db8::ffff"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}

func mustParseIPv6(s string) [16]byte {
	a, ok := tryParseIPv6(s)
	if !ok {
		panic(fmt.Errorf("cannot parse ipv6 address %q", s))
	}
	return a
}
