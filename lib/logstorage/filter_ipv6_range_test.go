package logstorage

import (
	"net"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestMatchIPv6Range(t *testing.T) {
	t.Parallel()

	f := func(s, minValue, maxValue string, resultExpected bool) {
		t.Helper()
		parseIP := func(s string) [16]byte {
			ip := net.ParseIP(s).To16()
			if ip == nil {
				t.Fatalf("cannot parse IPv6 address %q in test", s)
			}
			var a [16]byte
			copy(a[:], ip)
			return a
		}
		minIP := parseIP(minValue)
		maxIP := parseIP(maxValue)
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

	parseIP := func(s string) [16]byte {
		ip := net.ParseIP(s).To16()
		if ip == nil {
			t.Fatalf("cannot parse IPv6 address %q in test", s)
		}
		var a [16]byte
		copy(a[:], ip)
		return a
	}

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
			minValue:  parseIP("::0"),
			maxValue:  parseIP("::2"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 2})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  parseIP("::1"),
			maxValue:  parseIP("::1"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 2})

		// mismatch
		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  parseIP("::2"),
			maxValue:  parseIP("::3"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = &filterIPv6Range{
			fieldName: "non-existing-column",
			minValue:  parseIP("::0"),
			maxValue:  parseIP("::ffff"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  parseIP("::2"),
			maxValue:  parseIP("::0"),
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
					"::1",
				},
			},
		}

		// match
		fr := &filterIPv6Range{
			fieldName: "foo",
			minValue:  parseIP("::0"),
			maxValue:  parseIP("::2"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{1, 7})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  parseIP("2001:db8::"),
			maxValue:  parseIP("2001:db8::ffff"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3})

		// mismatch
		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  parseIP("::3"),
			maxValue:  parseIP("::4"),
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
			minValue:  parseIP("::0"),
			maxValue:  parseIP("::2"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{2})

		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  parseIP("2001:db8::"),
			maxValue:  parseIP("2001:db8::ffff"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", []int{10})

		// mismatch
		fr = &filterIPv6Range{
			fieldName: "foo",
			minValue:  parseIP("::3"),
			maxValue:  parseIP("::4"),
		}
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
