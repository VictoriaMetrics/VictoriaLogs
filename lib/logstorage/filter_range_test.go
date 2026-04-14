package logstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestFilterRange(t *testing.T) {
	t.Parallel()

	t.Run("const-column", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"10",
					"10",
					"10",
				},
			},
		}

		// match
		fr := newFilterRange("foo", -10, 20, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 2})

		fr = newFilterRange("foo", 10, 10, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 2})

		fr = newFilterRange("foo", 10, 20, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{0, 1, 2})

		// mismatch
		fr = newFilterRange("foo", -10, 9.99, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 20, -10, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 10.1, 20, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("non-existing-column", 10, 20, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 11, 10, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	t.Run("dict", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"",
					"10",
					"Abc",
					"20",
					"10.5",
					"10 AFoobarbaz",
					"foobar",
				},
			},
		}

		// match
		fr := newFilterRange("foo", -10, 20, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{1, 3, 4})

		fr = newFilterRange("foo", 10, 20, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{1, 3, 4})

		fr = newFilterRange("foo", 10.1, 19.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{4})

		// mismatch
		fr = newFilterRange("foo", -11, 0, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 11, 19, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 20.1, 100, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 20, 10, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

	})

	t.Run("strings", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"A FOO",
					"a 10",
					"10",
					"20",
					"15.5",
					"-5",
					"a fooBaR",
					"a kjlkjf dfff",
					"a ТЕСТЙЦУК НГКШ ",
					"a !!,23.(!1)",
				},
			},
		}

		// match
		fr := newFilterRange("foo", -100, 100, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{2, 3, 4, 5})

		fr = newFilterRange("foo", 10, 20, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{2, 3, 4})

		fr = newFilterRange("foo", -5, -5, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{5})

		// mismatch
		fr = newFilterRange("foo", -10, -5.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 20.1, 100, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 20, 10, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	t.Run("uint8", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"123",
					"12",
					"32",
					"0",
					"0",
					"12",
					"1",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fr := newFilterRange("foo", 0, 3, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7, 8})

		fr = newFilterRange("foo", 0.1, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{6, 7})

		fr = newFilterRange("foo", -1e18, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7})

		// mismatch
		fr = newFilterRange("foo", -1e18, -0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 0.1, 0.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 2.9, 0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

	})

	t.Run("uint16", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"123",
					"12",
					"32",
					"0",
					"0",
					"65535",
					"1",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fr := newFilterRange("foo", 0, 3, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7, 8})

		fr = newFilterRange("foo", 0.1, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{6, 7})

		fr = newFilterRange("foo", -1e18, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7})

		// mismatch
		fr = newFilterRange("foo", -1e18, -0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 0.1, 0.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 2.9, 0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	t.Run("uint32", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"123",
					"12",
					"32",
					"0",
					"0",
					"65536",
					"1",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fr := newFilterRange("foo", 0, 3, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7, 8})

		fr = newFilterRange("foo", 0.1, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{6, 7})

		fr = newFilterRange("foo", -1e18, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7})

		// mismatch
		fr = newFilterRange("foo", -1e18, -0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 0.1, 0.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 2.9, 0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	t.Run("uint64", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"123",
					"12",
					"32",
					"0",
					"0",
					"12345678901",
					"1",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fr := newFilterRange("foo", -inf, 3, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7, 8})

		fr = newFilterRange("foo", 0.1, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{6, 7})

		fr = newFilterRange("foo", -1e18, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7})

		fr = newFilterRange("foo", 1000, inf, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{5})

		// mismatch
		fr = newFilterRange("foo", -1e18, -0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 0.1, 0.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 2.9, 0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	t.Run("int64", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"123",
					"12",
					"-32",
					"0",
					"0",
					"12345678901",
					"1",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fr := newFilterRange("foo", -inf, 3, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{2, 3, 4, 6, 7, 8})

		fr = newFilterRange("foo", -10, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7})

		fr = newFilterRange("foo", -1e18, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{2, 3, 4, 6, 7})

		fr = newFilterRange("foo", 1000, inf, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{5})

		// mismatch
		fr = newFilterRange("foo", -1, -0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 0.1, 0.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 2.9, 0.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)
	})

	t.Run("float64", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"123",
					"12",
					"32",
					"0",
					"0",
					"123456.78901",
					"-0.2",
					"2",
					"-334",
					"4",
					"5",
				},
			},
		}

		// match
		fr := newFilterRange("foo", -inf, 3, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 7, 8})

		fr = newFilterRange("foo", 0.1, 2.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{7})

		fr = newFilterRange("foo", -1e18, 1.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{3, 4, 6, 8})

		fr = newFilterRange("foo", 1000, inf, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{5})

		// mismatch
		fr = newFilterRange("foo", -1e18, -334.1, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 0.1, 0.9, "")
		testFilterMatchForColumns(t, columns, fr, "foo", nil)

		fr = newFilterRange("foo", 2.9, 0.1, "")
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

		fr := newFilterRange("foo", -100, 100, "")
		testFilterMatchForColumns(t, columns, fr, "foo", []int{1})
	})

	t.Run("timestamp-iso8601", func(t *testing.T) {
		columns := []column{
			{
				name: "_msg",
				values: []string{
					"2006-01-02T15:04:05.001Z",
					"2006-01-02T15:04:05.002Z",
					"2006-01-02T15:04:05.003Z",
					"2006-01-02T15:04:05.004Z",
					"2006-01-02T15:04:05.005Z",
					"2006-01-02T15:04:05.006Z",
					"2006-01-02T15:04:05.007Z",
					"2006-01-02T15:04:05.008Z",
					"2006-01-02T15:04:05.009Z",
				},
			},
		}

		// range filter always mismatches timestamp
		fr := newFilterRange("_msg", -100, 100, "")
		testFilterMatchForColumns(t, columns, fr, "_msg", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
