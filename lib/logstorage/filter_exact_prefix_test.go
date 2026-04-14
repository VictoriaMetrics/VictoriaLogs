package logstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestFilterExactPrefix(t *testing.T) {
	t.Parallel()

	t.Run("single-row", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"abc def",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "abc def")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0})

		fep = newFilterExactPrefix("foo", "abc d")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0})

		fep = newFilterExactPrefix("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0})

		// mismatch
		fep = newFilterExactPrefix("foo", "xabc")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("non-existing column", "abc")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
	})

	t.Run("const-column", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"abc def",
					"abc def",
					"abc def",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "abc def")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2})

		fep = newFilterExactPrefix("foo", "ab")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2})

		fep = newFilterExactPrefix("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2})

		// mismatch
		fep = newFilterExactPrefix("foo", "foobar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("non-existing column", "x")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
	})

	t.Run("dict", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"",
					"foobar",
					"abc",
					"afdf foobar baz",
					"fddf foobarbaz",
					"foobarbaz",
					"foobar",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "foobar")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{1, 5, 6})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		// mismatch
		fep = newFilterExactPrefix("foo", "baz")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("non-existing column", "foobar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
	})

	t.Run("strings", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"a foo",
					"a foobar",
					"aa abc a",
					"ca afdf a,foobar baz",
					"aa fddf foobarbaz",
					"a afoobarbaz",
					"a foobar baz",
					"a kjlkjf dfff",
					"a ТЕСТЙЦУК НГКШ ",
					"a !!,23.(!1)",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "aa ")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{2, 4})

		fep = newFilterExactPrefix("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		// mismatch
		fep = newFilterExactPrefix("foo", "aa b")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "fobar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("non-existing-column", "aa")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
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
		fep := newFilterExactPrefix("foo", "12")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 5})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fep = newFilterExactPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "999")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "7")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
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
					"12",
					"1",
					"2",
					"3",
					"467",
					"5",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "12")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 5})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fep = newFilterExactPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "999")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "7")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
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
					"12",
					"1",
					"2",
					"3",
					"65536",
					"5",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "12")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 5})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fep = newFilterExactPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "99999")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "7")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
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
					"12",
					"1",
					"2",
					"3",
					"123456789012",
					"5",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "12")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 5, 9})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fep = newFilterExactPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "1234567890123")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "7")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
	})

	t.Run("int64", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"123",
					"12",
					"32",
					"0",
					"0",
					"-12",
					"1",
					"-2",
					"3",
					"123456789012",
					"5",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "12")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 9})

		fep = newFilterExactPrefix("foo", "-12")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{5})

		fep = newFilterExactPrefix("foo", "-")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{5, 7})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fep = newFilterExactPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "1234567890123")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "7")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
	})

	t.Run("float64", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"1234",
					"0",
					"3454",
					"-65536",
					"1234.5678901",
					"1",
					"2",
					"3",
					"4",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "123")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 4})

		fep = newFilterExactPrefix("foo", "1234.567")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{4})

		fep = newFilterExactPrefix("foo", "-65536")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{3})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fep = newFilterExactPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "6511")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
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
					"127.0.0.2",
					"127.0.0.1",
					"127.0.4.2",
					"127.0.0.1",
					"12.0.127.6",
					"55.55.55.55",
					"66.66.66.66",
					"7.7.7.7",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("foo", "127.0.")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{2, 4, 5, 6, 7})

		fep = newFilterExactPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fep, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		// mismatch
		fep = newFilterExactPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)

		fep = newFilterExactPrefix("foo", "255")
		testFilterMatchForColumns(t, columns, fep, "foo", nil)
	})

	t.Run("timestamp-iso8601", func(t *testing.T) {
		columns := []column{
			{
				name: "_msg",
				values: []string{
					"2006-01-02T15:04:05.001Z",
					"2006-01-02T15:04:05.002Z",
					"2006-01-02T15:04:05.003Z",
					"2006-01-02T15:04:06.004Z",
					"2006-01-02T15:04:06.005Z",
					"2006-01-02T15:04:07.006Z",
					"2006-01-02T15:04:10.007Z",
					"2006-01-02T15:04:12.008Z",
					"2006-01-02T15:04:15.009Z",
				},
			},
		}

		// match
		fep := newFilterExactPrefix("_msg", "2006-01-02T15:04:05")
		testFilterMatchForColumns(t, columns, fep, "_msg", []int{0, 1, 2})

		fep = newFilterExactPrefix("_msg", "")
		testFilterMatchForColumns(t, columns, fep, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fep = newFilterExactPrefix("_msg", "bar")
		testFilterMatchForColumns(t, columns, fep, "_msg", nil)

		fep = newFilterExactPrefix("_msg", "0")
		testFilterMatchForColumns(t, columns, fep, "_msg", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
