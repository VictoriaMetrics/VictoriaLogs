package logstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestMatchSubstring(t *testing.T) {
	t.Parallel()

	f := func(s, substring string, resultExpected bool) {
		t.Helper()
		result := matchSubstring(s, substring)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
	}

	f("", "", true)
	f("foo", "", true)
	f("", "foo", false)
	f("foo", "foo", true)
	f("foo bar", "foo", true)
	f("foo bar", "bar", true)
	f("a foo bar", "foo", true)
	f("a foo bar", "fo", true)
	f("a foo bar", "oo", true)
	f("a foo bar", "goo", false)
	f("foobar", "foo", true)
	f("foobar", "bar", true)
	f("foobar", "oob", true)
	f("foobar", "boob", false)
	f("afoobar foo", "foo", true)
	f("раз два (три!)", "три", true)
	f("", "foo bar", false)
	f("foo bar", "foo bar", true)
	f("(foo bar)", "foo bar", true)
	f("afoo bar", "foo bar", true)
	f("afoo bar", "afoo ba", true)
	f("foo bar! baz", "foo bar!", true)
	f("a.foo bar! baz", ".foo bar! ", true)
	f("foo bar! baz", "foo bar! b", true)
	f("255.255.255.255", "5", true)
	f("255.255.255.255", "55", true)
	f("255.255.255.255", "355", false)
	f("255.255.255.255", "255", true)
	f("255.255.255.255", "5.255", true)
	f("255.255.255.255", "255.25", true)
	f("255.255.255.255", "255.255", true)
	f("255.255.255.255", "255.2557", false)
}

func TestFilterSubstring(t *testing.T) {
	t.Parallel()

	t.Run("single-row", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"abc def",
				},
			},
			{
				name: "other column",
				values: []string{
					"asdfdsf",
				},
			},
		}

		// match
		fp := newFilterSubstring("foo", "abc")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterSubstring("foo", "ab")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterSubstring("foo", "abc def")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterSubstring("foo", "def")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterSubstring("other column", "asdfdsf")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterSubstring("foo", "bc")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterSubstring("non-existing column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		// mismatch
		fp = newFilterSubstring("other column", "sdd")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("non-existing column", "abc")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
	})

	t.Run("const-column", func(t *testing.T) {
		columns := []column{
			{
				name: "other-column",
				values: []string{
					"x",
					"x",
					"x",
				},
			},
			{
				name: "foo",
				values: []string{
					"abc def",
					"abc def",
					"abc def",
				},
			},
			{
				name: "_msg",
				values: []string{
					"1 2 3",
					"1 2 3",
					"1 2 3",
				},
			},
		}

		// match
		fp := newFilterSubstring("foo", "abc")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterSubstring("foo", "ab")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterSubstring("foo", "abc de")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterSubstring("foo", " de")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterSubstring("foo", "abc def")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterSubstring("other-column", "x")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterSubstring("_msg", " 2 ")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterSubstring("non-existing column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		// mismatch
		fp = newFilterSubstring("foo", "abc def ")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "x")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("other-column", "foo")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("non-existing column", "x")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("_msg", "foo")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
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
					"afoobarbaz",
					"foobar",
				},
			},
		}

		// match
		fp := newFilterSubstring("foo", "foobar")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 3, 4, 5, 6})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		fp = newFilterSubstring("foo", "ba")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 3, 4, 5, 6})

		fp = newFilterSubstring("non-existing column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		// mismatch
		fp = newFilterSubstring("foo", "barz")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("non-existing column", "foobar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
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
					"a fddf foobarbaz",
					"a afoobarbaz",
					"a foobar",
					"a kjlkjf dfff",
					"a ТЕСТЙЦУК НГКШ ",
					"a !!,23.(!1)",
				},
			},
		}

		// match
		fp := newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = newFilterSubstring("foo", "a")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = newFilterSubstring("foo", "НГК")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{8})

		fp = newFilterSubstring("foo", "aa a")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2})

		fp = newFilterSubstring("foo", "!,")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{9})

		fp = newFilterSubstring("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = newFilterSubstring("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 3, 4, 5, 6})

		// mismatch
		fp = newFilterSubstring("foo", "aa ax")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "qwe rty abc")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "barasdfsz")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "@")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
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
		fp := newFilterSubstring("foo", "12")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 5})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fp = newFilterSubstring("foo", "0")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3, 4})

		fp = newFilterSubstring("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fp = newFilterSubstring("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "1234")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
	})

	t.Run("uint16", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"1234",
					"0",
					"3454",
					"65535",
					"1234",
					"1",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fp := newFilterSubstring("foo", "123")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = newFilterSubstring("foo", "0")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1})

		fp = newFilterSubstring("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		// mismatch
		fp = newFilterSubstring("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "123456")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
	})

	t.Run("uint32", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"1234",
					"0",
					"3454",
					"65536",
					"1234",
					"1",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fp := newFilterSubstring("foo", "123")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = newFilterSubstring("foo", "65536")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		fp = newFilterSubstring("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		// mismatch
		fp = newFilterSubstring("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "12345678901")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
	})

	t.Run("uint64", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"1234",
					"0",
					"3454",
					"65536",
					"12345678901",
					"1",
					"2",
					"3",
					"4",
				},
			},
		}

		// match
		fp := newFilterSubstring("foo", "1234")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = newFilterSubstring("foo", "12345678901")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{4})

		fp = newFilterSubstring("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fp = newFilterSubstring("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "12345678901234567890")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
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
		fp := newFilterSubstring("foo", "123")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = newFilterSubstring("foo", "1234.5678901")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{4})

		fp = newFilterSubstring("foo", "56789")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{4})

		fp = newFilterSubstring("foo", "-6553")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		fp = newFilterSubstring("foo", "65536")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		fp = newFilterSubstring("foo", "23")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = newFilterSubstring("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fp = newFilterSubstring("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "7344.8943")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "-1234")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "+1234")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "23423")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "678911")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "12345678901234567890")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
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
		fp := newFilterSubstring("foo", "127.0.0.1")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 7})

		fp = newFilterSubstring("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		fp = newFilterSubstring("foo", "12")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 6, 7, 8, 9})

		fp = newFilterSubstring("foo", "127.0.0")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 7})

		fp = newFilterSubstring("foo", "2.3.")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterSubstring("foo", "0")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 2, 4, 5, 6, 7, 8})

		fp = newFilterSubstring("foo", "27.0")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 6, 7})

		fp = newFilterSubstring("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		// mismatch
		fp = newFilterSubstring("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "8")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "127.1")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "27.022")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterSubstring("foo", "255.255.255.255")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)
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

		// match
		fp := newFilterSubstring("_msg", "2006-01-02T15:04:05.005Z")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{4})

		fp = newFilterSubstring("_msg", "")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = newFilterSubstring("_msg", "2006-01-0")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = newFilterSubstring("_msg", "002")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{1})

		fp = newFilterSubstring("_msg", "06")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = newFilterSubstring("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fp = newFilterSubstring("_msg", "bar")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		fp = newFilterSubstring("_msg", "2006-03-02T15:04:05.005Z")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		fp = newFilterSubstring("_msg", "8007")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		// This filter shouldn't match row=4, since it has different string representation of the timestamp
		fp = newFilterSubstring("_msg", "2006-01-02T16:04:05.005+01:00")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		// This filter shouldn't match row=4, since it contains too many digits for millisecond part
		fp = newFilterSubstring("_msg", "2006-01-02T15:04:05.00500Z")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
