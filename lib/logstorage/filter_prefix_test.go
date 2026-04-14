package logstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestMatchPrefix(t *testing.T) {
	t.Parallel()

	f := func(s, prefix string, resultExpected bool) {
		t.Helper()
		result := matchPrefix(s, prefix)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
	}

	f("", "", false)
	f("foo", "", true)
	f("", "foo", false)
	f("foo", "foo", true)
	f("foo bar", "foo", true)
	f("foo bar", "bar", true)
	f("a foo bar", "foo", true)
	f("a foo bar", "fo", true)
	f("a foo bar", "oo", false)
	f("foobar", "foo", true)
	f("foobar", "bar", false)
	f("foobar", "oob", false)
	f("afoobar foo", "foo", true)
	f("раз два (три!)", "три", true)
	f("", "foo bar", false)
	f("foo bar", "foo bar", true)
	f("(foo bar)", "foo bar", true)
	f("afoo bar", "foo bar", false)
	f("afoo bar", "afoo ba", true)
	f("foo bar! baz", "foo bar!", true)
	f("a.foo bar! baz", ".foo bar! ", true)
	f("foo bar! baz", "foo bar! b", true)
	f("255.255.255.255", "5", false)
	f("255.255.255.255", "55", false)
	f("255.255.255.255", "255", true)
	f("255.255.255.255", "5.255", false)
	f("255.255.255.255", "255.25", true)
	f("255.255.255.255", "255.255", true)
}

func TestFilterPrefix(t *testing.T) {
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
		fp := newFilterPrefix("foo", "abc")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterPrefix("foo", "ab")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterPrefix("foo", "abc def")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterPrefix("foo", "def")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterPrefix("other column", "asdfdsf")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		// mismatch
		fp = newFilterPrefix("foo", "bc")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("other column", "sd")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing column", "abc")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing column", "")
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
		fp := newFilterPrefix("foo", "abc")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterPrefix("foo", "ab")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterPrefix("foo", "abc de")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterPrefix("foo", " de")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterPrefix("foo", "abc def")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterPrefix("other-column", "x")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = newFilterPrefix("_msg", " 2 ")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		// mismatch
		fp = newFilterPrefix("foo", "abc def ")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "x")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("other-column", "foo")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing column", "x")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("_msg", "foo")
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
		fp := newFilterPrefix("foo", "foobar")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 3, 4, 6})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 2, 3, 4, 5, 6})

		fp = newFilterPrefix("foo", "ba")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		// mismatch
		fp = newFilterPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing column", "foobar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing column", "")
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
		fp := newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = newFilterPrefix("foo", "a")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = newFilterPrefix("foo", "НГК")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{8})

		fp = newFilterPrefix("foo", "aa a")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2})

		fp = newFilterPrefix("foo", "!,")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{9})

		// mismatch
		fp = newFilterPrefix("foo", "aa ax")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "qwe rty abc")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "@")
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
		fp := newFilterPrefix("foo", "12")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 5})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fp = newFilterPrefix("foo", "0")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3, 4})

		// mismatch
		fp = newFilterPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "1234")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing-column", "")
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
		fp := newFilterPrefix("foo", "123")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = newFilterPrefix("foo", "0")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1})

		// mismatch
		fp = newFilterPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "123456")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing-column", "")
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
		fp := newFilterPrefix("foo", "123")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = newFilterPrefix("foo", "65536")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		// mismatch
		fp = newFilterPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "12345678901")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing-column", "")
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
		fp := newFilterPrefix("foo", "1234")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = newFilterPrefix("foo", "12345678901")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{4})

		// mismatch
		fp = newFilterPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "12345678901234567890")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing-column", "")
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
		fp := newFilterPrefix("foo", "123")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = newFilterPrefix("foo", "1234.5678901")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{4})

		fp = newFilterPrefix("foo", "56789")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{4})

		fp = newFilterPrefix("foo", "-6553")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		fp = newFilterPrefix("foo", "65536")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		// mismatch
		fp = newFilterPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "7344.8943")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "-1234")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "+1234")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "23")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "678")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "33")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "12345678901234567890")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing-column", "")
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
		fp := newFilterPrefix("foo", "127.0.0.1")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 7})

		fp = newFilterPrefix("foo", "")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		fp = newFilterPrefix("foo", "12")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 6, 7, 8, 9})

		fp = newFilterPrefix("foo", "127.0.0")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 7})

		fp = newFilterPrefix("foo", "2.3.")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = newFilterPrefix("foo", "0")
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 2, 4, 5, 6, 7, 8})

		// mismatch
		fp = newFilterPrefix("foo", "bar")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "8")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "127.1")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "27.0")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("foo", "255.255.255.255")
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = newFilterPrefix("non-existing-column", "")
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
		fp := newFilterPrefix("_msg", "2006-01-02T15:04:05.005Z")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{4})

		fp = newFilterPrefix("_msg", "")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = newFilterPrefix("_msg", "2006-01-0")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = newFilterPrefix("_msg", "002")
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{1})

		// mismatch
		fp = newFilterPrefix("_msg", "bar")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		fp = newFilterPrefix("_msg", "2006-03-02T15:04:05.005Z")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		fp = newFilterPrefix("_msg", "06")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		// This filter shouldn't match row=4, since it has different string representation of the timestamp
		fp = newFilterPrefix("_msg", "2006-01-02T16:04:05.005+01:00")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		// This filter shouldn't match row=4, since it contains too many digits for millisecond part
		fp = newFilterPrefix("_msg", "2006-01-02T15:04:05.00500Z")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		fp = newFilterPrefix("non-existing-column", "")
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
