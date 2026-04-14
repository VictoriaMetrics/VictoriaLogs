package logstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestMatchSequence(t *testing.T) {
	t.Parallel()

	f := func(s string, phrases []string, resultExpected bool) {
		t.Helper()
		result := matchSequence(s, phrases)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
	}

	f("", []string{""}, true)
	f("foo", []string{""}, true)
	f("", []string{"foo"}, false)
	f("foo", []string{"foo"}, true)
	f("foo bar", []string{"foo"}, true)
	f("foo bar", []string{"bar"}, true)
	f("foo bar", []string{"foo bar"}, true)
	f("foo bar", []string{"foo", "bar"}, true)
	f("foo bar", []string{"foo", " bar"}, true)
	f("foo bar", []string{"foo ", "bar"}, true)
	f("foo bar", []string{"foo ", " bar"}, false)
	f("foo bar", []string{"bar", "foo"}, false)
}

func TestFilterSequence(t *testing.T) {
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
		fs := newFilterSequence("foo", []string{"abc"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0})

		fs = newFilterSequence("foo", []string{"def"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0})

		fs = newFilterSequence("foo", []string{"abc def"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0})

		fs = newFilterSequence("foo", []string{"abc ", "", "def", ""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0})

		fs = newFilterSequence("foo", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0})

		// mismatch
		fs = newFilterSequence("foo", []string{"ab"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"abc", "abc"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"abc", "def", "foo"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
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
		fs := newFilterSequence("foo", []string{"abc", " def"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2})

		fs = newFilterSequence("foo", []string{"abc ", ""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2})

		fs = newFilterSequence("non-existing-column", []string{"", ""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2})

		// mismatch
		fs = newFilterSequence("foo", []string{"abc def ", "foobar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("non-existing column", []string{"x", "yz"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
	})

	t.Run("dict", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"",
					"baz foobar",
					"abc",
					"afdf foobar baz",
					"fddf foobarbaz",
					"afoobarbaz",
					"foobar",
				},
			},
		}

		// match
		fs := newFilterSequence("foo", []string{"foobar", "baz"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{3})

		fs = newFilterSequence("foo", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		// mismatch
		fs = newFilterSequence("foo", []string{"baz", "aaaa"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("non-existing column", []string{"foobar", "aaaa"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
	})

	t.Run("strings", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"a bb foo",
					"bb a foobar",
					"aa abc a",
					"ca afdf a,foobar baz",
					"a fddf foobarbaz",
					"a afoobarbaz",
					"a foobar bb",
					"a kjlkjf dfff",
					"a ТЕСТЙЦУК НГКШ ",
					"a !!,23.(!1)",
				},
			},
		}

		// match
		fs := newFilterSequence("foo", []string{"a", "bb"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 6})

		fs = newFilterSequence("foo", []string{"НГКШ", " "})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{8})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fs = newFilterSequence("foo", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fs = newFilterSequence("foo", []string{"!,", "(!1)"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{9})

		// mismatch
		fs = newFilterSequence("foo", []string{"aa a", "bcdasqq"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"@", "!!!!"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
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
		fs := newFilterSequence("foo", []string{"12"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{1, 5})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("foo", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fs = newFilterSequence("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"", "bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234", "567"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
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
					"256",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fs := newFilterSequence("foo", []string{"12"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{1, 5})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("foo", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fs = newFilterSequence("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"", "bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234", "567"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
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
					"65536",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fs := newFilterSequence("foo", []string{"12"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{1, 5})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("foo", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fs = newFilterSequence("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"", "bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234", "567"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
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
					"12345678901",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fs := newFilterSequence("foo", []string{"12"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{1, 5})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("foo", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fs = newFilterSequence("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"", "bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234", "567"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
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
					"12",
					"12345678901",
					"2",
					"3",
					"4",
					"5",
				},
			},
		}

		// match
		fs := newFilterSequence("foo", []string{"12"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{1, 5})

		fs = newFilterSequence("foo", []string{"-32"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{2})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("foo", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fs = newFilterSequence("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"", "bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"1234", "567"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
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
		fs := newFilterSequence("foo", []string{"-", "65536"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{3})

		fs = newFilterSequence("foo", []string{"1234.", "5678901"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{4})

		fs = newFilterSequence("foo", []string{"", "5678901"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{4})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fs = newFilterSequence("foo", []string{"", ""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fs = newFilterSequence("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"65536", "-"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"5678901", "1234"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"12345678901234567890"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
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
					"1.0.127.6",
					"55.55.55.55",
					"66.66.66.66",
					"7.7.7.7",
				},
			},
		}

		// match
		fs := newFilterSequence("foo", []string{"127.0.0.1"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{2, 4, 5, 7})

		fs = newFilterSequence("foo", []string{"127", "1"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{2, 4, 5, 7})

		fs = newFilterSequence("foo", []string{"127.0.0"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{2, 4, 5, 7})

		fs = newFilterSequence("foo", []string{"2.3", ".4"})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0})

		fs = newFilterSequence("foo", []string{})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		fs = newFilterSequence("foo", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		// mismatch
		fs = newFilterSequence("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"5"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"127.", "1", "1", "345"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"27.0"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)

		fs = newFilterSequence("foo", []string{"255.255.255.255"})
		testFilterMatchForColumns(t, columns, fs, "foo", nil)
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
		fs := newFilterSequence("_msg", []string{"2006-01-02T15:04:05.005Z"})
		testFilterMatchForColumns(t, columns, fs, "_msg", []int{4})

		fs = newFilterSequence("_msg", []string{"2006-01", "04:05."})
		testFilterMatchForColumns(t, columns, fs, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fs = newFilterSequence("_msg", []string{"2006", "002Z"})
		testFilterMatchForColumns(t, columns, fs, "_msg", []int{1})

		fs = newFilterSequence("_msg", []string{})
		testFilterMatchForColumns(t, columns, fs, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fs = newFilterSequence("_msg", []string{""})
		testFilterMatchForColumns(t, columns, fs, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fs = newFilterSequence("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fs, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fs = newFilterSequence("_msg", []string{"bar"})
		testFilterMatchForColumns(t, columns, fs, "_msg", nil)

		fs = newFilterSequence("_msg", []string{"002Z", "2006"})
		testFilterMatchForColumns(t, columns, fs, "_msg", nil)

		fs = newFilterSequence("_msg", []string{"2006-04-02T15:04:05.005Z", "2023"})
		testFilterMatchForColumns(t, columns, fs, "_msg", nil)

		fs = newFilterSequence("_msg", []string{"06"})
		testFilterMatchForColumns(t, columns, fs, "_msg", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
