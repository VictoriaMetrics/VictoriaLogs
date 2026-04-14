package logstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestFilterIn(t *testing.T) {
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
		fi := newFilterInValues("foo", []string{"abc def", "abc", "foobar"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0})

		fi = newFilterInValues("other column", []string{"asdfdsf", ""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0})

		fi = newFilterInValues("non-existing-column", []string{"", "foo"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0})

		// mismatch
		fi = newFilterInValues("foo", []string{"abc", "def"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"", "abc"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("other column", []string{"sd"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("non-existing column", []string{"abc", "def"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("foo", []string{"aaaa", "abc def", "foobar"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2})

		fi = newFilterInValues("non-existing-column", []string{"", "abc"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2})

		// mismatch
		fi = newFilterInValues("foo", []string{"abc def ", "foobar"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("non-existing column", []string{"x"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("foo", []string{"foobar", "aaaa", "abc", "baz"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{1, 2, 6})

		fi = newFilterInValues("foo", []string{"bbbb", "", "aaaa"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		// mismatch
		fi = newFilterInValues("foo", []string{"bar", "aaaa"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("non-existing column", []string{"foobar", "aaaa"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("foo", []string{"a foobar", "aa abc a"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{1, 2, 6})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		// mismatch
		fi = newFilterInValues("foo", []string{"aa a"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("foo", []string{"12", "32"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{1, 2, 5})

		fi = newFilterInValues("foo", []string{"0"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{3, 4})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fi = newFilterInValues("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"33"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"1234"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("foo", []string{"12", "32"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{1, 2, 5})

		fi = newFilterInValues("foo", []string{"0"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{3, 4})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fi = newFilterInValues("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"33"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"123456"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("foo", []string{"12", "32"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{1, 2, 5})

		fi = newFilterInValues("foo", []string{"0"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{3, 4})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fi = newFilterInValues("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"33"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"12345678901"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("foo", []string{"12", "32"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{1, 2, 5})

		fi = newFilterInValues("foo", []string{"0"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{3, 4})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fi = newFilterInValues("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"33"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("foo", []string{"12", "-32"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{1, 2, 5})

		fi = newFilterInValues("foo", []string{"0"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{3, 4})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fi = newFilterInValues("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"33"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("foo", []string{"1234", "1", "foobar", "123211"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 5})

		fi = newFilterInValues("foo", []string{"1234.5678901"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{4})

		fi = newFilterInValues("foo", []string{"-65536"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{3})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fi = newFilterInValues("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"65536"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"123"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"12345678901234567890"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
					"55.55.55.55",
					"66.66.66.66",
					"7.7.7.7",
				},
			},
		}

		// match
		fi := newFilterInValues("foo", []string{"127.0.0.1", "24.54.1.2", "127.0.4.2"})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{2, 4, 5, 6, 7})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		// mismatch
		fi = newFilterInValues("foo", []string{"bar"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{""})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"5"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)

		fi = newFilterInValues("foo", []string{"255.255.255.255"})
		testFilterMatchForColumns(t, columns, fi, "foo", nil)
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
		fi := newFilterInValues("_msg", []string{"2006-01-02T15:04:05.005Z", "foobar"})
		testFilterMatchForColumns(t, columns, fi, "_msg", []int{4})

		fi = newFilterInValues("non-existing-column", []string{""})
		testFilterMatchForColumns(t, columns, fi, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fi = newFilterInValues("_msg", []string{"bar"})
		testFilterMatchForColumns(t, columns, fi, "_msg", nil)

		fi = newFilterInValues("_msg", []string{})
		testFilterMatchForColumns(t, columns, fi, "_msg", nil)

		fi = newFilterInValues("_msg", []string{""})
		testFilterMatchForColumns(t, columns, fi, "_msg", nil)

		fi = newFilterInValues("_msg", []string{"2006-04-02T15:04:05.005Z"})
		testFilterMatchForColumns(t, columns, fi, "_msg", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
