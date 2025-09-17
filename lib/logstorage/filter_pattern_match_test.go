package logstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestFilterPatternMatch(t *testing.T) {
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("abc", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("ab", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("abc def", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("def", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = &filterPatternMatch{
			fieldName: "other column",
			pm:        newPatternMatcher("asdfdsf", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("bc", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = &filterPatternMatch{
			fieldName: "non-existing column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "other column",
			pm:        newPatternMatcher("sdd", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "non-existing column",
			pm:        newPatternMatcher("abc", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("abc", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("ab", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("abc de", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher(" de", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("abc def", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = &filterPatternMatch{
			fieldName: "other-column",
			pm:        newPatternMatcher("x", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher(" 2 ", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		fp = &filterPatternMatch{
			fieldName: "non-existing column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("abc def ", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("x", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "other-column",
			pm:        newPatternMatcher("foo", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "non-existing column",
			pm:        newPatternMatcher("x", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("foo", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("foobar", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 3, 4, 5, 6})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("ba", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 3, 4, 5, 6})

		fp = &filterPatternMatch{
			fieldName: "non-existing column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("barz", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "non-existing column",
			pm:        newPatternMatcher("foobar", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("a", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("НГК", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{8})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("aa a", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("!,", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{9})

		fp = &filterPatternMatch{
			fieldName: "non-existing-column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("bar", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 3, 4, 5, 6})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("aa ax", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("qwe rty abc", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("barasdfsz", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("@", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("12", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 5})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("0", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3, 4})

		fp = &filterPatternMatch{
			fieldName: "non-existing-column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("bar", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("33", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("1234", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("123", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("0", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1})

		fp = &filterPatternMatch{
			fieldName: "non-existing-column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("bar", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("33", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("123456", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("123", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("65536", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		fp = &filterPatternMatch{
			fieldName: "non-existing-column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("bar", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("33", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("12345678901", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("1234", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("12345678901", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{4})

		fp = &filterPatternMatch{
			fieldName: "non-existing-column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("bar", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("33", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("12345678901234567890", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("123", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("1234.5678901", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{4})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("56789", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{4})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("-6553", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("65536", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{3})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("23", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 4})

		fp = &filterPatternMatch{
			fieldName: "non-existing-column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("bar", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("7344.8943", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("-1234", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("+1234", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("23423", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("678911", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("33", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("12345678901234567890", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("127.0.0.1", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 7})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("12", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 6, 7, 8, 9})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("127.0.0", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 7})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("2.3.", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("0", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{1, 2, 4, 5, 6, 7, 8})

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("27.0", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{2, 4, 5, 6, 7})

		fp = &filterPatternMatch{
			fieldName: "non-existing-column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("bar", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("8", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("127.1", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("27.022", false),
		}
		testFilterMatchForColumns(t, columns, fp, "foo", nil)

		fp = &filterPatternMatch{
			fieldName: "foo",
			pm:        newPatternMatcher("255.255.255.255", false),
		}
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
		fp := &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("2006-01-02T15:04:05.005Z", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{4})

		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("2006-01-0", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("002", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{1})

		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("06", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		fp = &filterPatternMatch{
			fieldName: "non-existing-column",
			pm:        newPatternMatcher("", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("bar", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("2006-03-02T15:04:05.005Z", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("8007", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		// This filter shouldn't match row=4, since it has different string representation of the timestamp
		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("2006-01-02T16:04:05.005+01:00", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)

		// This filter shouldn't match row=4, since it contains too many digits for millisecond part
		fp = &filterPatternMatch{
			fieldName: "_msg",
			pm:        newPatternMatcher("2006-01-02T15:04:05.00500Z", false),
		}
		testFilterMatchForColumns(t, columns, fp, "_msg", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
