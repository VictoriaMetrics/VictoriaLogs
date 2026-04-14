package logstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestMatchAnyCasePhrase(t *testing.T) {
	t.Parallel()

	f := func(s, phraseLowercase string, resultExpected bool) {
		t.Helper()
		result := matchAnyCasePhrase(s, phraseLowercase)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
	}

	// empty phrase matches only empty string
	f("", "", true)
	f("foo", "", false)
	f("тест", "", false)

	// empty string doesn't match non-empty phrase
	f("", "foo", false)
	f("", "тест", false)

	// full match
	f("foo", "foo", true)
	f("FOo", "foo", true)
	f("Test ТЕСт 123", "test тест 123", true)

	// phrase match
	f("a foo", "foo", true)
	f("foo тест bar", "тест", true)
	f("foo ТЕСТ bar", "тест bar", true)

	// mismatch
	f("foo", "fo", false)
	f("тест", "foo", false)
	f("Тест", "ест", false)
}

func TestFilterAnyCasePhrase(t *testing.T) {
	t.Parallel()

	t.Run("single-row", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"aBc DEf",
				},
			},
			{
				name: "other column",
				values: []string{
					"aSDfdsF",
				},
			},
		}

		// match
		pf := newFilterAnyCasePhrase("foo", "Abc")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0})

		pf = newFilterAnyCasePhrase("foo", "abc def")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0})

		pf = newFilterAnyCasePhrase("foo", "def")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0})

		pf = newFilterAnyCasePhrase("other column", "ASdfdsf")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "ab")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("other column", "sd")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("non-existing column", "abc")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
	})

	t.Run("const-column", func(t *testing.T) {
		columns := []column{
			{
				name: "other-column",
				values: []string{
					"X",
					"x",
					"x",
				},
			},
			{
				name: "foo",
				values: []string{
					"aBC def",
					"abc DEf",
					"Abc deF",
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
		pf := newFilterAnyCasePhrase("foo", "abc")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2})

		pf = newFilterAnyCasePhrase("foo", "def")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2})

		pf = newFilterAnyCasePhrase("foo", " def")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2})

		pf = newFilterAnyCasePhrase("foo", "abc def")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2})

		pf = newFilterAnyCasePhrase("other-column", "x")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2})

		pf = newFilterAnyCasePhrase("_msg", " 2 ")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "abc def ")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "x")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("other-column", "foo")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("non-existing column", "x")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("_msg", "foo")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
	})

	t.Run("dict", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"",
					"fooBar",
					"ABc",
					"afdf foobar BAz",
					"fddf fOObARbaz",
					"AfooBarbaz",
					"foobar",
				},
			},
		}

		// match
		pf := newFilterAnyCasePhrase("foo", "FoobAr")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{1, 3, 6})

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0})

		pf = newFilterAnyCasePhrase("foo", "baZ")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{3})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "bar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("non-existing column", "foobar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
	})

	t.Run("strings", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"a foo",
					"A Foobar",
					"aA aBC a",
					"ca afdf a,foobar baz",
					"a fddf foobarbaz",
					"a aFOObarbaz",
					"a foobar",
					"a kjlkjf dfff",
					"a ТЕСТЙЦУК НГКШ ",
					"a !!,23.(!1)",
				},
			},
		}

		// match
		pf := newFilterAnyCasePhrase("foo", "A")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		pf = newFilterAnyCasePhrase("foo", "НгкШ")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{8})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		pf = newFilterAnyCasePhrase("foo", "!,")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{9})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "aa a")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "bar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "@")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
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
		pf := newFilterAnyCasePhrase("foo", "12")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{1, 5})

		pf = newFilterAnyCasePhrase("foo", "0")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{3, 4})

		pf = newFilterAnyCasePhrase("non-existing-clumn", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "bar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "33")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "1234")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
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
		pf := newFilterAnyCasePhrase("foo", "1234")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 4})

		pf = newFilterAnyCasePhrase("foo", "0")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{1})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "bar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "33")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "123456")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
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
		pf := newFilterAnyCasePhrase("foo", "1234")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 4})

		pf = newFilterAnyCasePhrase("foo", "65536")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{3})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "bar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "33")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "12345678901")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
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
		pf := newFilterAnyCasePhrase("foo", "1234")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0})

		pf = newFilterAnyCasePhrase("foo", "12345678901")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{4})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "bar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "33")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "12345678901234567890")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
	})

	t.Run("int64", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"1234",
					"0",
					"3454",
					"65536",
					"-12345678901",
					"1",
					"2",
					"3",
					"4",
				},
			},
		}

		// match
		pf := newFilterAnyCasePhrase("foo", "1234")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0})

		pf = newFilterAnyCasePhrase("foo", "-12345678901")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{4})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "bar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "33")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "12345678901234567890")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
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
		pf := newFilterAnyCasePhrase("foo", "1234")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 4})

		pf = newFilterAnyCasePhrase("foo", "1234.5678901")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{4})

		pf = newFilterAnyCasePhrase("foo", "5678901")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{4})

		pf = newFilterAnyCasePhrase("foo", "-65536")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{3})

		pf = newFilterAnyCasePhrase("foo", "65536")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{3})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "bar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "-1234")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "+1234")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "123")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "5678")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "33")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "12345678901234567890")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
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
		pf := newFilterAnyCasePhrase("foo", "127.0.0.1")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{2, 4, 5, 7})

		pf = newFilterAnyCasePhrase("foo", "127")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{2, 4, 5, 6, 7, 8})

		pf = newFilterAnyCasePhrase("foo", "127.0.0")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{2, 4, 5, 7})

		pf = newFilterAnyCasePhrase("foo", "2.3")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0})

		pf = newFilterAnyCasePhrase("foo", "0")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{1, 2, 4, 5, 6, 7, 8})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		// mismatch
		pf = newFilterAnyCasePhrase("foo", "bar")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "5")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "127.1")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "27.0")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)

		pf = newFilterAnyCasePhrase("foo", "255.255.255.255")
		testFilterMatchForColumns(t, columns, pf, "foo", nil)
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
		pf := newFilterAnyCasePhrase("_msg", "2006-01-02t15:04:05.005z")
		testFilterMatchForColumns(t, columns, pf, "_msg", []int{4})

		pf = newFilterAnyCasePhrase("_msg", "2006-01")
		testFilterMatchForColumns(t, columns, pf, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		pf = newFilterAnyCasePhrase("_msg", "002Z")
		testFilterMatchForColumns(t, columns, pf, "_msg", []int{1})

		pf = newFilterAnyCasePhrase("non-existing-column", "")
		testFilterMatchForColumns(t, columns, pf, "_msg", []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

		// mismatch
		pf = newFilterAnyCasePhrase("_msg", "bar")
		testFilterMatchForColumns(t, columns, pf, "_msg", nil)

		pf = newFilterAnyCasePhrase("_msg", "")
		testFilterMatchForColumns(t, columns, pf, "_msg", nil)

		pf = newFilterAnyCasePhrase("_msg", "2006-03-02T15:04:05.005Z")
		testFilterMatchForColumns(t, columns, pf, "_msg", nil)

		pf = newFilterAnyCasePhrase("_msg", "06")
		testFilterMatchForColumns(t, columns, pf, "_msg", nil)

		// This filter shouldn't match row=4, since it has different string representation of the timestamp
		pf = newFilterAnyCasePhrase("_msg", "2006-01-02T16:04:05.005+01:00")
		testFilterMatchForColumns(t, columns, pf, "_msg", nil)

		// This filter shouldn't match row=4, since it contains too many digits for millisecond part
		pf = newFilterAnyCasePhrase("_msg", "2006-01-02T15:04:05.00500Z")
		testFilterMatchForColumns(t, columns, pf, "_msg", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
