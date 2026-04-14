package logstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestMatchJSONArrayContainsAny(t *testing.T) {
	t.Parallel()

	f := func(s string, values []string, resultExpected bool) {
		t.Helper()

		tokenss := make([][]string, len(values))
		for i, v := range values {
			tokenss[i] = tokenizeStrings(nil, []string{v})
		}

		result := matchJSONArrayContainsAny(s, values, tokenss)
		if result != resultExpected {
			t.Fatalf("unexpected result for s=%q, values=%q; got %v; want %v", s, values, result, resultExpected)
		}
	}

	// Empty values
	f("", nil, false)
	f("foo", nil, false)
	f("[]", nil, false)
	f(`["foo"]`, nil, false)

	// Not JSON array
	f("", []string{"foo"}, false)
	f("foo", []string{"foo"}, false)
	f("{}", []string{"foo"}, false)

	// JSON array doesn't contain the needed values
	f("[]", []string{"foo"}, false)
	f(`["bar"]`, []string{"foo"}, false)
	f(`["bar","baz"]`, []string{"foo"}, false)
	f(`["bar","baz"]`, []string{""}, false)
	f(`[1,2]`, []string{"3"}, false)

	// JSON array contains the needed values
	f(`["foo"]`, []string{"foo", "bar"}, true)
	f(`["bar","foo"]`, []string{"foo"}, true)
	f(`[  "foo"  ,  "bar"  ]`, []string{"abc", "foo", "bar"}, true)
	f(`["foo","bar",""]`, []string{""}, true)
	f(`["a","foo","b"]`, []string{"x", "foo", "y"}, true)

	// Mixed types
	f(`[123]`, []string{"123"}, true)
	f(`[true]`, []string{"true"}, true)
	f(`["123"]`, []string{"123"}, true)
	f(`[null]`, []string{"null"}, true)

	// Leading and trailing whitespace (valid JSON)
	f(" \t\r\n[\"foo\"]  ", []string{"foo"}, true)

	// Tricky cases
	f(`["foo bar"]`, []string{"foo"}, false) // partial match
	f(`["foobar"]`, []string{"foo"}, false)  // partial match
	f(`["foo"]`, []string{"fo"}, false)      // partial match

	// Escaped strings in JSON
	f(`["a\"b"]`, []string{`a"b`}, true)  // \" escape => a"b
	f(`["a\nb"]`, []string{"a\nb"}, true) // \n escape
	f(`["a\/b"]`, []string{"a/b"}, true)  // \/ escape is valid in JSON

	// The \u0062 => 'b' isn't found because of performance reasons.
	f(`["a\u0062"]`, []string{"ab"}, false)

	// Nested structures (ignored by current implementation)
	f(`[{"a":"b"}]`, []string{`{"a":"b"}`}, false) // nested object ignored
	f(`[["a"]]`, []string{`["a"]`}, false)         // nested array ignored

	// Mixed with simple value
	f(`[["a"], "b"]`, []string{"b"}, true)
}

func TestFilterJSONArrayContainsAny(t *testing.T) {
	t.Parallel()

	t.Run("const-column", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					`["a","b"]`,
					`["a","b"]`,
					`["a","b"]`,
				},
			},
		}

		// match
		fa := newFilterJSONArrayContainsAny("foo", []string{"a"})
		testFilterMatchForColumns(t, columns, fa, "foo", []int{0, 1, 2})

		fa = newFilterJSONArrayContainsAny("foo", []string{"b"})
		testFilterMatchForColumns(t, columns, fa, "foo", []int{0, 1, 2})

		// mismatch
		fa = newFilterJSONArrayContainsAny("foo", []string{"c"})
		testFilterMatchForColumns(t, columns, fa, "foo", nil)

		fa = newFilterJSONArrayContainsAny("non-existing-column", []string{"a"})
		testFilterMatchForColumns(t, columns, fa, "foo", nil)
	})

	t.Run("dict", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					"",
					`["a"]`,
					`["b"]`,
					`["a","b"]`,
					`"a"`, // not an array
					`[1,2]`,
				},
			},
		}

		// match
		fa := newFilterJSONArrayContainsAny("foo", []string{"a"})
		testFilterMatchForColumns(t, columns, fa, "foo", []int{1, 3})

		fa = newFilterJSONArrayContainsAny("foo", []string{"b"})
		testFilterMatchForColumns(t, columns, fa, "foo", []int{2, 3})

		// mismatch
		fa = newFilterJSONArrayContainsAny("foo", []string{"c"})
		testFilterMatchForColumns(t, columns, fa, "foo", nil)
	})

	t.Run("strings", func(t *testing.T) {
		columns := []column{
			{
				name: "foo",
				values: []string{
					`["apple", "banana"]`,
					`["orange"]`,
					`not array`,
					`["apple"]`,
					`[]`,
				},
			},
		}

		// match
		fa := newFilterJSONArrayContainsAny("foo", []string{"apple"})
		testFilterMatchForColumns(t, columns, fa, "foo", []int{0, 3})

		// mismatch
		fa = newFilterJSONArrayContainsAny("foo", []string{"pear"})
		testFilterMatchForColumns(t, columns, fa, "foo", nil)
	})

	// Remove the remaining data files for the test
	fs.MustRemoveDir(t.Name())
}
