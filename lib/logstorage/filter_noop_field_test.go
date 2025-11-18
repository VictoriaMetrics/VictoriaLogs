package logstorage

import (
	"testing"
)

func TestFilterNoopField(t *testing.T) {
	t.Parallel()

	t.Run("matches-empty-and-non-empty-values", func(t *testing.T) {
		columns := []column{
			{
				name:   "foo",
				values: []string{"abc", "def", "", "ghi"},
			},
		}

		pf := &filterNoopField{
			fieldName: "foo",
		}
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3})
	})

	t.Run("matches-constant-value-columns", func(t *testing.T) {
		columns := []column{
			{
				name:   "_msg",
				values: []string{"foo", "foo", "foo"},
			},
			{
				name:   "bar",
				values: []string{"", "", ""},
			},
		}

		pf := &filterNoopField{
			fieldName: "_msg",
		}
		testFilterMatchForColumns(t, columns, pf, "_msg", []int{0, 1, 2})

		pf = &filterNoopField{
			fieldName: "bar",
		}
		testFilterMatchForColumns(t, columns, pf, "bar", []int{0, 1, 2})
	})

	t.Run("matches-repeated-values-with-empty", func(t *testing.T) {
		columns := []column{
			{
				name:   "foo",
				values: []string{"", "foobar", "abc", "foobar", "abc", ""},
			},
		}

		pf := &filterNoopField{
			fieldName: "foo",
		}
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3, 4, 5})
	})

	t.Run("matches-diverse-string-content", func(t *testing.T) {
		columns := []column{
			{
				name:   "foo",
				values: []string{"ascii", "ТЕСТЙЦУК", "!!,23.(!1)", ""},
			},
		}

		pf := &filterNoopField{
			fieldName: "foo",
		}
		testFilterMatchForColumns(t, columns, pf, "foo", []int{0, 1, 2, 3})
	})
}

func TestFilterNoopFieldParse(t *testing.T) {
	t.Parallel()

	f := func(s, fieldNameExpected string) {
		t.Helper()
		q, err := ParseQuery(s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fa, ok := q.f.(*filterNoopField)
		if !ok {
			t.Fatalf("unexpected filter type; got %T; want *filterNoopField; filter=%s", q.f, q.f)
		}
		if fa.fieldName != fieldNameExpected {
			t.Fatalf("unexpected fieldName; got %q; want %q", fa.fieldName, fieldNameExpected)
		}
	}
	fErr := func(s string) {
		t.Helper()
		_, err := ParseQuery(s)
		if err == nil {
			t.Fatalf("expected error for query %q, but got none", s)
		}
	}

	f("foo:**", "foo")   // Parses field name from "foo:**"
	f("_msg:**", "_msg") // Parses field name from "_msg:**"
	fErr("foo:prefix**") // Rejects "foo:prefix**" syntax
	fErr("**")           // Rejects bare "**" without field name

	t.Run("equivalence", func(t *testing.T) {
		columns := []column{
			{
				name:   "foo",
				values: []string{"abc", "", "def"},
			},
		}

		faNoopField := &filterNoopField{
			fieldName: "foo",
		}
		faOr := &filterOr{
			filters: []filter{
				&filterPrefix{
					fieldName: "foo",
					prefix:    "",
				},
				&filterPhrase{
					fieldName: "foo",
					phrase:    "",
				},
			},
		}
		testFilterMatchForColumns(t, columns, faNoopField, "foo", []int{0, 1, 2})
		testFilterMatchForColumns(t, columns, faOr, "foo", []int{0, 1, 2})
	})
}

func TestFilterNoopFieldString(t *testing.T) {
	t.Parallel()

	f := func(fieldName, resultExpected string) {
		t.Helper()
		fa := &filterNoopField{
			fieldName: fieldName,
		}
		result := fa.String()
		if result != resultExpected {
			t.Fatalf("unexpected result; got %q; want %q", result, resultExpected)
		}
	}

	f("foo", "foo:**")   // Serializes to "foo:**"
	f("_msg", "_msg:**") // Serializes to "_msg:**" (explicit field name required)
}
