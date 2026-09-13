package logstorage

import (
	"testing"
)

func TestParsePipeCoalesceSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`coalesce(a) as b`)
	f(`coalesce(foo, bar) as result`)
	f(`coalesce(foo, bar, baz) as result`)
	f(`coalesce(foo, bar) default " " as result`)
	f(`coalesce(foo, bar) default foobar as result`)
	f(`coalesce(foo, bar) default "coalesce" as result`)
	f(`coalesce("foo bar", "foo-bar") as result`)

	// prefix filters
	f(`coalesce(foo*, bar, baz*)`)
	f(`coalesce(foo*, bar, baz*) default x as abc`)
}

func TestParsePipeCoalesceFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`coalesce`)
	f(`coalesce()`)
	f(`coalesce(foo) as`)
	f(`coalesce(foo) x`)
	f(`coalesce(foo) as x*`)
	f(`coalesce foo, bar as result`)
	f(`coalesce(foo, bar) result`)
	f(`coalesce(foo, bar) as`)
	f(`coalesce(foo,,) as result`)
	f(`coalesce(,foo) as result`)
	f(`coalesce(foo) default count() as result`)
}

func TestPipeCoalesce(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// a single value with default value
	f("coalesce(a) default 'foo'", [][]Field{
		{
			{"_msg", `test`},
			{"b", `value_b`},
		},
		{
			{"a", `value_a`},
		},
	}, [][]Field{
		{
			{"_msg", `foo`},
			{"b", `value_b`},
		},
		{
			{"_msg", `value_a`},
			{"a", `value_a`},
		},
	})

	// field prefix
	f("coalesce(a*, b)", [][]Field{
		{
			{"_msg", `test`},
			{"abc", `value_a`},
			{"b", `value_b`},
		},
		{
			{"_msg", `test`},
			{"b", `value_b`},
		},
		{
			{"_msg", `test`},
		},
	}, [][]Field{
		{
			{"_msg", `value_a`},
			{"abc", `value_a`},
			{"b", `value_b`},
		},
		{
			{"_msg", `value_b`},
			{"b", `value_b`},
		},
		{
			{"_msg", ``},
		},
	})

	// multiple values
	f("coalesce(a, b)", [][]Field{
		{
			{"_msg", `test`},
			{"a", `value_a`},
			{"b", `value_b`},
		},
		{
			{"_msg", `test`},
			{"b", `value_b`},
		},
		{
			{"_msg", `test`},
			{"a", `value_a`},
		},
		{
			{"_msg", `test`},
		},
	}, [][]Field{
		{
			{"_msg", `value_a`},
			{"a", `value_a`},
			{"b", `value_b`},
		},
		{
			{"_msg", `value_b`},
			{"b", `value_b`},
		},
		{
			{"_msg", `value_a`},
			{"a", `value_a`},
		},
		{
			{"_msg", ``},
		},
	})

	f("coalesce(a, b) as result", [][]Field{
		{
			{"_msg", `test`},
			{"b", `value_b`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"b", `value_b`},
			{"result", `value_b`},
		},
	})

	f(`coalesce(a, b) default "default_value" as result`, [][]Field{
		{
			{"_msg", `test`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"result", `default_value`},
		},
	})

	f(`coalesce(x, y, z) default "unknown" as result`, [][]Field{
		{
			{"_msg", `test`},
			{"a", `value`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"a", `value`},
			{"result", `unknown`},
		},
	})

	f("coalesce(a, b, c) as result", [][]Field{
		{
			{"_msg", `test`},
			{"b", `value_b`},
			{"c", `value_c`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"b", `value_b`},
			{"c", `value_c`},
			{"result", `value_b`},
		},
	})

	f(`coalesce(a, b) default "" as result`, [][]Field{
		{
			{"_msg", `test`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"result", ``},
		},
	})
}

func TestPipeCoalesceUpdateNeededFields(t *testing.T) {
	f := func(s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f("coalesce(s1, s2) as d", "*", "", "*", "d")

	// the destination field intersects with the source field
	f("coalesce(s1, s2) as s1", "*", "", "*", "")

	// all the needed fields, unneded fields do not intersect with source fields
	f("coalesce(s1, s2) as d", "*", "f1,f2", "*", "d,f1,f2")

	// all the needed fields, unneeded fields intersect with the source fields
	f("coalesce(s1, s2) as d", "*", "s1,f1,f2", "*", "d,f1,f2")

	// all the needed fields, unneded field intersects with the destination
	f("coalesce(s1, s2) as d", "*", "d,f1,f2", "*", "d,f1,f2")
	f("coalesce(s1, s2) as s1", "*", "s1,f1,f2", "*", "f1,f2,s1")

	// Needed fields do not intersect with the destination
	f("coalesce(s1, s2) as d", "f1,f2", "", "f1,f2", "")
	f("coalesce(s1, s2) as d", "s1,f1,f2", "", "f1,f2,s1", "")

	// needed fields intersect with the destination
	f("coalesce(s1, s2) as d", "d,f1,f2", "", "f1,f2,s1,s2", "")
	f("coalesce(s1, s2*, s3) as d", "s1,d,f1,f2", "", "f1,f2,s1,s2*,s3", "")
}
