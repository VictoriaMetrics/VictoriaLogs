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
	f(`coalesce(foo, bar) as result default " "`)
	f(`coalesce(foo, bar) as result default foobar`)
	f(`coalesce(foo, bar) as result default "coalesce"`)
}

func TestParsePipeCoalesceFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`coalesce`)
	f(`coalesce()`)
	f(`coalesce(foo)`)
	f(`coalesce(foo, bar)`)
	f(`coalesce foo, bar as result`)
	f(`coalesce(foo, bar) result`)
	f(`coalesce(foo, bar) as`)
	f(`coalesce(foo,,) as result`)
	f(`coalesce(,foo) as result`)
	f(`coalesce(foo) as result default count()`)
}

func TestPipeCoalesce(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	f("coalesce(a, b) as result", [][]Field{
		{
			{"_msg", `test`},
			{"a", `value_a`},
			{"b", `value_b`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"a", `value_a`},
			{"b", `value_b`},
			{"result", `value_a`},
		},
	})

	f("coalesce(a, b) as result", [][]Field{
		{
			{"_msg", `test`},
			{"a", ``},
			{"b", `value_b`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"a", ``},
			{"b", `value_b`},
			{"result", `value_b`},
		},
	})

	f("coalesce(a, b) as result", [][]Field{
		{
			{"_msg", `test`},
			{"a", ``},
			{"b", ``},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"a", ``},
			{"b", ``},
			{"result", ``},
		},
	})

	f(`coalesce(a, b) as result default "default_value"`, [][]Field{
		{
			{"_msg", `test`},
			{"a", ``},
			{"b", ``},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"a", ``},
			{"b", ``},
			{"result", `default_value`},
		},
	})

	f(`coalesce(x, y, z) as result default "unknown"`, [][]Field{
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
			{"a", ``},
			{"b", `value_b`},
			{"c", `value_c`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"a", ``},
			{"b", `value_b`},
			{"c", `value_c`},
			{"result", `value_b`},
		},
	})

	f("coalesce(a) as result", [][]Field{
		{
			{"_msg", `test`},
			{"a", `value_a`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"a", `value_a`},
			{"result", `value_a`},
		},
	})

	f("coalesce(a, b) as a", [][]Field{
		{
			{"_msg", `test`},
			{"a", ``},
			{"b", `value_b`},
		},
	}, [][]Field{
		{
			{"_msg", `test`},
			{"a", `value_b`},
			{"b", `value_b`},
		},
	})

	f(`coalesce(a, b) as result default ""`, [][]Field{
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

	f("coalesce(s1, s2) as d", "*", "", "*", "d")
	f("coalesce(s1, s2) as d", "*", "f1,f2", "*", "d,f1,f2")
	f("coalesce(s1, s2) as d", "*", "s1,f1,f2", "*", "d,f1,f2")
	f("coalesce(s1, s2) as d", "*", "d,f1,f2", "*", "d,f1,f2")
	f("coalesce(s1, s2) as d", "f1,f2", "", "f1,f2", "")
	f("coalesce(s1, s2) as d", "s1,f1,f2", "", "f1,f2,s1", "")
	f("coalesce(s1, s2) as d", "d,f1,f2", "", "f1,f2,s1,s2", "")
	f("coalesce(s1, s2, s3) as d", "s1,d,f1,f2", "", "f1,f2,s1,s2,s3", "")
}
