package logstorage

import (
	"testing"
)

func TestParsePipeDeduplicateSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`deduplicate`)
	f(`deduplicate by (x)`)
	f(`deduplicate by (x, y)`)
}

func TestParsePipeDeduplicateFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`deduplicate by`)
	f(`deduplicate by ()`)
	f(`deduplicate by (*)`)
	f(`deduplicate by (a*)`)
	f(`deduplicate by a*`)
	f(`deduplicate by foo bar`)
	f(`deduplicate foo bar`)
}

func TestPipeDeduplicate(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// deduplicate by all the fields
	f("deduplicate", [][]Field{
		{
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `2`},
			{"b", `4`},
		},
	}, [][]Field{
		{
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `2`},
			{"b", `4`},
		},
	})

	// missing fields are equivalent to empty fields
	f("deduplicate", [][]Field{
		{
			{"a", `2`},
			{"b", ``},
		},
		{
			{"a", `2`},
		},
		{
			{"b", `2`},
		},
	}, [][]Field{
		{
			{"a", `2`},
			{"b", ``},
		},
		{
			{"b", `2`},
		},
	})

	// deduplicate by a single field
	f("deduplicate by (a)", [][]Field{
		{
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `3`},
			{"b", `3`},
		},
	}, [][]Field{
		{
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `3`},
			{"b", `3`},
		},
	})

	// deduplicate by multiple fields
	f("deduplicate by (a, b)", [][]Field{
		{
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `2`},
			{"b", `4`},
		},
		{
			{"a", `2`},
			{"b", `4`},
		},
	}, [][]Field{
		{
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `2`},
			{"b", `4`},
		},
	})

	// deduplicate by a missing field
	f("deduplicate by (x)", [][]Field{
		{
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `3`},
			{"b", `4`},
		},
	}, [][]Field{
		{
			{"a", `2`},
			{"b", `3`},
		},
	})
}

func TestPipeDeduplicateUpdateNeededFields(t *testing.T) {
	f := func(s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f("deduplicate", "*", "", "*", "")
	f("deduplicate by (a)", "*", "", "*", "")

	// all the needed fields, plus unneeded fields
	f("deduplicate", "*", "f1,f2", "*", "")
	f("deduplicate by (a)", "*", "f1,f2", "*", "f1,f2")

	// needed fields
	f("deduplicate", "f1,f2", "", "*", "")
	f("deduplicate by (a)", "f1,f2", "", "a,f1,f2", "")
}
