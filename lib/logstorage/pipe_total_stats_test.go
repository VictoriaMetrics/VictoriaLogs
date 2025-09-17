package logstorage

import (
	"testing"
)

func TestParsePipeTotalStatsSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`total_stats count(*) as rows`)
	f(`total_stats count(a*, b) as rows`)
	f(`total_stats by (x, y) count(*) as rows, sum(n) as total_sum`)
}

func TestParsePipeTotalStatsFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`total_stats`)
	f(`total_stats by`)
	f(`total_stats foo`)
	f(`total_stats count`)
	f(`total_stats by(x) foo`)
	f(`total_stats by (*) count()`)
	f(`total_stats by (x*) count()`)
	f(`total_stats count() as *`)
	f(`total_stats count() as x*`)

	// duplicate output name
	f(`total_stats sum() x, count() x`)
}

func TestPipeTotalStats(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// missing result name
	f("total_stats count()", [][]Field{
		{
			{"_time", `bar`},
			{"a", `1`},
		},
		{
			{"_time", "foo"},
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `2`},
			{"b", `54`},
		},
	}, [][]Field{
		{
			{"a", `2`},
			{"b", `54`},
			{"count(*)", "3"},
		},
		{
			{"_time", `bar`},
			{"a", `1`},
			{"count(*)", "3"},
		},
		{
			{"_time", "foo"},
			{"a", `2`},
			{"b", `3`},
			{"count(*)", "3"},
		},
	})

	// set result name
	f("total_stats count() x", [][]Field{
		{
			{"_time", `bar`},
			{"a", `1`},
		},
		{
			{"_time", "foo"},
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `2`},
			{"b", `54`},
		},
	}, [][]Field{
		{
			{"a", `2`},
			{"b", `54`},
			{"x", "3"},
		},
		{
			{"_time", `bar`},
			{"a", `1`},
			{"x", "3"},
		},
		{
			{"_time", "foo"},
			{"a", `2`},
			{"b", `3`},
			{"x", "3"},
		},
	})

	// multiple results
	f("total_stats count() total_count, sum(a) total_sum, min(b) total_b_min, max() total_max_all", [][]Field{
		{
			{"_time", `bar`},
			{"a", `1`},
		},
		{
			{"_time", "foo"},
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `2`},
			{"b", `54`},
		},
	}, [][]Field{
		{
			{"a", `2`},
			{"b", `54`},
			{"total_count", "3"},
			{"total_sum", "5"},
			{"total_b_min", ""},
			{"total_max_all", "foo"},
		},
		{
			{"_time", `bar`},
			{"a", `1`},
			{"total_count", "3"},
			{"total_sum", "5"},
			{"total_b_min", ""},
			{"total_max_all", "foo"},
		},
		{
			{"_time", "foo"},
			{"a", `2`},
			{"b", `3`},
			{"total_count", "3"},
			{"total_sum", "5"},
			{"total_b_min", ""},
			{"total_max_all", "foo"},
		},
	})

	// total stats with groupings
	f("total_stats by (a, b) sum(c) sum_c, min(c) min_c", [][]Field{
		{
			{"_time", "2025-07-25T10:20:30Z"},
			{"a", "foo"},
			{"b", "bar"},
			{"c", "5"},
		},
		{
			{"_time", "2025-07-24T10:20:30Z"},
			{"a", "foo"},
			{"b", "bar"},
			{"c", "8"},
		},
		{
			{"_time", "2025-07-26T10:20:30Z"},
			{"a", "foo"},
			{"b", "bar"},
		},
		{
			{"_time", "2025-07-25T10:20:30Z"},
			{"a", "foo"},
			{"c", "55"},
		},
		{
			{"_time", "2025-07-24T10:20:30Z"},
			{"a", "foo"},
			{"c", "81"},
		},
		{
			{"_time", "2025-07-26T10:20:30Z"},
			{"a", "foo"},
		},
	}, [][]Field{
		{
			{"_time", "2025-07-24T10:20:30Z"},
			{"a", "foo"},
			{"c", "81"},
			{"sum_c", "136"},
			{"min_c", ""},
		},
		{
			{"_time", "2025-07-25T10:20:30Z"},
			{"a", "foo"},
			{"c", "55"},
			{"sum_c", "136"},
			{"min_c", ""},
		},
		{
			{"_time", "2025-07-26T10:20:30Z"},
			{"a", "foo"},
			{"sum_c", "136"},
			{"min_c", ""},
		},
		{
			{"_time", "2025-07-24T10:20:30Z"},
			{"a", "foo"},
			{"b", "bar"},
			{"c", "8"},
			{"sum_c", "13"},
			{"min_c", ""},
		},
		{
			{"_time", "2025-07-25T10:20:30Z"},
			{"a", "foo"},
			{"b", "bar"},
			{"c", "5"},
			{"sum_c", "13"},
			{"min_c", ""},
		},
		{
			{"_time", "2025-07-26T10:20:30Z"},
			{"a", "foo"},
			{"b", "bar"},
			{"sum_c", "13"},
			{"min_c", ""},
		},
	})
}

func TestPipeTotalStatsUpdateNeededFields(t *testing.T) {
	f := func(s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f("total_stats count() r1", "*", "", "*", "r1")
	f("total_stats count(*) r1", "*", "", "*", "r1")
	f("total_stats count(f1,f2) r1", "*", "", "*", "r1")
	f("total_stats count(f1,r1) r1", "*", "", "*", "")
	f("total_stats count(f1,f2) r1, sum(f3,f4) r2", "*", "", "*", "r1,r2")
	f("total_stats by (b1,b2) count(f1,f2) r1", "*", "", "*", "r1")
	f("total_stats by (b1,r2) count(f1,f2) r1, count(f1,f3) r2", "*", "", "*", "r1")

	// all the needed fields, unneeded fields do not intersect with stats fields
	f("total_stats count() r1", "*", "f1,f2", "*", "f1,f2,r1")
	f("total_stats count(*) r1", "*", "f1,f2", "*", "f1,f2,r1")
	f("total_stats count(f1,f2) r1", "*", "f3,f4", "*", "f3,f4,r1")
	f("total_stats count(f1,f5) r1, sum(f3,f4) r2", "*", "f5,f6", "*", "f6,r1,r2")
	f("total_stats by (f4,b2) count(f1,f2) r1", "*", "f3,f4", "*", "f3,r1")
	f("total_stats by (b1,b2) count(f5,f2) r1, count(f1,f3) r2", "*", "f4,f5", "*", "f4,r1,r2")

	// all the needed fields, unneeded fields intersect with stats fields
	f("total_stats count() r1", "*", "r1,r2", "*", "r1,r2")
	f("total_stats count(*) r1", "*", "r1,r2", "*", "r1,r2")
	f("total_stats count(f1,f2) r1", "*", "r1,r2", "*", "r1,r2")
	f("total_stats count(f1,f2) r1, sum(f3,f4) r2", "*", "r1,r3", "*", "r1,r2,r3")
	f("total_stats by (b1,b2) count(f1,f2) r1", "*", "r1,r2", "*", "r1,r2")
	f("total_stats by (b1,b2) count(f1,f2) r1", "*", "r1,r2,b1", "*", "r1,r2")
	f("total_stats by (b1,b2) count(f1,f2) r1", "*", "r1,r2,b1,b2", "*", "r1,r2")
	f("total_stats by (b1,b2) count(f1,f2) r1, count(f1,f3) r2", "*", "r1,r3", "*", "r1,r2,r3")

	// needed fields do not intersect with stats fields
	f("total_stats count() r1", "r2", "", "r2", "")
	f("total_stats count(*) r1", "r2", "", "r2", "")
	f("total_stats count(f1,f2) r1", "r2", "", "r2", "")
	f("total_stats count(f1,f2) r1, sum(f3,f4) r2", "r3", "", "r3", "")
	f("total_stats by (b1,b2) count(f1,f2) r1", "r2", "", "b1,b2,r2", "")
	f("total_stats by (b1,b2) count(f1,f2) r1, count(f1,f3) r2", "r3", "", "b1,b2,r3", "")

	// needed fields intersect with stats fields
	f("total_stats count() r1", "r1,r2", "", "r2", "")
	f("total_stats count(*) r1", "r1,r2", "", "r2", "")
	f("total_stats count(f1,f2) r1", "r1,r2", "", "f1,f2,r2", "")
	f("total_stats count(f1,f2) r1, sum(f3,f4) r2", "r1,r3", "", "f1,f2,r3", "")
	f("total_stats by (b1,b2) count(f1,f2) r1", "r1,r2", "", "b1,b2,f1,f2,r2", "")
	f("total_stats by (b1,b2) count(f1,f2) r1, count(f1,f3) r2", "r1,r3", "", "b1,b2,f1,f2,r3", "")
}
