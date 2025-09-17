package logstorage

import (
	"testing"
)

func TestParsePipeRunningStatsSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`running_stats count(*) as rows`)
	f(`running_stats count(a*, b) as rows`)
	f(`running_stats by (x) count(*) as rows, sum(x) as running_sum`)
}

func TestParsePipeRunningStatsFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`running_stats`)
	f(`running_stats by`)
	f(`running_stats foo`)
	f(`running_stats count`)
	f(`running_stats by(x) foo`)
	f(`running_stats by (*) count()`)
	f(`running_stats by (x*) count()`)
	f(`running_stats count() as *`)
	f(`running_stats count() as x*`)

	// duplicate output name
	f(`running_stats sum() x, count() x`)
}

func TestPipeRunningStats(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// missing result name
	f("running_stats count()", [][]Field{
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
			{"count(*)", "1"},
		},
		{
			{"_time", `bar`},
			{"a", `1`},
			{"count(*)", "2"},
		},
		{
			{"_time", "foo"},
			{"a", `2`},
			{"b", `3`},
			{"count(*)", "3"},
		},
	})

	// set result name
	f("running_stats count() x", [][]Field{
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
			{"x", "1"},
		},
		{
			{"_time", `bar`},
			{"a", `1`},
			{"x", "2"},
		},
		{
			{"_time", "foo"},
			{"a", `2`},
			{"b", `3`},
			{"x", "3"},
		},
	})

	// multiple results
	f("running_stats count() running_count, sum(a) running_sum, min(b) running_b_min, max() running_max_all", [][]Field{
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
			{"running_count", "1"},
			{"running_sum", "2"},
			{"running_b_min", "54"},
			{"running_max_all", "54"},
		},
		{
			{"_time", `bar`},
			{"a", `1`},
			{"running_count", "2"},
			{"running_sum", "3"},
			{"running_b_min", ""},
			{"running_max_all", "bar"},
		},
		{
			{"_time", "foo"},
			{"a", `2`},
			{"b", `3`},
			{"running_count", "3"},
			{"running_sum", "5"},
			{"running_b_min", ""},
			{"running_max_all", "foo"},
		},
	})

	// running stats with groupings
	f("running_stats by (a, b) sum(c) running_c, min(c) min_c", [][]Field{
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
			{"running_c", "81"},
			{"min_c", "81"},
		},
		{
			{"_time", "2025-07-25T10:20:30Z"},
			{"a", "foo"},
			{"c", "55"},
			{"running_c", "136"},
			{"min_c", "55"},
		},
		{
			{"_time", "2025-07-26T10:20:30Z"},
			{"a", "foo"},
			{"running_c", "136"},
			{"min_c", ""},
		},
		{
			{"_time", "2025-07-24T10:20:30Z"},
			{"a", "foo"},
			{"b", "bar"},
			{"c", "8"},
			{"running_c", "8"},
			{"min_c", "8"},
		},
		{
			{"_time", "2025-07-25T10:20:30Z"},
			{"a", "foo"},
			{"b", "bar"},
			{"c", "5"},
			{"running_c", "13"},
			{"min_c", "5"},
		},
		{
			{"_time", "2025-07-26T10:20:30Z"},
			{"a", "foo"},
			{"b", "bar"},
			{"running_c", "13"},
			{"min_c", ""},
		},
	})
}

func TestPipeRunningStatsUpdateNeededFields(t *testing.T) {
	f := func(s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f("running_stats count() r1", "*", "", "*", "r1")
	f("running_stats count(*) r1", "*", "", "*", "r1")
	f("running_stats count(f1,f2) r1", "*", "", "*", "r1")
	f("running_stats count(f1,r1) r1", "*", "", "*", "")
	f("running_stats count(f1,f2) r1, sum(f3,f4) r2", "*", "", "*", "r1,r2")
	f("running_stats by (b1,b2) count(f1,f2) r1", "*", "", "*", "r1")
	f("running_stats by (b1,r2) count(f1,f2) r1, count(f1,f3) r2", "*", "", "*", "r1")

	// all the needed fields, unneeded fields do not intersect with stats fields
	f("running_stats count() r1", "*", "f1,f2", "*", "f1,f2,r1")
	f("running_stats count(*) r1", "*", "f1,f2", "*", "f1,f2,r1")
	f("running_stats count(f1,f2) r1", "*", "f3,f4", "*", "f3,f4,r1")
	f("running_stats count(f1,f5) r1, sum(f3,f4) r2", "*", "f5,f6", "*", "f6,r1,r2")
	f("running_stats by (f4,b2) count(f1,f2) r1", "*", "f3,f4", "*", "f3,r1")
	f("running_stats by (b1,b2) count(f5,f2) r1, count(f1,f3) r2", "*", "f4,f5", "*", "f4,r1,r2")

	// all the needed fields, unneeded fields intersect with stats fields
	f("running_stats count() r1", "*", "r1,r2", "*", "r1,r2")
	f("running_stats count(*) r1", "*", "r1,r2", "*", "r1,r2")
	f("running_stats count(f1,f2) r1", "*", "r1,r2", "*", "r1,r2")
	f("running_stats count(f1,f2) r1, sum(f3,f4) r2", "*", "r1,r3", "*", "r1,r2,r3")
	f("running_stats by (b1,b2) count(f1,f2) r1", "*", "r1,r2", "*", "r1,r2")
	f("running_stats by (b1,b2) count(f1,f2) r1", "*", "r1,r2,b1", "*", "r1,r2")
	f("running_stats by (b1,b2) count(f1,f2) r1", "*", "r1,r2,b1,b2", "*", "r1,r2")
	f("running_stats by (b1,b2) count(f1,f2) r1, count(f1,f3) r2", "*", "r1,r3", "*", "r1,r2,r3")

	// needed fields do not intersect with stats fields
	f("running_stats count() r1", "r2", "", "r2", "")
	f("running_stats count(*) r1", "r2", "", "r2", "")
	f("running_stats count(f1,f2) r1", "r2", "", "r2", "")
	f("running_stats count(f1,f2) r1, sum(f3,f4) r2", "r3", "", "r3", "")
	f("running_stats by (b1,b2) count(f1,f2) r1", "r2", "", "b1,b2,r2", "")
	f("running_stats by (b1,b2) count(f1,f2) r1, count(f1,f3) r2", "r3", "", "b1,b2,r3", "")

	// needed fields intersect with stats fields
	f("running_stats count() r1", "r1,r2", "", "r2", "")
	f("running_stats count(*) r1", "r1,r2", "", "r2", "")
	f("running_stats count(f1,f2) r1", "r1,r2", "", "f1,f2,r2", "")
	f("running_stats count(f1,f2) r1, sum(f3,f4) r2", "r1,r3", "", "f1,f2,r3", "")
	f("running_stats by (b1,b2) count(f1,f2) r1", "r1,r2", "", "b1,b2,f1,f2,r2", "")
	f("running_stats by (b1,b2) count(f1,f2) r1, count(f1,f3) r2", "r1,r3", "", "b1,b2,f1,f2,r3", "")
}
