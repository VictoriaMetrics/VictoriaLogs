package logstorage

import "testing"

func TestParsePipeTopOverTimeSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`top_over_time step 1h by (x)`)
	f(`top_over_time step 1h 5 by (x, y)`)
	f(`top_over_time step 1h by (x) rank`)
	f(`top_over_time step 1h by (x) hits as foo`)
	f(`top_over_time step 1h offset 30m by (x)`)
	f(`bottom_over_time step 5m by (a)`)
}

func TestParsePipeTopOverTimeFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`top_over_time`)
	f(`top_over_time 1h`)
	f(`top_over_time step by (x)`)
	f(`top_over_time step 1h`)
	f(`top_over_time step 0 by (x)`)
	f(`top_over_time step 1h by ()`)
	f(`top_over_time step 1h by (_time)`)
	f(`bottom_over_time step 1h`)
}

func TestPipeTopOverTime(t *testing.T) {
	rows := [][]Field{
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "1"},
		},
		{
			{"_time", "2024-01-01T00:00:30Z"},
			{"ip", "2"},
		},
		{
			{"_time", "2024-01-01T00:00:45Z"},
			{"ip", "1"},
		},
		{
			{"_time", "2024-01-01T01:00:00Z"},
			{"ip", "3"},
		},
		{
			{"_time", "2024-01-01T01:00:10Z"},
			{"ip", "3"},
		},
		{
			{"_time", "2024-01-01T01:00:20Z"},
			{"ip", "4"},
		},
	}

	expectPipeResults(t, "top_over_time step 1m 1 by (ip)", rows, [][]Field{
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "1"},
			{"hits", "2"},
		},
		{
			{"_time", "2024-01-01T01:00:00Z"},
			{"ip", "3"},
			{"hits", "2"},
		},
	})

	expectPipeResults(t, "bottom_over_time step 1m 1 by (ip)", rows, [][]Field{
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "2"},
			{"hits", "1"},
		},
		{
			{"_time", "2024-01-01T01:00:00Z"},
			{"ip", "4"},
			{"hits", "1"},
		},
	})

	expectPipeResults(t, "top_over_time step 1m 2 by (ip) rank as pos", rows, [][]Field{
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "1"},
			{"hits", "2"},
			{"pos", "1"},
		},
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "2"},
			{"hits", "1"},
			{"pos", "2"},
		},
		{
			{"_time", "2024-01-01T01:00:00Z"},
			{"ip", "3"},
			{"hits", "2"},
			{"pos", "1"},
		},
		{
			{"_time", "2024-01-01T01:00:00Z"},
			{"ip", "4"},
			{"hits", "1"},
			{"pos", "2"},
		},
	})

	// hits rename + rank
	expectPipeResults(t, "top_over_time step 1m by (ip) hits as cnt rank", rows, [][]Field{
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "1"},
			{"cnt", "2"},
			{"rank", "1"},
		},
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "2"},
			{"cnt", "1"},
			{"rank", "2"},
		},
		{
			{"_time", "2024-01-01T01:00:00Z"},
			{"ip", "3"},
			{"cnt", "2"},
			{"rank", "1"},
		},
		{
			{"_time", "2024-01-01T01:00:00Z"},
			{"ip", "4"},
			{"cnt", "1"},
			{"rank", "2"},
		},
	})

	// multiple by fields, default limit (10) keeps all in bucket
	rowsMulti := [][]Field{
		{{"_time", "2024-01-01T00:00:00Z"}, {"ip", "1"}, {"path", "/a"}},
		{{"_time", "2024-01-01T00:00:10Z"}, {"ip", "1"}, {"path", "/a"}},
		{{"_time", "2024-01-01T00:00:20Z"}, {"ip", "1"}, {"path", "/b"}},
		{{"_time", "2024-01-01T00:00:30Z"}, {"ip", "2"}, {"path", "/a"}},
		{{"_time", "2024-01-01T00:00:40Z"}, {"ip", "2"}, {"path", "/a"}},
	}
	expectPipeResults(t, "top_over_time step 1m by (ip, path)", rowsMulti, [][]Field{
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "2"},
			{"path", "/a"},
			{"hits", "2"},
		},
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "1"},
			{"path", "/a"},
			{"hits", "2"},
		},
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "1"},
			{"path", "/b"},
			{"hits", "1"},
		},
	})

	// bottom with rank rename
	expectPipeResults(t, "bottom_over_time step 1m 2 by (ip) rank as idx", rows, [][]Field{
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "2"},
			{"hits", "1"},
			{"idx", "1"},
		},
		{
			{"_time", "2024-01-01T00:00:00Z"},
			{"ip", "1"},
			{"hits", "2"},
			{"idx", "2"},
		},
		{
			{"_time", "2024-01-01T01:00:00Z"},
			{"ip", "4"},
			{"hits", "1"},
			{"idx", "1"},
		},
		{
			{"_time", "2024-01-01T01:00:00Z"},
			{"ip", "3"},
			{"hits", "2"},
			{"idx", "2"},
		},
	})
}
