package logstorage

import (
	"testing"
)

func TestParsePipeTimeAddSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`time_add 1h`)
	f(`time_add 1h at foo`)
}

func TestParsePipeTimeAddFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`time_add`)
	f(`time_add at`)
	f(`time_add at foo`)
	f(`time_add 1d at`)
}

func TestPipeTimeAdd(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// time_add for _time field
	f(`time_add 1d`, [][]Field{
		{
			{"_time", `2025-08-20T10:20:30Z`},
			{"bar", `abc`},
		},
		{
			{"_time", `2025-08-22T10:20:30Z`},
			{"x", `y`},
		},
	}, [][]Field{
		{
			{"_time", `2025-08-21T10:20:30.000000000Z`},
			{"bar", `abc`},
		},
		{
			{"_time", `2025-08-23T10:20:30.000000000Z`},
			{"x", `y`},
		},
	})

	// time_add for non-_time field
	f(`time_add -1d at abc`, [][]Field{
		{
			{"_time", `123`},
			{"abc", `2025-08-20T10:20:30Z`},
		},
		{
			{"_time", `2025-08-22T10:20:30Z`},
			{"abc", `foobar`},
		},
	}, [][]Field{
		{
			{"_time", `123`},
			{"abc", `2025-08-19T10:20:30.000000000Z`},
		},
		{
			{"_time", `2025-08-22T10:20:30Z`},
			{"abc", `foobar`},
		},
	})
}

func TestPipeTimeAddUpdateNeededFields(t *testing.T) {
	f := func(s string, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f(`time_add 1h at x`, "*", "", "*", "")

	// unneeded fields do not intersect with the field
	f(`time_add 1h at x`, "*", "f1,f2", "*", "f1,f2")

	// unneeded fields intersect with the field
	f(`time_add 1h at x`, "*", "x", "*", "x")

	// needed fields do not intersect with the field
	f(`time_add 1h at x`, "f1,f2", "", "f1,f2", "")

	// needed fields intersect with the field
	f(`time_add 1h at x`, "f1,f2,x", "", "f1,f2,x", "")
}
