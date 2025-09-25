package logstorage

import (
	"testing"
)

func TestParsePipeSetStreamFieldsSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`set_stream_fields foo`)
	f(`set_stream_fields foo, bar`)
	f(`set_stream_fields if (x:y) foo, bar`)
	f(`set_stream_fields "if"`)
	f(`set_stream_fields "by"`)
	f(`set_stream_fields "as"`)
}

func TestParsePipeSetStreamFieldsFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`set_stream_fields`)
	f(`set_stream_fields if`)
	f(`set_stream_fields foo bar`)
}

func TestPipeSetStreamFields(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	f(`set_stream_fields foo,bar`, [][]Field{
		{
			{"foo", `aaa`},
			{"bar", `bb`},
			{"baz", "c"},
		},
		{
			{"_stream_id", "asbc"},
			{"_stream", "asdfdsafs"},
			{"foo", `abc`},
			{"baz", "ghkl"},
			{"d", "foobar"},
		},
	}, [][]Field{
		{
			{"_stream_id", ``},
			{"_stream", `{bar="bb",foo="aaa"}`},
			{"foo", `aaa`},
			{"bar", `bb`},
			{"baz", "c"},
		},
		{
			{"_stream_id", ``},
			{"_stream", `{foo="abc"}`},
			{"foo", `abc`},
			{"baz", "ghkl"},
			{"d", "foobar"},
		},
	})

	// conditional set_stream_fields
	f(`set_stream_fields if (baz:c) foo,bar`, [][]Field{
		{
			{"foo", `aaa`},
			{"bar", `bb`},
			{"baz", "c"},
		},
		{
			{"_stream_id", "asbc"},
			{"_stream", "asdfdsafs"},
			{"foo", `abc`},
			{"baz", "ghkl"},
			{"d", "foobar"},
		},
	}, [][]Field{
		{
			{"_stream_id", ``},
			{"_stream", `{bar="bb",foo="aaa"}`},
			{"foo", `aaa`},
			{"bar", `bb`},
			{"baz", "c"},
		},
		{
			{"_stream_id", "asbc"},
			{"_stream", "asdfdsafs"},
			{"foo", `abc`},
			{"baz", "ghkl"},
			{"d", "foobar"},
		},
	})
}

func TestPipeSetStreamFieldsUpdateNeededFields(t *testing.T) {
	f := func(s string, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f(`set_stream_fields x,y`, "*", "", "*", "_stream")
	f(`set_stream_fields if (f1:a) x,y`, "*", "", "*", "")

	// unneeded fields do not intersect with the requested fields
	f(`set_stream_fields x,y`, "*", "f1,f2", "*", "_stream,f1,f2")
	f(`set_stream_fields if (f1:a) x,y`, "*", "f1,f2", "*", "f2")

	// unneeded fields intersect with the requested fields
	f(`set_stream_fields f1,y`, "*", "f1,f2", "*", "_stream,f2")
	f(`set_stream_fields if (f1:a) x,f2`, "*", "f1,f2", "*", "")

	// needed fields do not intersect with the requested fields
	f(`set_stream_fields x,y`, "f1,f2", "", "f1,f2", "")
	f(`set_stream_fields if (f1:a f3:b) x,y`, "f1,f2", "", "f1,f2", "")

	// needed fields intersect with output field
	f(`set_stream_fields x,y`, "f1,f2", "", "f1,f2", "")
	f(`set_stream_fields x,y`, "f1,f2,_stream", "", "f1,f2,x,y", "")
	f(`set_stream_fields if (f1:a f3:b) x,y`, "f1,f2", "", "f1,f2", "")
	f(`set_stream_fields if (f1:a f3:b) x,y`, "f1,f2,_stream", "", "_stream,f1,f2,f3,x,y", "")
}
