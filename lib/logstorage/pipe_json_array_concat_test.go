package logstorage

import (
	"testing"
)

func TestParsePipeJSONArrayConcatSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`json_array_concat`)
	f(`json_array_concat ","`)
	f(`json_array_concat ", "`)
	f(`json_array_concat from foo`)
	f(`json_array_concat as bar`)
	f(`json_array_concat "," from foo`)
	f(`json_array_concat "," as bar`)
	f(`json_array_concat from foo as bar`)
	f(`json_array_concat "," from foo as bar`)
}

func TestParsePipeJSONArrayConcatFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`json_array_concat from`)
	f(`json_array_concat "," from *`)
	f(`json_array_concat "," from foo*`)
	f(`json_array_concat "," from foo as`)
	f(`json_array_concat "," from foo as *`)
	f(`json_array_concat "," from foo as bar*`)
	f(`json_array_concat "," from foo as bar baz`)
}

func TestPipeJSONArrayConcat(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// basic
	f(`json_array_concat "," from foo`, [][]Field{
		{{"foo", `["a","b","c"]`}},
	}, [][]Field{
		{{"foo", "a,b,c"}},
	})

	// no delimiter
	f(`json_array_concat from foo`, [][]Field{
		{{"foo", `["a","b","c"]`}},
	}, [][]Field{
		{{"foo", "abc"}},
	})

	// no "from" keyword
	f(`json_array_concat "," foo`, [][]Field{
		{{"foo", `["a","b","c"]`}},
	}, [][]Field{
		{{"foo", "a,b,c"}},
	})

	// no "from" and no "as" keyword
	f(`json_array_concat "," foo bar`, [][]Field{
		{{"foo", `["a","b","c"]`}},
	}, [][]Field{
		{{"foo", `["a","b","c"]`}, {"bar", "a,b,c"}},
	})

	// single element
	f(`json_array_concat "," from foo`, [][]Field{
		{{"foo", `["only"]`}},
	}, [][]Field{
		{{"foo", "only"}},
	})

	// non-string item types
	f(`json_array_concat "," from foo`, [][]Field{
		{{"foo", `["hello",42,true,{"a":1},null]`}},
	}, [][]Field{
		{{"foo", `hello,42,true,{"a":1},null`}},
	})

	// empty array
	f(`json_array_concat "," from foo`, [][]Field{
		{{"foo", `[]`}},
	}, [][]Field{
		{{"foo", ""}},
	})

	// non-array input
	f(`json_array_concat "," from foo`, [][]Field{
		{{"foo", `not-an-array`}},
	}, [][]Field{
		{{"foo", ""}},
	})

	// missing source field
	f(`json_array_concat "," from foo`, [][]Field{
		{{"bar", "baz"}},
	}, [][]Field{
		{{"bar", "baz"}, {"foo", ""}},
	})

	// write result to a different field, source field unchanged
	f(`json_array_concat "," from foo as result`, [][]Field{
		{{"foo", `["a","b","c"]`}},
	}, [][]Field{
		{{"foo", `["a","b","c"]`}, {"result", "a,b,c"}},
	})

	// default source and result field (_msg)
	f(`json_array_concat ","`, [][]Field{
		{{"_msg", `["x","y","z"]`}},
	}, [][]Field{
		{{"_msg", "x,y,z"}},
	})

	// JSON whitespace around JSON array
	f(`json_array_concat "," from foo`, [][]Field{
		{{"foo", `  ["a","b","c"]`}},
		{{"foo", "\t[\"d\",\"e\",\"f\"]"}},
		{{"foo", "\n[\"g\",\"h\",\"i\"]"}},
		{{"foo", "\r[\"j\",\"k\",\"l\"]"}},
		{{"foo", " \t\n\r[\"m\",\"n\",\"o\"] \r\n\t"}},
	}, [][]Field{
		{{"foo", "a,b,c"}},
		{{"foo", "d,e,f"}},
		{{"foo", "g,h,i"}},
		{{"foo", "j,k,l"}},
		{{"foo", "m,n,o"}},
	})

	// malformed JSON array starting with [
	f(`json_array_concat "," from foo`, [][]Field{
		{{"foo", `["a"`}},
		{{"foo", `[1,`}},
	}, [][]Field{
		{{"foo", ""}},
		{{"foo", ""}},
	})

	// slow path: multiple rows with different and repeated values
	f(`json_array_concat "," from foo`, [][]Field{
		{{"foo", `["a","b"]`}},
		{{"foo", `["x","y","z"]`}},
		{{"foo", `["a","b"]`}},
	}, [][]Field{
		{{"foo", "a,b"}},
		{{"foo", "x,y,z"}},
		{{"foo", "a,b"}},
	})
}

func TestPipeJSONArrayConcatUpdateNeededFields(t *testing.T) {
	f := func(s string, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f(`json_array_concat "," from y as x`, "*", "", "*", "x")
	f(`json_array_concat "," from x as x`, "*", "", "*", "")

	// unneeded fields do not intersect with output field
	f(`json_array_concat "," from y as x`, "*", "f1,f2", "*", "f1,f2,x")
	f(`json_array_concat "," from x as x`, "*", "f1,f2", "*", "f1,f2")

	// unneeded fields intersect with output field
	f(`json_array_concat "," from z as x`, "*", "x,y", "*", "x,y")
	f(`json_array_concat "," from y as x`, "*", "x,y", "*", "x,y")
	f(`json_array_concat "," from x as x`, "*", "x,y", "*", "x,y")

	// needed fields do not intersect with output field
	f(`json_array_concat "," from y as z`, "x,y", "", "x,y", "")
	f(`json_array_concat "," from z as z`, "x,y", "", "x,y", "")

	// needed fields intersect with output field
	f(`json_array_concat "," from z as f2`, "f2,y", "", "y,z", "")
	f(`json_array_concat "," from y as f2`, "f2,y", "", "y", "")
	f(`json_array_concat "," from y as y`, "f2,y", "", "f2,y", "")
}
