package logstorage

import (
	"reflect"
	"testing"
)

func TestParsePipeSplitSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`split ","`)
	f(`split "-" as bar`)
	f(`split ";" from foo`)
	f(`split ". " from foo as bar`)
}

func TestParsePipeSplitFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`split`)
	f(`split as x`)
	f(`split " " as *`)
	f(`split " " as`)
	f(`split " " from`)
	f(`split " " from *`)
	f(`split " " from x*`)
	f(`split foo bar, baz`)
	f(`split foo, bar`)
}

func TestPipeSplit(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// split by missing field
	f("split ',' x", [][]Field{
		{
			{"a", `["foo",1,{"baz":"x"},[1,2],null,NaN]`},
			{"q", "w"},
		},
	}, [][]Field{
		{
			{"a", `["foo",1,{"baz":"x"},[1,2],null,NaN]`},
			{"q", "w"},
			{"x", `[""]`},
		},
	})

	// split by a field without separators
	f("split ' ' q", [][]Field{
		{
			{"a", `["foo",1,{"baz":"x"},[1,2],null,NaN]`},
			{"q", "!#$%,"},
		},
	}, [][]Field{
		{
			{"a", `["foo",1,{"baz":"x"},[1,2],null,NaN]`},
			{"q", `["!#$%,"]`},
		},
	})

	// split by a field with separators
	f("split ', ' a", [][]Field{
		{
			{"a", `foo, bar baz`},
			{"q", "w"},
		},
		{
			{"a", "b,c, d, ef"},
			{"c", "d"},
		},
	}, [][]Field{
		{
			{"a", `["foo","bar baz"]`},
			{"q", "w"},
		},
		{
			{"a", `["b,c","d","ef"]`},
			{"c", "d"},
		},
	})

	// split by empty separator
	f("split '' a", [][]Field{
		{
			{"a", `foo,bar`},
			{"q", "w"},
		},
		{
			{"a", "b,c"},
			{"c", "d"},
		},
	}, [][]Field{
		{
			{"a", `["f","o","o",",","b","a","r"]`},
			{"q", "w"},
		},
		{
			{"a", `["b",",","c"]`},
			{"c", "d"},
		},
	})

	// split into another field
	f("split ',' from a as b", [][]Field{
		{
			{"a", `foo,bar baz`},
			{"q", "w"},
		},
		{
			{"a", "b"},
			{"c", "d"},
		},
	}, [][]Field{
		{
			{"a", `foo,bar baz`},
			{"b", `["foo","bar baz"]`},
			{"q", "w"},
		},
		{
			{"a", "b"},
			{"b", `["b"]`},
			{"c", "d"},
		},
	})

	// split from _msg inplace
	f("split ','", [][]Field{
		{
			{"_msg", `foo,bar baz`},
			{"q", "w"},
		},
		{
			{"_msg", "b"},
			{"c", "d"},
		},
	}, [][]Field{
		{
			{"_msg", `["foo","bar baz"]`},
			{"q", "w"},
		},
		{
			{"_msg", `["b"]`},
			{"c", "d"},
		},
	})

	// split from _msg into other field
	f("split ',' as b", [][]Field{
		{
			{"_msg", `foo,bar foo`},
			{"q", "w"},
		},
		{
			{"_msg", "b"},
			{"c", "d"},
		},
	}, [][]Field{
		{
			{"_msg", `foo,bar foo`},
			{"b", `["foo","bar foo"]`},
			{"q", "w"},
		},
		{
			{"_msg", "b"},
			{"b", `["b"]`},
			{"c", "d"},
		},
	})
}

func TestPipeSplitUpdateNeededFields(t *testing.T) {
	f := func(s string, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f("split ' ' x", "*", "", "*", "")
	f("split ' ' x y", "*", "", "*", "y")

	// all the needed fields, unneeded fields do not intersect with src
	f("split ' ' x", "*", "f1,f2", "*", "f1,f2")
	f("split ' ' x as y", "*", "f1,f2", "*", "f1,f2,y")

	// all the needed fields, unneeded fields intersect with src
	f("split ' ' x", "*", "f2,x", "*", "f2,x")
	f("split ' ' x y", "*", "f2,x", "*", "f2,y")
	f("split ' ' x y", "*", "f2,y", "*", "f2,y")

	// needed fields do not intersect with src
	f("split ' ' x", "f1,f2", "", "f1,f2", "")
	f("split ' ' x y", "f1,f2", "", "f1,f2", "")

	// needed fields intersect with src
	f("split ' ' x", "f2,x", "", "f2,x", "")
	f("split ' ' x y", "f2,x", "", "f2,x", "")
	f("split ' ' x y", "f2,y", "", "f2,x", "")
}

func TestSplitString(t *testing.T) {
	f := func(s, separator string, resultExpected []string) {
		t.Helper()

		result := splitString(nil, s, separator)
		if !reflect.DeepEqual(result, resultExpected) {
			t.Fatalf("unexpected result\ngot\n%q\nwant\n%q", result, resultExpected)
		}
	}

	// empty input string
	f("", "", nil)
	f("", "foobar", []string{""})

	// empty separator
	f("Шzч", "", []string{"Ш", "z", "ч"})

	// single-char delimiter
	f(",foo,bar,,baz,", ",", []string{"", "foo", "bar", "", "baz", ""})

	// multi-char delimiter with unicode chars
	f("йцуквенгшвевоы", "ве", []string{"йцук", "нгш", "воы"})

	// missing separator
	f("foobar", "aaaa", []string{"foobar"})
}
