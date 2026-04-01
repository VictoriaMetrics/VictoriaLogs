package logstorage

import (
	"reflect"
	"testing"
)

func TestParsePipeJoinSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`join by (foo) (error)`)
	f(`join by (foo, bar) (a:b | fields x, y)`)
	f(`join by (foo) (a:b) prefix c`)
	f(`join by (foo) (bar | join by (x, z) (y))`)
	f(`join by (x) (y) inner`)
	f(`join by (x) (y) inner prefix a.b`)

	// inline rows
	f(`join by (x) rows({"x":"y","z":"qwe"},{"x":"123","z":"456"})`)
	f(`join by (x) rows({"x":"y","z":"qwe"},{"x":"123","z":"456"}) prefix abc`)
	f(`join by (x) rows({"x":"y","z":"qwe"},{"x":"123","z":"456"}) inner prefix abc`)

	// stream filter in subquery
	f(`join by (x) ({foo="bar"})`)
}

func TestParsePipeJoinFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`join`)
	f(`join by () (abc)`)
	f(`join by (*) (abc)`)
	f(`join by (f, *) (abc)`)
	f(`join by (x)`)
	f(`join by`)
	f(`join (`)
	f(`join by (foo) bar`)
	f(`join by (x) ()`)
	f(`join by (x) (`)
	f(`join by (x) (abc`)
	f(`join (x) (y) prefix`)
	f(`join (x) (y) prefix |`)
	f(`join by (x) (y) inner prefix x inner`)

	// invalid inline rows
	f(`join by (x) rows({foo,bar})`)
}

func TestPipeJoinUpdateNeededFields(t *testing.T) {
	f := func(s string, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f("join on (x, y) (abc)", "*", "", "*", "")

	// all the needed fields, unneeded fields do not intersect with src
	f("join on (x, y) (abc) inner", "*", "f1,f2", "*", "f1,f2")

	// all the needed fields, unneeded fields intersect with src
	f("join on (x, y) (abc)", "*", "f2,x", "*", "f2")

	// needed fields do not intersect with src
	f("join on (x, y) (abc)", "f1,f2", "", "f1,f2,x,y", "")

	// needed fields intersect with src
	f("join on (x, y) (abc)", "f2,x", "", "f2,x,y", "")
}

func TestParseRows_Success(t *testing.T) {
	f := func(s string, resultExpected []string) {
		t.Helper()

		lex := newLexer(s, 0)
		rows, err := parseRows(lex)
		if err != nil {
			t.Fatalf("unexpected error in parseRows: %s", err)
		}
		if !lex.isEnd() {
			t.Fatalf("unexpected tail left: [%s]", lex.rawToken+lex.s)
		}

		var result []string
		for _, row := range rows {
			result = append(result, string(MarshalFieldsToJSON(nil, row)))
		}
		if !reflect.DeepEqual(result, resultExpected) {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}

	f("rows()", nil)
	f(`rows({})`, []string{`{}`})
	f(` rows ( {  } ) `, []string{`{}`})
	f(`rows({"a":"b"},{"c":"d","qwe":"rty"})`, []string{`{"a":"b"}`, `{"c":"d","qwe":"rty"}`})
	f(`rows({ "a" : "b" , } { "c" :'d' , 'qwe' : "rty"} ,)`, []string{`{"a":"b"}`, `{"c":"d","qwe":"rty"}`})
	f(`rows({a="b" "c": -1.24/sd-f}  {})`, []string{`{"a":"b","c":"-1.24/sd-f"}`, `{}`})
}

func TestParseRows_Failure(t *testing.T) {
	f := func(s string) {
		t.Helper()

		lex := newLexer(s, 0)
		_, err := parseRows(lex)
		if err == nil {
			if !lex.isEnd() {
				return
			}
			t.Fatalf("expecting non-nil error")
		}
	}

	f("")
	f("foo")
	f("{")
	f("rows")
	f("rows(")
	f(`rows({foo`)
	f(`rows({"foo"`)
	f(`rows({"foo"}`)
	f(`rows({"foo":`)
	f(`rows({"foo":}`)
	f(`rows({"foo",bar})`)
	f(`rows({"foo":"bar",,})`)
	f(`rows({,})`)
	f(`rows({"foo":"bar")`)
	f(`rows({"foo":"bar")`)
	f(`rows({"foo":[]})`)
	f(`rows({"foo":1. 23})`)
	f(`rows({"foo":{}})`)
	f(`rows({"foo":nu ll})`)

	// non-empty tail
	f(`rows({}) foo`)
}
