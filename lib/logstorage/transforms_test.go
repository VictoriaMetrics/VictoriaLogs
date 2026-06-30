package logstorage

import (
	"reflect"
	"testing"
	"time"
)

func TestParseTransforms(t *testing.T) {
	f := func(s, want string) {
		t.Helper()

		tr, err := parseTransformsProgram(s)
		if err != nil {
			t.Fatalf("cannot parse transforms\nconfig:\n%s\nerror: %s", s, err)
		}
		got := tr.String()
		if got != want {
			t.Fatalf("unexpected parse result\nwant:\n%s\ngot:\n%s", want, got)
		}

		// Re-parse the String() output and ensure it round-trips to the same result.
		tr2, err := parseTransformsProgram(got)
		if err != nil {
			t.Fatalf("cannot reparse transformations\nconfig:\n%s\nerror: %s", got, err)
		}
		got2 := tr2.String()
		if got2 != want {
			t.Fatalf("transformations are not the same after round-trip parsing\nwant:\n%s\ngot:\n%s", want, got2)
		}
	}

	// A single pipe at the top level.
	from := "format foo as bar;"
	into := "format foo as bar;"
	f(from, into)

	// A single pipe at the top level.
	from = "unpack_json;"
	into = "unpack_json;"
	f(from, into)

	// Pipes chain in a line.
	from = "unpack_json | format foo as bar | unpack_words | pack_json;"
	into = "unpack_json | format foo as bar | unpack_words | pack_json;"
	f(from, into)

	// Pipes chain delimited by ';'.
	from = `unpack_json;
format foo as bar;
unpack_words | pack_json;
pack_json;`
	into = `unpack_json;
format foo as bar;
unpack_words | pack_json;
pack_json;`
	f(from, into)

	// Trailing '|' continues a single pipe chain across newlines.
	from = `unpack_json |
format foo as bar |
unpack_words | # foo bar
pack_json;`
	into = `unpack_json | format foo as bar | unpack_words | pack_json;`
	f(from, into)

	// Empty conditional block content.
	from = `if (foo:=bar) {} else if (bar:=foo) {} else {}`
	into = `if (foo:=bar) {} else if (bar:=foo) {} else {}`
	f(from, into)

	// Conditional block.
	from = `if (foo:=bar) { # foo bar
  unpack_json;
}`
	into = `if (foo:=bar) {
  unpack_json;
}`
	f(from, into)

	// Two adjacent 'if' blocks with no 'else' are independent chains.
	from = `if (foo:=bar or '()') {
  unpack_json;
}
if (a:=b or v:'(' or v:')') {
  pack_json;
}`
	into = `if (foo:=bar or "()") {
  unpack_json;
}
if (a:=b or v:"(" or v:")") {
  pack_json;
}`
	f(from, into)

	// A full 'if ... else if ... else' chain is one transformIf.
	from = `if (level:="") {
  format unknown as level;
} else if (level:=error) { # foo bar
  format critical as severity;
} else {
  format normal as severity;
}`
	into = `if (level:="") {
  format unknown as level;
} else if (level:=error) {
  format critical as severity;
} else {
  format normal as severity;
}`
	f(from, into)

	// An empty named block.
	from = `block foobar {}
do foobar;`
	into = `block foobar {}
do foobar;`
	f(from, into)

	// Named block with content.
	from = `block normalize {
  unpack_json;
  pack_json;
  if (foo:=bar) {
    unpack_words;
  }
}
do normalize;`
	into = `block normalize {
  unpack_json;
  pack_json;
  if (foo:=bar) {
    unpack_words;
  }
}
do normalize;`
	f(from, into)

	// Blocks declared at the end of the program.
	from = `do a;
do b;
do a;
do b;
block a {
  unpack_json;
}
block b {
  pack_json;
}`
	into = `block a {
  unpack_json;
}
block b {
  pack_json;
}
do a;
do b;
do a;
do b;`
	f(from, into)

	// 'send' at the top level of program.
	from = `send;`
	into = `send;`
	f(from, into)

	// 'send' inside a named block.
	from = `block enrich {
  if (foo:=bar) {
    send;
  }
  pack_json;
}
do enrich;`
	into = `block enrich {
  if (foo:=bar) {
    send;
  }
  pack_json;
}
do enrich;`
	f(from, into)

	// 'return' at the top level of program.
	from = `return;`
	into = `return;`
	f(from, into)

	// 'return' inside named block.
	from = `block foobar { return; }`
	into = `block foobar {
  return;
}
`
	f(from, into)

	// 'drop' at the top level of program.
	from = `drop;`
	into = `drop;`
	f(from, into)

	// 'drop' inside a named block.
	from = `block enrich {
  if (foo:=bar) {
    drop;
  }
  pack_json;
}
do enrich;`
	into = `block enrich {
  if (foo:=bar) {
    drop;
  }
  pack_json;
}
do enrich;`
	f(from, into)

	// Ensure every supported pipe parses without errors with semicolon at the end.
	from = `coalesce (user_id, username, email) default "anonymous" as user;
collapse_nums at _msg;
copy host as server;
cp host as server;
decolorize;
del foo, bar;
delete foo, bar;
drop_empty_fields;
extract 'email: <email>,' from foo;
extract_regexp "(?P<ip>([0-9]+[.]){3}[0-9]+)" from _msg;
eval x+y;
fields foo, bar;
format foo as bar;
hash foo;
json_array_len (foo);
keep foo, bar;
len foo;
math x+y;
mv a as b;
pack_json;
pack_logfmt;
rename a as b;
replace ("secret-password", "***") at _msg;
replace_regexp ("host-(.+?)-foo", "$1") at _msg;
rm foo;
sample 100;
set_stream_fields host, path;
split "," from _msg;
time_add 1h;
unpack_json;
unpack_logfmt;
unpack_syslog;
unpack_words;
unroll by foo;`
	into = `coalesce(user_id, username, email) default anonymous as user;
collapse_nums;
copy host as server;
copy host as server;
decolorize;
delete foo, bar;
delete foo, bar;
drop_empty_fields;
extract "email: <email>," from foo;
extract_regexp "(?P<ip>([0-9]+[.]){3}[0-9]+)";
math (x + y) as "x + y";
fields foo, bar;
format foo as bar;
hash(foo);
json_array_len(foo);
fields foo, bar;
len(foo);
math (x + y) as "x + y";
rename a as b;
pack_json;
pack_logfmt;
rename a as b;
replace ("secret-password", "***");
replace_regexp ("host-(.+?)-foo", "$1");
delete foo;
sample 100;
set_stream_fields host, path;
split ",";
time_add 1h;
unpack_json;
unpack_logfmt;
unpack_syslog;
unpack_words;
unroll by (foo);`
	f(from, into)
}

func TestParseTransformsFailure(t *testing.T) {
	f := func(s string) {
		t.Helper()
		tr, err := parseTransformsProgram(s)
		if err == nil {
			t.Fatalf("expected error when parsing\nconfig:\n%s\ngot:\n%s", s, tr.String())
		}
	}

	// Empty program is not valid.
	f("")

	// Line is not ended with ';'.
	f(`unpack_json`)

	// Valid LogsQL, but not transforms config.
	f("*;")

	// Curly braces at the top level.
	f(`{}`)
	f(`{
  format foo as bar;
  pack_json;
}`)
	f("delete x };")
	f("delete x {;")
	f("delete x | { delete y;")
	f("delete x } delete y;")

	// Unknown pipe name.
	f("foobar;")

	// Block declared with a reserved keyword as its name.
	f(`block if {}`)

	// Two blocks declared with the same name.
	f(`block a {}
block a {}`)

	// Block is declared with special symbols.
	f(`block abc^$ {}`)

	// Quoted block name.
	f(`block "abc" {}`)

	// Unterminated named block.
	f(`block`)
	f(`block foo;`)
	f(`block foo {`)
	f(`block foo {
  unpack_words;`)

	// Unterminated conditional block.
	f(`if (foo:=bar) {
  unpack_json; `)
	f(`if`)
	f(`if (`)
	f(`if (foo:=bar`)
	f(`if (foo:=bar)`)
	f(`if (foo:=bar) {`)
	f(`if (foo:=bar) {
unpack_words;`)

	// 'if' condition without parentheses.
	f(`if foo:=bar {
  unpack_json;
}`)

	// Execute undeclared block.
	f(`do foobar;`)

	// Recursive call.
	f(`block foobar {
  do foobar;
}`)

	// Nested recursive call.
	f(`block foo {
	do bar;
}
block bar {
	do foo;
}`)

	// Deeply nested recursive call.
	f(`block foo {
  do bar;
}
block bar {
  do baz;
}
block baz {
  do foo;
}`)

	// Conditional recursive call.
	f(`block foo {
  do bar;
}
block bar {
  if (abc) {
    do baz;
  }
}
block baz {
  if (def) {
    do foo;
  }
}`)

	// Time filter.
	f(`if (_time:5m) {}`)

	// Time filter in named blocks.
	f(`block foo {
  if (_time:offset 5m) {}
}
do foo;`)

	// Deeply nested time filter.
	f(`block foo { do bar; }
block bar { do baz; }
block baz { if (_time:[5m, 1m]) {} }
do foo;`)

	// Time filter in a pipe.
	f(`unpack_json if (foo:=bar and _time:7m);`)

	// Subquery.
	f(`if (foo:=bar and level:in(* | fields level)) {}`)

	// Subquery in a pipe.
	f(`unpack_json if (foo:=bar and level:in(* | fields level));`)
}

func TestTransformsProgram(t *testing.T) {
	f := func(program string, rows, rowsExpected [][]Field) {
		t.Helper()
		tr, err := parseTransformsProgram(program)
		if err != nil {
			t.Fatalf("cannot parse transforms config:\n%s\nerror: %s", program, err)
		}

		workersCount := 5
		ppTest := newTestPipeProcessor()
		pp := tr.newProcessor(workersCount, ppTest, ppTest)

		brw := newTestBlockResultWriter(workersCount, pp)
		for _, row := range rows {
			brw.writeRow(row)
		}
		if err := pp.flush(); err != nil {
			t.Fatal(err)
		}
		brw.flush()

		ppTest.expectRows(t, rowsExpected)
	}

	// Single 'unpack_json' pipe.
	f("unpack_json;", [][]Field{
		{{"_msg", `{"a":"b","foo":"bar","ping":"pong"}`}},
	}, [][]Field{
		{{"_msg", `{"a":"b","foo":"bar","ping":"pong"}`}, {"a", "b"}, {"foo", "bar"}, {"ping", "pong"}},
	})

	// Single 'copy' pipe.
	f(`copy _msg as copy_msg;`, [][]Field{
		{{"_msg", "hello"}},
	}, [][]Field{
		{{"_msg", "hello"}, {"copy_msg", "hello"}},
	})

	// Multiple pipes in a single line.
	f(`unpack_json | delete a;`, [][]Field{
		{{"_msg", `{"a":"b","foo":"bar"}`}},
	}, [][]Field{
		{{"_msg", `{"a":"b","foo":"bar"}`}, {"foo", "bar"}},
	})

	// Single 'if'.
	f(`if (level:=error) {
  unpack_json;
}`, [][]Field{
		{{"_msg", `{"a":"b","foo":"bar","ping":"pong"}`}},
		{{"_msg", `{"a":"b","foo":"bar","ping":"pong"}`}, {"level", "error"}},
	}, [][]Field{
		{{"_msg", `{"a":"b","foo":"bar","ping":"pong"}`}},
		{{"_msg", `{"a":"b","foo":"bar","ping":"pong"}`}, {"level", "error"}, {"a", "b"}, {"foo", "bar"}, {"ping", "pong"}},
	})

	// if-else chain.
	f(`if (level:=error) {
  format first as branch;
} else if (level:=warn) {
  format second as branch;
} else {
  format third as branch;
}`, [][]Field{
		{{"_msg", "1"}, {"level", "error"}},
		{{"_msg", "2"}, {"level", "warn"}},
		{{"_msg", "3"}, {"level", "info"}},
	}, [][]Field{
		{{"_msg", "1"}, {"level", "error"}, {"branch", "first"}},
		{{"_msg", "2"}, {"level", "warn"}, {"branch", "second"}},
		{{"_msg", "3"}, {"level", "info"}, {"branch", "third"}},
	})

	// Nested 'if'.
	f(`if (level:=error) {
  if (code:=500) {
    format crit as severity;
  }
}`, [][]Field{
		{{"_msg", "1"}, {"level", "error"}, {"code", "500"}},
		{{"_msg", "2"}, {"level", "error"}, {"code", "200"}},
		{{"_msg", "3"}, {"level", "info"}},
	}, [][]Field{
		{{"_msg", "1"}, {"level", "error"}, {"code", "500"}, {"severity", "crit"}},
		{{"_msg", "2"}, {"level", "error"}, {"code", "200"}},
		{{"_msg", "3"}, {"level", "info"}},
	})

	// 'return' at the top level passes rows through; transforms after it are dead code.
	f(`delete x;
return;
delete y;`, [][]Field{
		{{"_msg", "m"}, {"x", "abc"}, {"y", "def"}},
	}, [][]Field{
		{{"_msg", "m"}, {"y", "def"}},
	})

	// 'send' at the top level passes rows through; transforms after it are dead code.
	f(`delete x;
send;
delete y;`, [][]Field{
		{{"_msg", "m"}, {"x", "abc"}, {"y", "def"}},
	}, [][]Field{
		{{"_msg", "m"}, {"y", "def"}},
	})

	// Named block call.
	f(`block set_foo_bar {
  format bar as foo;
}
do set_foo_bar;`, [][]Field{
		{{"_msg", "abc"}},
	}, [][]Field{
		{{"_msg", "abc"}, {"foo", "bar"}},
	})

	// Early return inside a named block.
	f(`block normalize_errors {
  if (!level:=error) {
    return;
  }
  unpack_json;
}
do normalize_errors;
if (_msg:*) {
  format processed as status;
}`, [][]Field{
		{{"_msg", `{"parsed":false}`}, {"level", "info"}},
		{{"_msg", `{"parsed":true}`}, {"level", "error"}},
	}, [][]Field{
		{{"_msg", `{"parsed":false}`}, {"level", "info"}, {"status", "processed"}},
		{{"_msg", `{"parsed":true}`}, {"level", "error"}, {"parsed", "true"}, {"status", "processed"}},
	})

	// Nested named block call.
	f(`block c { delete x; }
block b { do c; }
block a { do b; }
do a;`, [][]Field{
		{{"_msg", "foobar"}, {"x", "abc"}},
	}, [][]Field{
		{{"_msg", "foobar"}},
	})

	// 'do' inside an 'if' branch.
	f(`block enrich {
  format yes as enriched;
}
if (level:=error) {
  do enrich;
}`, [][]Field{
		{{"_msg", "1"}, {"level", "error"}},
		{{"_msg", "2"}, {"level", "info"}},
	}, [][]Field{
		{{"_msg", "1"}, {"level", "error"}, {"enriched", "yes"}},
		{{"_msg", "2"}, {"level", "info"}},
	})

	// 'do' with no 'return'.
	f(`block enrich {
  format yes as enriched;
}
do enrich;
format done as status;`, [][]Field{
		{{"_msg", "x"}},
	}, [][]Field{
		{{"_msg", "x"}, {"enriched", "yes"}, {"status", "done"}},
	})

	// 'return' inside a 'do'.
	f(`block maybe_skip {
  if ("skip":=yes) {
    return;
  }
  format bar as foo;
}
do maybe_skip;
format finished as status;`, [][]Field{
		{{"_msg", "a"}, {"skip", "yes"}},
		{{"_msg", "b"}, {"skip", "no"}},
	}, [][]Field{
		{{"_msg", "a"}, {"skip", "yes"}, {"status", "finished"}},
		{{"_msg", "b"}, {"skip", "no"}, {"foo", "bar"}, {"status", "finished"}},
	})

	// Drop all the logs.
	f(`drop;`, [][]Field{
		{{"_msg", "a"}, {"skip", "yes"}},
		{{"_msg", "b"}, {"skip", "no"}},
	}, [][]Field{})

	// Drop based on condition.
	f(`if (level:=debug) { drop; }`, [][]Field{
		{{"_msg", "a"}, {"level", "debug"}},
		{{"_msg", "b"}, {"level", "info"}},
		{{"_msg", "c"}},
	}, [][]Field{
		{{"_msg", "b"}, {"level", "info"}},
		{{"_msg", "c"}},
	})

	// Drop based on condition inside a named block.
	f(`block foo {
  do bar;
}
block bar {
  do baz;
}
block baz {
  if (!drop:=true) {
    return;
  }
  drop;
}
do baz;`, [][]Field{
		{{"_msg", "a"}, {"drop", "true"}},
		{{"_msg", "b"}, {"drop", "false"}},
		{{"_msg", "c"}},
	}, [][]Field{
		{{"_msg", "b"}, {"drop", "false"}},
		{{"_msg", "c"}},
	})
}

func TestTransformsProgramSetTime(t *testing.T) {
	f := func(program string, timestamp, timestampExpected int64) {
		t.Helper()

		var timestampsGot []int64
		storeTimestamp := func(lr *LogRows) {
			lr.ForEachRow(func(_ uint64, r *InsertRow) {
				timestampsGot = append(timestampsGot, r.Timestamp)
			})
		}

		lr := GetLogRows(nil, nil, nil, nil, "")
		defer PutLogRows(lr)
		fields := []Field{
			{"_msg", "foobar"},
		}
		lr.MustAdd(TenantID{}, timestamp, fields, -1)

		prog, err := ParseTransformsProgram(program)
		if err != nil {
			t.Fatal(err)
		}
		tr := prog.NewTransformer(storeTimestamp)
		tr.Transform(lr)

		if len(timestampsGot) != 1 {
			t.Fatalf("expected 1 timestamp, got %d", len(timestampsGot))
		}
		tsGot := timestampsGot[0]
		if tsGot != timestampExpected {
			t.Fatalf("unexpected timestamp; got %v; want %v", tsGot, timestampExpected)
		}
	}

	// Modify original _time field.
	f(`time_add 0h;`, 1234, 1234)
	f(`time_add 1h;`, 1, 1+nsecsPerHour)
	f(`time_add 2h;`, 1234, 1234+2*nsecsPerHour)

	// Change the type of the '_time' field to string and then modify it.
	f(`format 1970-01-01T12:00:00Z as _time | time_add 1h;`, 1234, 13*nsecsPerHour)

	// Drop the _time field and then modify it.
	f(`delete _time | time_add 1h;`, 1234, nsecsPerHour)
}

// TestTransformsProgramSetTimeWithBlockSource ensures blockResult with brSrc correctly restores modified _time column.
// See https://github.com/VictoriaMetrics/VictoriaLogs/pull/1508#discussion_r3523272574
func TestTransformsProgramSetTimeWithBlockSource(t *testing.T) {
	var timestampsGot []int64
	storeTimestamp := func(lr *LogRows) {
		lr.ForEachRow(func(_ uint64, r *InsertRow) {
			timestampsGot = append(timestampsGot, r.Timestamp)
		})
	}

	lr := GetLogRows(nil, nil, nil, nil, "")
	defer PutLogRows(lr)
	lr.MustAdd(TenantID{}, 1234, []Field{{"_msg", "match"}}, -1)
	lr.MustAdd(TenantID{}, 1234, []Field{{"_msg", "does-not-match"}}, -1)

	prog, err := ParseTransformsProgram(`if (="match") { time_add 1h; }`)
	if err != nil {
		t.Fatal(err)
	}
	tr := prog.NewTransformer(storeTimestamp)
	tr.Transform(lr)

	timestampsExpected := []int64{1234 + nsecsPerHour, 1234}
	if len(timestampsGot) != len(timestampsExpected) {
		t.Fatalf("expected %d timestamp, got %d", len(timestampsExpected), len(timestampsGot))
	}
	if !reflect.DeepEqual(timestampsGot, timestampsExpected) {
		t.Fatalf("unexpected timestamps\ngot:\n%v\nwant:\n%v", timestampsGot, timestampsExpected)
	}
}

func TestTransformsProgramSetTenantID(t *testing.T) {
	defaultTenantID := TenantID{
		AccountID: 1234,
		ProjectID: 5678,
	}
	f := func(program string, row []Field, tenantsExpected []TenantID) {
		t.Helper()

		var tenantsGot []TenantID
		storeTenant := func(lr *LogRows) {
			lr.ForEachRow(func(_ uint64, r *InsertRow) {
				tenantsGot = append(tenantsGot, r.TenantID)
			})
		}

		lr := GetLogRows(nil, nil, nil, nil, "")
		defer PutLogRows(lr)
		lr.MustAdd(defaultTenantID, time.Now().UnixNano(), row, -1)

		prog, err := ParseTransformsProgram(program)
		if err != nil {
			t.Fatal(err)
		}
		tr := prog.NewTransformer(storeTenant)
		tr.Transform(lr)

		if !reflect.DeepEqual(tenantsGot, tenantsExpected) {
			t.Fatalf("unexpected tenant are set\ngot\n%v\nwant\n%v", tenantsGot, tenantsExpected)
		}
	}

	// vl_account_id and vl_project_id fields are not touched.
	f(`unpack_json;`, []Field{}, []TenantID{defaultTenantID})

	// Ignore vl_account_id and vl_project_id fields when they come from log rows.
	f(`unpack_json;`, []Field{
		{"_msg", "foobar"},
		{"vl_account_id", "42"},
		{"vl_project_id", "42"},
	}, []TenantID{defaultTenantID})

	// vl_account_id and vl_project_id fields come from the unpack_json pipe.
	f(`unpack_json;`, []Field{
		{"_msg", `{"foo":"bar","vl_account_id":11,"vl_project_id":"12"}`},
	}, []TenantID{{11, 12}})

	// vl_account_id and vl_project_id fields are set via the format pipe.
	f(`format 3 as vl_account_id | format 4 as vl_project_id;`, []Field{
		{"_msg", `foobar`},
		{"vl_account_id", "1"},
		{"vl_project_id", "2"},
	}, []TenantID{{3, 4}})

	// vl_account_id and vl_project_id fields come from the unroll pipe.
	f(`unroll by (_msg) | unpack_json;`, []Field{
		{"_msg", `[{"foo":"bar","vl_account_id":1,"vl_project_id":2}, {"bar":"baz","vl_account_id":3,"vl_project_id":4}, {"ping":"pong"}]`},
		{"vl_account_id", "3"},
	}, []TenantID{{1, 2}, {3, 4}, defaultTenantID})
}
