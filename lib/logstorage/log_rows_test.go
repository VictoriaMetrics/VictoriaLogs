package logstorage

import (
	"reflect"
	"slices"
	"testing"
)

func TestLogRows_WildcardIgnoreFields(t *testing.T) {
	type opts struct {
		rows []string

		streamFields []string
		ignoreFields []string
		extraFields  []Field

		resultExpected []string
	}

	f := func(o opts) {
		t.Helper()

		lr := GetLogRows(o.streamFields, o.ignoreFields, nil, o.extraFields, "foobar")
		defer PutLogRows(lr)

		tid := TenantID{
			AccountID: 123,
			ProjectID: 456,
		}

		p := GetJSONParser()
		defer PutJSONParser(p)
		for i, r := range o.rows {
			if err := p.ParseLogMessage([]byte(r), nil, ""); err != nil {
				t.Fatalf("unexpected error when parsing %q: %s", r, err)
			}
			timestamp := int64(i)*1_000 + 1
			lr.mustAdd(tid, timestamp, p.Fields)
		}

		var result []string
		for i := range o.rows {
			s := lr.GetRowString(i)
			result = append(result, s)
		}
		if !reflect.DeepEqual(result, o.resultExpected) {
			t.Fatalf("unexpected result\ngot\n%v\nwant\n%v", result, o.resultExpected)
		}
	}

	o := opts{
		rows: []string{
			`{"foo.a":"bar","foo.b":"abc","z":"abc","x":"y","_msg":"aaa","foobar":"b"}`,
			`{"_msg":"x"}`,
		},
		streamFields: []string{"foo.a", "foo.b", "foobar"},
		ignoreFields: []string{"foo.*", "x"},
		extraFields: []Field{
			{
				Name:  "foo.a",
				Value: "1234",
			},
		},
		resultExpected: []string{
			`{"_msg":"aaa","_stream":"{foo.a=\"1234\",foobar=\"b\"}","_time":"1970-01-01T00:00:00.000000001Z","foo.a":"1234","foobar":"b","z":"abc"}`,
			`{"_msg":"x","_stream":"{foo.a=\"1234\"}","_time":"1970-01-01T00:00:00.000001001Z","foo.a":"1234"}`,
		},
	}
	f(o)
}

func TestLogRows_StreamFieldsOverride(t *testing.T) {
	type opts struct {
		rows []string

		streamFieldsLen int
		ignoreFields    []string

		resultExpected []string
	}

	f := func(o opts) {
		t.Helper()

		lr := GetLogRows(nil, o.ignoreFields, nil, nil, "foobar")
		defer PutLogRows(lr)

		tid := TenantID{
			AccountID: 123,
			ProjectID: 456,
		}

		p := GetJSONParser()
		defer PutJSONParser(p)
		for i, r := range o.rows {
			if err := p.ParseLogMessage([]byte(r), nil, ""); err != nil {
				t.Fatalf("unexpected error when parsing %q: %s", r, err)
			}
			timestamp := int64(i)*1_000 + 1
			lr.MustAdd(tid, timestamp, p.Fields, o.streamFieldsLen)
		}

		var result []string
		for i := range o.rows {
			s := lr.GetRowString(i)
			result = append(result, s)
		}
		if !reflect.DeepEqual(result, o.resultExpected) {
			t.Fatalf("unexpected result\ngot\n%v\nwant\n%v", result, o.resultExpected)
		}
	}

	var o opts

	o = opts{
		rows: []string{
			`{"xyz":"123","foo":"bar","_msg":"abc"}`,
			`{"xyz":"bar","_msg":"abc"}`,
			`{"xyz":"123","_msg":"abc"}`,
		},
		streamFieldsLen: 1,
		resultExpected: []string{
			`{"_msg":"abc","_stream":"{xyz=\"123\"}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar","xyz":"123"}`,
			`{"_msg":"abc","_stream":"{xyz=\"bar\"}","_time":"1970-01-01T00:00:00.000001001Z","xyz":"bar"}`,
			`{"_msg":"abc","_stream":"{xyz=\"123\"}","_time":"1970-01-01T00:00:00.000002001Z","xyz":"123"}`,
		},
	}
	f(o)

	o = opts{
		rows: []string{
			`{"foo":"bar","_msg":"abc"}`,
			`{"xyz":"bar","_msg":"abc"}`,
			`{"xyz":"123","_msg":"abc"}`,
		},
		streamFieldsLen: 0,
		ignoreFields:    []string{"xyz", "qwert"},
		resultExpected: []string{
			`{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar"}`,
			`{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z"}`,
			`{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z"}`,
		},
	}
	f(o)

	// normalize _stream
	o = opts{
		rows: []string{
			`{"_msg":"abc","_stream":"{service=\"foo\"}","service":"foo"}`, // remains the same
			`{"_msg":"abc","_stream":"{service=\"bar\"}","service":"foo"}`, // 'service' tag is outdated
			`{"_msg":"abc","_stream":"{service=\"baz\"}"}`,                 // 'service' field is missing
		},
		streamFieldsLen: -1,
		resultExpected: []string{
			`{"_msg":"abc","_stream":"{service=\"foo\"}","_time":"1970-01-01T00:00:00.000000001Z","service":"foo"}`,
			`{"_msg":"abc","_stream":"{service=\"foo\"}","_time":"1970-01-01T00:00:00.000001001Z","service":"foo"}`,
			`{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z"}`,
		},
	}
	f(o)
}

func TestLogRows_DeduplicatedFields(t *testing.T) {
	f := func(row, extraFields, resultExpected string) {
		t.Helper()

		rowParser := getLogfmtParser()
		defer putLogfmtParser(rowParser)
		rowParser.parse(row)

		extraFieldsParser := getLogfmtParser()
		defer putLogfmtParser(extraFieldsParser)
		extraFieldsParser.parse(extraFields)

		lr := GetLogRows(nil, nil, nil, extraFieldsParser.fields, "")
		defer PutLogRows(lr)
		lr.MustAdd(TenantID{}, 0, rowParser.fields, -1)

		var gotRows []string
		lr.ForEachRow(func(_ uint64, r *InsertRow) {
			v := MarshalFieldsToLogfmt(nil, r.Fields)
			gotRows = append(gotRows, string(v))
		})
		if len(gotRows) != 1 {
			t.Fatalf("unexpected rows count in result: %s", gotRows)
		}
		got := gotRows[0]

		if got != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", got, resultExpected)
		}
	}

	// No duplicates
	f("a=1 b=2", "", "a=1 b=2")

	// Unsorted fields with duplicates
	f("b=1 a=2 b=3", "", "a=2 b=1")
	f("b=1 b=3 a=2", "", "a=2 b=1")

	// Sorted fields with duplicates
	f("a=1 b=2 b=3", "", "a=1 b=2")

	// The same field repeated more than two times
	f("a=1 a=2 a=3", "", "a=1")
	f("a=1 a=2 a=3 a=4", "", "a=1")
	f("a=1 a=2 a=3 a=5", "", "a=1")
	f("c=0 a=1 a=2 a=3 a=5 b=6", "", "a=1 b=6 c=0")

	// Multiple duplicated unsorted fields
	f("a=1 a=2 b=3 b=4 c=5 c=6", "", "a=1 b=3 c=5")

	// Multiple duplicated sorted fields
	f("c=1 a=1 b=1 a=2 c=2 b=2", "", "a=1 b=1 c=1")

	// Case-sensitive field names
	f("A=1 a=2", "", "A=1 a=2")

	// Ignore empty fields with empty values
	f("a= a=2", "", "a=2")

	// Duplicate special field
	f("a=1 _msg=foo b=2 _msg=bar", "", "_msg=foo a=1 b=2")

	// extraFields contain duplicates
	f("a=1", "x=1 x=2", "a=1 x=1")

	// extraFields are unsorted and contain duplicates
	f("a=1", "b=2 c=3 b=4", "a=1 b=2 c=3")

	// extraFields intersect with row fields
	f("a=1", "a=2", "a=2")

	// Multiple extraFields intersect with row fields
	f("a=1 a=2", "a=3 a=4", "a=3")

	// Row fields and extraFields must be merged in the sorted order
	f("z=1", "a=2", "a=2 z=1")
	f("b=1 z=2", "c=3", "b=1 c=3 z=2")

	// The first value must win for many duplicated unsorted fields
	f("i=1 h=1 g=1 f=1 e=1 d=1 c=1 b=1 a=1 i=2 h=2 g=2 f=2 e=2 d=2 c=2 b=2 a=2", "",
		"a=1 b=1 c=1 d=1 e=1 f=1 g=1 h=1 i=1")
}

func TestLogRows_DuplicatedStreamFieldsOverride(t *testing.T) {
	f := func(row, resultExpected string) {
		t.Helper()

		rowParser := getLogfmtParser()
		defer putLogfmtParser(rowParser)
		rowParser.parse(row)

		lr := GetLogRows(nil, nil, nil, nil, "")
		defer PutLogRows(lr)
		lr.MustAdd(TenantID{}, 0, rowParser.fields, -1)

		var gotRows []string
		lr.ForEachRow(func(_ uint64, r *InsertRow) {
			fields := slices.Clone(r.Fields)

			stream := getStreamTagsString(r.StreamTagsCanonical)
			fields = append(fields, Field{"_stream", stream})

			sortFieldsByName(fields)

			v := MarshalFieldsToLogfmt(nil, fields)
			gotRows = append(gotRows, string(v))
		})
		if len(gotRows) != 1 {
			t.Fatalf("unexpected rows count in result: %s", gotRows)
		}
		got := gotRows[0]

		if got != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", got, resultExpected)
		}
	}

	// Single _stream field
	f(`_stream={host="a"} host=a`, `_stream="{host=\"a\"}" host=a`)

	// Multiple _stream fields
	f(`_stream="{host=\"a\"}" _stream="{host=\"b\"}" host=a`, `_stream="{host=\"a\"}" host=a`)

	// Multiple _stream fields, the first stream must be normalized
	f(`_stream="{host=\"a\"}" _stream="{foobar=\"b\"}" host=c`, `_stream="{host=\"c\"}" host=c`)

	// Multiple _stream fields, the first stream is empty
	f(`_stream={} _stream={host="a"} host=a`, `_stream={} host=a`)

	// Three unsorted _stream fields
	f(`a=b _stream="{host=\"a\"}" b=c _stream="{host=\"b\"}" c=d _stream="{host=\"c\"}" e=f host=a`, `_stream="{host=\"a\"}" a=b b=c c=d e=f host=a`)

	// _stream has duplicated sorted tags
	f(`_stream="{a=\"1\",a=\"2\"}" a=1 a=2"`, `_stream="{a=\"1\"}" a=1`)

	// _stream has duplicated unsorted tags
	f(`_stream="{b=\"3\",a=\"1\",a=\"2\"}" a=1 a=2 b=3`, `_stream="{a=\"1\",b=\"3\"}" a=1 b=3`)

	// Ignore _stream_id
	f(`_stream="{a=\"1\"}" a=1 _stream_id=foobar _stream_id=baz`, `_stream="{a=\"1\"}" a=1`)
}

func TestLogRows_DuplicatedStreamFields(t *testing.T) {
	streamFields := []string{"app", "host"}
	f := func(row, resultExpected string) {
		t.Helper()

		rowParser := getLogfmtParser()
		defer putLogfmtParser(rowParser)
		rowParser.parse(row)

		lr := GetLogRows(streamFields, nil, nil, nil, "")
		defer PutLogRows(lr)
		lr.MustAdd(TenantID{}, 0, rowParser.fields, -1)

		var gotRows []string
		lr.ForEachRow(func(_ uint64, r *InsertRow) {
			fields := slices.Clone(r.Fields)

			stream := getStreamTagsString(r.StreamTagsCanonical)
			fields = append(fields, Field{"_stream", stream})

			sortFieldsByName(fields)

			v := MarshalFieldsToLogfmt(nil, fields)
			gotRows = append(gotRows, string(v))
		})
		if len(gotRows) != 1 {
			t.Fatalf("unexpected rows count in result: %s", gotRows)
		}
		got := gotRows[0]

		if got != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", got, resultExpected)
		}
	}

	// No duplicates
	f("app=foo host=h1 a=1", `_stream="{app=\"foo\",host=\"h1\"}" a=1 app=foo host=h1`)

	// No stream fields in the row
	f("a=1 b=2", `_stream={} a=1 b=2`)

	f("a=1 app=foo b=2 app=bar", `_stream="{app=\"foo\"}" a=1 app=foo b=2`)

	// Sorted duplicated stream field
	f("app=foo app=bar b=2", `_stream="{app=\"foo\"}" app=foo b=2`)

	// The same stream field repeated more than two times
	f("app=1 app=2 app=3 b=4", `_stream="{app=\"1\"}" app=1 b=4`)

	// Multiple duplicated unsorted stream fields
	f("host=h2 app=foo host=h1 app=bar", `_stream="{app=\"foo\",host=\"h2\"}" app=foo host=h2`)

	// Duplicated stream field with the empty value
	f("app= app=foo b=1", `_stream="{app=\"foo\"}" app=foo b=1`)

	// Stream field with the empty value only
	f("app= b=1", `_stream={} b=1`)

	// Case-sensitive stream field names
	f("APP=1 app=2", `APP=1 _stream="{app=\"2\"}" app=2`)
}

func TestLogRows_DuplicatedStreamFieldsLen(t *testing.T) {
	f := func(row string, streamFieldsLen int, resultExpected string) {
		t.Helper()

		rowParser := getLogfmtParser()
		defer putLogfmtParser(rowParser)
		rowParser.parse(row)

		lr := GetLogRows(nil, nil, nil, nil, "")
		defer PutLogRows(lr)
		lr.MustAdd(TenantID{}, 0, rowParser.fields, streamFieldsLen)

		var gotRows []string
		lr.ForEachRow(func(_ uint64, r *InsertRow) {
			fields := slices.Clone(r.Fields)
			fields = append(fields, Field{"_stream", getStreamTagsString(r.StreamTagsCanonical)})
			sortFieldsByName(fields)
			gotRows = append(gotRows, string(MarshalFieldsToLogfmt(nil, fields)))
		})
		if len(gotRows) != 1 {
			t.Fatalf("unexpected rows count in result: %s", gotRows)
		}
		if gotRows[0] != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", gotRows[0], resultExpected)
		}
	}

	// No duplicates
	f("app=foo host=h1 a=1", 2, `_stream="{app=\"foo\",host=\"h1\"}" a=1 app=foo host=h1`)

	// No stream fields
	f("a=1 b=2", 0, `_stream={} a=1 b=2`)

	// Duplicated field inside the prefix
	f("app=1 app=2 b=3", 2, `_stream="{app=\"1\"}" app=1 b=3`)

	// Duplicate outside the prefix
	f("app=1 b=2 app=3", 1, `_stream="{app=\"1\"}" app=1 b=2`)

	// Empty value inside the prefix, non-empty duplicate outside
	f("app= b=1 app=bar", 1, `_stream="{app=\"bar\"}" app=bar b=1`)

	// Stream field with the empty value only
	f("app= b=1", 1, `_stream={} b=1`)
}

func TestLogRows_DefaultMsgValue(t *testing.T) {
	type opts struct {
		rows []string

		streamFields     []string
		ignoreFields     []string
		decolorizeFields []string
		extraFields      []Field
		defaultMsgValue  string

		resultExpected []string
	}

	f := func(o opts) {
		t.Helper()

		lr := GetLogRows(o.streamFields, o.ignoreFields, o.decolorizeFields, o.extraFields, o.defaultMsgValue)
		defer PutLogRows(lr)

		tid := TenantID{
			AccountID: 123,
			ProjectID: 456,
		}

		p := GetJSONParser()
		defer PutJSONParser(p)
		for i, r := range o.rows {
			if err := p.ParseLogMessage([]byte(r), nil, ""); err != nil {
				t.Fatalf("unexpected error when parsing %q: %s", r, err)
			}
			timestamp := int64(i)*1_000 + 1
			lr.mustAdd(tid, timestamp, p.Fields)
		}

		var result []string
		for i := range o.rows {
			s := lr.GetRowString(i)
			result = append(result, s)
		}
		if !reflect.DeepEqual(result, o.resultExpected) {
			t.Fatalf("unexpected result\ngot\n%v\nwant\n%v", result, o.resultExpected)
		}
	}

	var o opts

	f(o)

	// default options
	o = opts{
		rows: []string{
			`{"foo":"bar"}`,
			`{}`,
			`{"foo":"bar","a":"b"}`,
		},
		resultExpected: []string{
			`{"_stream":"{}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar"}`,
			`{"_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z"}`,
			`{"_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z","a":"b","foo":"bar"}`,
		},
	}
	f(o)

	// stream fields
	o = opts{
		rows: []string{
			`{"x":"y","foo":"bar"}`,
			`{"x":"y","foo":"bar","abc":"de"}`,
			`{}`,
		},
		streamFields: []string{"foo", "abc"},
		resultExpected: []string{
			`{"_stream":"{foo=\"bar\"}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar","x":"y"}`,
			`{"_stream":"{abc=\"de\",foo=\"bar\"}","_time":"1970-01-01T00:00:00.000001001Z","abc":"de","foo":"bar","x":"y"}`,
			`{"_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z"}`,
		},
	}
	f(o)

	// ignore fields
	o = opts{
		rows: []string{
			`{"x":"y","foo":"bar"}`,
			`{"x":"y"}`,
			`{}`,
		},
		streamFields: []string{"foo", "abc", "x"},
		ignoreFields: []string{"foo"},
		resultExpected: []string{
			`{"_stream":"{x=\"y\"}","_time":"1970-01-01T00:00:00.000000001Z","x":"y"}`,
			`{"_stream":"{x=\"y\"}","_time":"1970-01-01T00:00:00.000001001Z","x":"y"}`,
			`{"_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z"}`,
		},
	}
	f(o)

	// extra fields
	o = opts{
		rows: []string{
			`{"x":"y","foo":"bar"}`,
			`{}`,
		},
		streamFields: []string{"foo", "abc", "x"},
		ignoreFields: []string{"foo"},
		extraFields: []Field{
			{
				Name:  "foo",
				Value: "test",
			},
			{
				Name:  "abc",
				Value: "1234",
			},
		},
		resultExpected: []string{
			`{"_stream":"{abc=\"1234\",foo=\"test\",x=\"y\"}","_time":"1970-01-01T00:00:00.000000001Z","abc":"1234","foo":"test","x":"y"}`,
			`{"_stream":"{abc=\"1234\",foo=\"test\"}","_time":"1970-01-01T00:00:00.000001001Z","abc":"1234","foo":"test"}`,
		},
	}
	f(o)

	// default _msg value
	o = opts{
		rows: []string{
			`{"x":"y","foo":"bar"}`,
			`{"_msg":"ppp"}`,
			`{"abc":"ppp"}`,
		},
		streamFields:    []string{"abc", "x"},
		defaultMsgValue: "qwert",
		resultExpected: []string{
			`{"_msg":"qwert","_stream":"{x=\"y\"}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar","x":"y"}`,
			`{"_msg":"ppp","_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z"}`,
			`{"_msg":"qwert","_stream":"{abc=\"ppp\"}","_time":"1970-01-01T00:00:00.000002001Z","abc":"ppp"}`,
		},
	}
	f(o)

	// decolorize with _msg field
	o = opts{
		rows: []string{
			`{"_msg":"` + "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H" + `","abc":"de","bar":"baz"}`,
			`{"":"` + "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H" + `","abc":"de","bar":"baz"}`,
			`{"_msg":"abc","bar":"` + "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H" + `"}`,
			`{"_msg":"abc","bar":"baz","x":"` + "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H" + `"}`,
		},
		decolorizeFields: []string{"_msg", "bar"},
		resultExpected: []string{
			`{"_msg":"fooERROR bar","_stream":"{}","_time":"1970-01-01T00:00:00.000000001Z","abc":"de","bar":"baz"}`,
			`{"_msg":"fooERROR bar","_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z","abc":"de","bar":"baz"}`,
			`{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z","bar":"fooERROR bar"}`,
			`{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000003001Z","bar":"baz","x":"\u001b[mfoo\u001b[1;31mERROR bar\u001b[10;5H"}`,
		},
	}
	f(o)

	// decolorize with "" field name (canonical _msg field)
	o = opts{
		rows: []string{
			`{"_msg":"` + "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H" + `","abc":"de","bar":"baz"}`,
			`{"":"` + "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H" + `","abc":"de","bar":"baz"}`,
			`{"_msg":"abc","bar":"` + "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H" + `"}`,
			`{"_msg":"abc","bar":"baz","x":"` + "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H" + `"}`,
		},
		decolorizeFields: []string{"", "bar"},
		resultExpected: []string{
			`{"_msg":"fooERROR bar","_stream":"{}","_time":"1970-01-01T00:00:00.000000001Z","abc":"de","bar":"baz"}`,
			`{"_msg":"fooERROR bar","_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z","abc":"de","bar":"baz"}`,
			`{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z","bar":"fooERROR bar"}`,
			`{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000003001Z","bar":"baz","x":"\u001b[mfoo\u001b[1;31mERROR bar\u001b[10;5H"}`,
		},
	}
	f(o)
}

func TestInsertRow_MarshalUnmarshal(t *testing.T) {
	r := &InsertRow{
		TenantID: TenantID{
			AccountID: 123,
			ProjectID: 456,
		},
		StreamTagsCanonical: "foobar",
		Timestamp:           789,
		Fields: []Field{
			{
				Name:  "x",
				Value: "y",
			},
			{
				Name:  "qwe",
				Value: "rty",
			},
		},
	}
	data := r.Marshal(nil)

	var r2 InsertRow
	tail, err := r2.UnmarshalInplace(data)
	if err != nil {
		t.Fatalf("unexpected error when unmarshaling InsertRow: %s", err)
	}
	if len(tail) > 0 {
		t.Fatalf("unexpected tail left after unmarshaling InsertRow; len(tail)=%d; tail=%X", len(tail), tail)
	}
}

func TestInsertRow_MarshalJSON(t *testing.T) {
	f := func(ts int64, fields []Field, expected string) {
		t.Helper()

		r := InsertRow{
			Timestamp: ts,
			Fields:    fields,
		}
		got := r.AppendJSON(nil)

		if string(got) != expected {
			t.Fatalf("unexpected result\ngot\n%q\nwant\n%q", got, expected)
		}
	}

	// empty fields
	f(0, nil, `{"_time":"1970-01-01T00:00:00Z"}`)

	// non-empty fields
	f(123456789, []Field{
		{
			Name:  "x",
			Value: "y",
		},
		{
			Name:  "qwe",
			Value: "rty",
		},
	}, `{"_time":"1970-01-01T00:00:00.123456789Z","x":"y","qwe":"rty"}`)

	// empty values
	f(123456789, []Field{
		{
			Name:  "x",
			Value: "",
		},
		{
			Name:  "qwe",
			Value: "",
		},
	}, `{"_time":"1970-01-01T00:00:00.123456789Z"}`)

	// empty field name
	f(123456789, []Field{
		{
			Name:  "",
			Value: "y",
		},
	}, `{"_time":"1970-01-01T00:00:00.123456789Z","_msg":"y"}`)

	// escape quotes
	f(123456789, []Field{
		{
			Name:  "x",
			Value: `"y"`,
		},
	}, `{"_time":"1970-01-01T00:00:00.123456789Z","x":"\"y\""}`)
}
