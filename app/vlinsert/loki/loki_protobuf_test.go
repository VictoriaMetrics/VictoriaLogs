package loki

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type testLogMessageProcessor struct {
	pr PushRequest
}

func (tlp *testLogMessageProcessor) AddRow(timestamp int64, fields, streamFields []logstorage.Field) {
	if streamFields != nil {
		panic(fmt.Errorf("unexpected non-nil streamFields: %v", streamFields))
	}
	msg := ""
	for _, f := range fields {
		if f.Name == "_msg" {
			msg = f.Value
		}
	}
	var a []string
	for _, f := range fields {
		if f.Name == "_msg" {
			continue
		}
		item := fmt.Sprintf("%s=%q", f.Name, f.Value)
		a = append(a, item)
	}
	labels := "{" + strings.Join(a, ", ") + "}"
	tlp.pr.Streams = append(tlp.pr.Streams, Stream{
		Labels: labels,
		Entries: []Entry{
			{
				Timestamp: time.Unix(0, timestamp),
				Line:      strings.Clone(msg),
			},
		},
	})
}

func (tlp *testLogMessageProcessor) MustClose() {
}

func TestParseProtobufRequest_Success(t *testing.T) {
	f := func(s string, timestampsExpected []int64, resultExpected string) {
		t.Helper()

		tlp := &testLogMessageProcessor{}
		if err := parseJSONRequest([]byte(s), tlp, nil, false, false); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if len(tlp.pr.Streams) != len(timestampsExpected) {
			t.Fatalf("unexpected number of streams; got %d; want %d", len(tlp.pr.Streams), len(timestampsExpected))
		}

		data := tlp.pr.MarshalProtobuf(nil)

		tlp2 := &insertutil.TestLogMessageProcessor{}
		if err := parseProtobufRequest(data, tlp2, nil, false, false); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := tlp2.Verify(timestampsExpected, resultExpected); err != nil {
			t.Fatal(err)
		}
	}

	// Empty streams
	f(`{"streams":[]}`, nil, ``)
	f(`{"streams":[{"values":[]}]}`, nil, ``)
	f(`{"streams":[{"stream":{},"values":[]}]}`, nil, ``)
	f(`{"streams":[{"stream":{"foo":"bar"},"values":[]}]}`, nil, ``)

	// Empty stream labels
	f(`{"streams":[{"values":[["1577836800000000001", "foo bar"]]}]}`, []int64{1577836800000000001}, `{"_msg":"foo bar"}`)
	f(`{"streams":[{"stream":{},"values":[["1577836800000000001", "foo bar"]]}]}`, []int64{1577836800000000001}, `{"_msg":"foo bar"}`)

	// Non-empty stream labels
	f(`{"streams":[{"stream":{
	"label1": "value1",
	"label2": "value2"
},"values":[
	["1577836800000000001", "foo bar"],
	["1477836900005000002", "abc"],
	["147.78369e9", "foobar"]
]}]}`, []int64{1577836800000000001, 1477836900005000002, 147783690000000000}, `{"label1":"value1","label2":"value2","_msg":"foo bar"}
{"label1":"value1","label2":"value2","_msg":"abc"}
{"label1":"value1","label2":"value2","_msg":"foobar"}`)

	// Multiple streams
	f(`{
	"streams": [
		{
			"stream": {
				"foo": "bar",
				"a": "b"
			},
			"values": [
				["1577836800000000001", "foo bar"],
				["1577836900005000002", "abc"]
			]
		},
		{
			"stream": {
				"x": "y"
			},
			"values": [
				["1877836900005000002", "yx"]
			]
		}
	]
}`, []int64{1577836800000000001, 1577836900005000002, 1877836900005000002}, `{"foo":"bar","a":"b","_msg":"foo bar"}
{"foo":"bar","a":"b","_msg":"abc"}
{"x":"y","_msg":"yx"}`)
}

func TestParseProtobufRequest_ParseMessage(t *testing.T) {
	f := func(s string, msgFields []string, timestampsExpected []int64, resultExpected string) {
		t.Helper()

		tlp := &testLogMessageProcessor{}
		if err := parseJSONRequest([]byte(s), tlp, nil, false, false); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if len(tlp.pr.Streams) != len(timestampsExpected) {
			t.Fatalf("unexpected number of streams; got %d; want %d", len(tlp.pr.Streams), len(timestampsExpected))
		}

		data := tlp.pr.MarshalProtobuf(nil)

		tlp2 := &insertutil.TestLogMessageProcessor{}
		if err := parseProtobufRequest(data, tlp2, msgFields, false, true); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := tlp2.Verify(timestampsExpected, resultExpected); err != nil {
			t.Fatal(err)
		}
	}

	f(`{
	"streams": [
		{
			"stream": {
				"foo": "bar",
				"a": "b"
			},
			"values": [
				["1577836800000000001", "{\"user_id\":\"123\"}"],
				["1577836900005000002", "abc", {"trace_id":"pqw"}],
				["1577836900005000003", "{def}"]
			]
		},
		{
			"stream": {
				"x": "y"
			},
			"values": [
				["1877836900005000004", "{\"trace_id\":\"432\",\"parent_id\":\"qwerty\"}"]
			]
		}
	]
}`, []string{"a", "trace_id"}, []int64{1577836800000000001, 1577836900005000002, 1577836900005000003, 1877836900005000004}, `{"foo":"bar","a":"b","user_id":"123"}
{"foo":"bar","a":"b","trace_id":"pqw","_msg":"abc"}
{"foo":"bar","a":"b","_msg":"{def}"}
{"x":"y","_msg":"432","parent_id":"qwerty"}`)
}

func TestParsePromLabels_Success(t *testing.T) {
	f := func(s string) {
		t.Helper()
		fields, err := parsePromLabels(nil, s)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		var a []string
		for _, f := range fields {
			a = append(a, fmt.Sprintf("%s=%q", f.Name, f.Value))
		}
		result := "{" + strings.Join(a, ", ") + "}"
		if result != s {
			t.Fatalf("unexpected result;\ngot\n%s\nwant\n%s", result, s)
		}
	}

	f("{}")
	f(`{foo="bar"}`)
	f(`{foo="bar", baz="x", y="z"}`)
	f(`{foo="ba\"r\\z\n", a="", b="\"\\"}`)
}

func TestParsePromLabels_Failure(t *testing.T) {
	f := func(s string) {
		t.Helper()
		fields, err := parsePromLabels(nil, s)
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		if len(fields) > 0 {
			t.Fatalf("unexpected non-empty fields: %s", fields)
		}
	}

	f("")
	f("{")
	f(`{foo}`)
	f(`{foo=bar}`)
	f(`{foo="bar}`)
	f(`{foo="ba\",r}`)
	f(`{foo="bar" baz="aa"}`)
	f(`foobar`)
	f(`foo{bar="baz"}`)
}
