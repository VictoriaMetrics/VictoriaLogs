package datadog

import (
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
)

func TestReadLogsRequestFailure(t *testing.T) {
	f := func(data string) {
		t.Helper()

		ts := time.Now().UnixNano()

		lmp := &insertutil.TestLogMessageProcessor{}
		if err := readLogsRequest(ts, []byte(data), lmp); err == nil {
			t.Fatalf("expecting non-empty error")
		}
		if err := lmp.Verify(nil, ""); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
	f("foobar")
	f(`{}`)
	f(`["create":{}]`)
	f(`{"create":{}}
foobar`)
}

func TestReadLogsRequestSuccess(t *testing.T) {
	f := func(data string, rowsExpected int, resultExpected string) {
		t.Helper()

		ts := time.Now().UnixNano()
		var timestampsExpected []int64
		for range rowsExpected {
			timestampsExpected = append(timestampsExpected, ts)
		}
		lmp := &insertutil.TestLogMessageProcessor{}
		if err := readLogsRequest(ts, []byte(data), lmp); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := lmp.Verify(timestampsExpected, resultExpected); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	// Verify non-empty data
	data := `[
		{
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"bar",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":{"message": "nested"},
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"foobar",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"baz",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"xyz",
			"service":"test"
		}, {
			"ddsource": "nginx",
			"ddtags":"tag1:value1,tag2:value2,",
			"hostname":"127.0.0.1",
			"message":"xyz",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":",tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"xyz",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"env:prod,foo",
			"hostname":"127.0.0.1",
			"message":"qux",
			"service":"test"
		}
	]`
	rowsExpected := 8
	resultExpected := `{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"bar","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"nested","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"foobar","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"baz","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"xyz","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"xyz","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"xyz","service":"test"}
{"ddsource":"nginx","env":"prod","foo":"no_label_value","hostname":"127.0.0.1","_msg":"qux","service":"test"}`
	f(data, rowsExpected, resultExpected)
}
