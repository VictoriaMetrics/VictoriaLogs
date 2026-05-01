package splunk

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
)

func TestProcessDataSuccess(t *testing.T) {
	f := func(data, timeField, msgField string, timestampsExpected []int64, resultExpected string) {
		t.Helper()

		timeFields := []string{timeField}
		msgFields := []string{msgField}
		tlp := &insertutil.TestLogMessageProcessor{}
		if err := processEvent([]byte(data), tlp, timeFields, msgFields, nil); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if err := tlp.Verify(timestampsExpected, resultExpected); err != nil {
			t.Fatal(err)
		}
	}

	data := `{"@timestamp":"2023-06-06T04:48:11.735Z","event":"foobar","source":"docker","host":"localhost"}` +
		`{"@timestamp":"2023-06-06T04:48:12.735+01:00","event":"baz"}` +
		`{"event":"xyz","@timestamp":"2023-06-06 04:48:13.735Z","x":"y"}`
	timeField := "@timestamp"
	msgField := "event"
	timestampsExpected := []int64{1686026891735000000, 1686023292735000000, 1686026893735000000}
	resultExpected := `{"_msg":"foobar","source":"docker","host":"localhost"}
{"_msg":"baz"}
{"_msg":"xyz","x":"y"}`
	f(data, timeField, msgField, timestampsExpected, resultExpected)

	// Non-existing msgField
	data = `{"@timestamp":"2023-06-06T04:48:11.735Z","log":{"offset":71770,"file":{"path":"/var/log/auth.log"}},"message":"foobar"}` +
		`{"@timestamp":"2023-06-06T04:48:12.735+01:00","message":"baz"}`
	timeField = "@timestamp"
	msgField = "foobar"
	timestampsExpected = []int64{1686026891735000000, 1686023292735000000}
	resultExpected = `{"log.offset":"71770","log.file.path":"/var/log/auth.log","message":"foobar"}
{"message":"baz"}`
	f(data, timeField, msgField, timestampsExpected, resultExpected)
}

func TestProcessDataFailure(t *testing.T) {
	f := func(data string) {
		t.Helper()

		tlp := &insertutil.TestLogMessageProcessor{}
		if err := processEvent([]byte(data), tlp, []string{"time"}, nil, nil); err == nil {
			t.Fatalf("expected error, got nil")
		}

		if err := tlp.Verify(nil, ""); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	// invalid json
	f("foobar")

	f(`foo
bar`)

	f(`
foo

`)

	// contains invalid JSON
	f(`{"time":"foobar}`)
}
