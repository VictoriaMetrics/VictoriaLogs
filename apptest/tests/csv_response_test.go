package tests

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestVlsingleQueryCSVResponse(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ingestRecords := []string{
		`{"_msg":"case 1\",2","_time": "2025-06-05T14:30:19.088007Z", "host": {"name": "foobar","os": {"version": "1.2.3"}}}`,
		`{"_msg":"case 2","_time": "2025-06-06T14:30:19.088007Z", "tags": ["foo", "bar"], "offset": 12345, "is_error": false}`,
		`{"_msg":"stats_pipe","_time": "2025-06-05T14:30:19.088007Z", "host": {"name": "foobar","os": {"version": "1.2.3"}}}`,
		`{"_msg":"stats_pipe","_time": "2025-06-06T14:30:19.088007Z", "tags": ["foo", "bar"], "offset": 12345, "is_error": false}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{})
	sut.ForceFlush(t)

	f := func(query, responseExpected string) {
		t.Helper()

		response, statusCode := sut.LogsQLQueryRaw(t, query, apptest.QueryOpts{
			Format: "csv",
		})
		if statusCode != 200 {
			t.Fatalf("unexpected status code; got %d; want 200; response body\n%s", statusCode, response)
		}
		if response != responseExpected {
			t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
		}
	}

	// query ending with fields pipe
	query := "case | sort by (_time) | fields _time, _msg, host.name, is_error"
	responseExpected := `_time,_msg,host.name,is_error
2025-06-05T14:30:19.088007Z,"case 1"",2",foobar,
2025-06-06T14:30:19.088007Z,case 2,,false
`
	f(query, responseExpected)

	// query ending with stats pipe
	query = "stats_pipe | stats by (host.name) sum(offset) as sum_offset, count() | sort by (sum_offset)"
	responseExpected = `sum_offset,host.name,count(*)
12345,,1
NaN,foobar,1
`
	f(query, responseExpected)

	// 'select all' query
	query = "* | rm _stream_id | sort by (_time, _msg desc)"
	responseExpected = `_msg,_stream,_time,host.name,host.os.version,is_error,offset,tags
stats_pipe,{},2025-06-05T14:30:19.088007Z,foobar,1.2.3,,,
"case 1"",2",{},2025-06-05T14:30:19.088007Z,foobar,1.2.3,,,
stats_pipe,{},2025-06-06T14:30:19.088007Z,,,false,12345,"[""foo"",""bar""]"
case 2,{},2025-06-06T14:30:19.088007Z,,,false,12345,"[""foo"",""bar""]"
`
	f(query, responseExpected)
}

func TestVlclusterQueryCSVResponse(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlcluster()

	ingestRecords := []string{
		`{"_msg":"case 1\",2","_time": "2025-06-05T14:30:19.088007Z", "host": {"name": "foobar","os": {"version": "1.2.3"}}}`,
		`{"_msg":"case 2","_time": "2025-06-06T14:30:19.088007Z", "tags": ["foo", "bar"], "offset": 12345, "is_error": false}`,
		`{"_msg":"stats_pipe","_time": "2025-06-05T14:30:19.088007Z", "host": {"name": "foobar","os": {"version": "1.2.3"}}}`,
		`{"_msg":"stats_pipe","_time": "2025-06-06T14:30:19.088007Z", "tags": ["foo", "bar"], "offset": 12345, "is_error": false}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{})
	sut.ForceFlush(t)

	f := func(query, responseExpected string) {
		t.Helper()

		response, statusCode := sut.LogsQLQueryRaw(t, query, apptest.QueryOpts{
			Format: "csv",
		})
		if statusCode != 200 {
			t.Fatalf("unexpected status code; got %d; want 200; response body\n%s", statusCode, response)
		}
		if response != responseExpected {
			t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
		}
	}

	// query ending with fields pipe
	query := "case | sort by (_time) | fields _time, _msg, host.name, is_error"
	responseExpected := `_time,_msg,host.name,is_error
2025-06-05T14:30:19.088007Z,"case 1"",2",foobar,
2025-06-06T14:30:19.088007Z,case 2,,false
`
	f(query, responseExpected)

	// query ending with stats pipe
	query = "stats_pipe | stats by (host.name) sum(offset) as sum_offset, count() | sort by (sum_offset)"
	responseExpected = `sum_offset,host.name,count(*)
12345,,1
NaN,foobar,1
`
	f(query, responseExpected)

	// 'select all' query
	query = "* | rm _stream_id | sort by (_time, _msg desc)"
	responseExpected = `_msg,_stream,_time,host.name,host.os.version,is_error,offset,tags
stats_pipe,{},2025-06-05T14:30:19.088007Z,foobar,1.2.3,,,
"case 1"",2",{},2025-06-05T14:30:19.088007Z,foobar,1.2.3,,,
stats_pipe,{},2025-06-06T14:30:19.088007Z,,,false,12345,"[""foo"",""bar""]"
case 2,{},2025-06-06T14:30:19.088007Z,,,false,12345,"[""foo"",""bar""]"
`
	f(query, responseExpected)
}
