package tests

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/google/go-cmp/cmp"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleKeyConcepts verifies cases from https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model for vl-single.
func TestVlsingleKeyConcepts(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	type opts struct {
		ingestRecords   []string
		ingestQueryArgs apptest.IngestOpts
		wantResponse    *apptest.LogsQLQueryResponse
		query           string
		selectQueryArgs apptest.QueryOpts
	}

	f := func(opts *opts) {
		t.Helper()
		sut.JSONLineWrite(t, opts.ingestRecords, opts.ingestQueryArgs)
		sut.ForceFlush(t)
		got := sut.LogsQLQuery(t, opts.query, opts.selectQueryArgs)
		assertLogsQLResponseEqual(t, got, opts.wantResponse)
	}

	// nested objects flatten
	f(&opts{
		ingestRecords: []string{
			`{"_msg":"case 1","_time": "2025-06-05T14:30:19.088007Z", "host": {"name": "foobar","os": {"version": "1.2.3"}}}`,
			`{"_msg":"case 1","_time": "2025-06-05T14:30:19.088007Z", "tags": ["foo", "bar"], "offset": 12345, "is_error": false}`,
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 1","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","host.name":"foobar","host.os.version":"1.2.3"}`,
				`{"_msg":"case 1","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","is_error":"false","offset":"12345","tags":"[\"foo\",\"bar\"]"}`,
			},
		},
		query: "case 1",
	})

	// obtain _msg value from non-default field
	f(&opts{
		ingestRecords: []string{
			`{"my_msg":"case 2","_time": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
			`{"my_msg_other":"case 2","_time": "2025-06-05T14:30:19.088007Z", "bar":"foo"}`,
		},
		ingestQueryArgs: apptest.IngestOpts{
			MessageField: "my_msg,my_msg_other",
		},
		query: "case 2",
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 2","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
				`{"_msg":"case 2","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo"}`,
			},
		},
	})

	// populate stream fields
	f(&opts{
		ingestRecords: []string{
			`{"my_msg":"case 3","_time": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
			`{"my_msg":"case 3","_time": "2025-06-05T14:30:19.088007Z", "bar":"foo"}`,
			`{"my_msg":"case 3","_time": "2025-06-05T14:30:19.088007Z", "bar":"foo","foo":"bar","baz":"bar"}`,
		},
		ingestQueryArgs: apptest.IngestOpts{
			MessageField: "my_msg",
			StreamFields: "foo,bar,baz",
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 3","_stream":"{foo=\"bar\"}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
				`{"_msg":"case 3","_stream":"{bar=\"foo\"}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo"}`,
				`{"_msg":"case 3","_stream":"{bar=\"foo\",baz=\"bar\",foo=\"bar\"}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo","foo":"bar","baz":"bar"}`,
			},
		},
		query: "case 3",
	})

	// obtain _time value from non-default field
	f(&opts{
		ingestRecords: []string{
			`{"_msg":"case 4","my_time_field": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
			`{"_msg":"case 4","my_other_time_field": "2025-06-05T14:30:19.088007Z", "bar":"foo"}`,
		},
		ingestQueryArgs: apptest.IngestOpts{
			TimeField: "my_time_field,my_other_time_field",
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 4","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
				`{"_msg":"case 4","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo"}`,
			},
		},
		query: "case 4",
	})

	// use global_filter option
	f(&opts{
		ingestRecords: []string{
			`{"_msg":"case 5","_time": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
			`{"_msg":"case 5","_time": "2025-06-05T14:30:19.088007Z", "foo":"abc", "x":"y"}`,
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 5","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar","x":"y"}`,
			},
		},
		query: "options(global_filter=('case 5')) foo:=bar | join by (_msg) (foo:=abc)",
	})

	// use field_max, field_min, row_max and row_min on _time column
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1294
	f(&opts{
		ingestRecords: []string{
			`{"_time":"2025-06-05T14:30:19Z","a":"b1","_msg":"issue 1294"}`,
			`{"_time":"2025-06-05T14:30:20Z","a":"b2","_msg":"issue 1294"}`,
			`{"_time":"2025-06-05T14:30:21Z","a":"b3","_msg":"issue 1294"}`,
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"a_max":"b3","a_min":"b1","a_max_row":"{\"a\":\"b3\"}","a_min_row":"{\"a\":\"b1\"}"}`,
			},
		},
		query: "'issue 1294' | field_max(_time, a) a_max, field_min(_time, a) a_min, row_max(_time, a) a_max_row, row_min(_time, a) a_min_row",
	})
}

// TestVlclusterKeyConcepts verifies cases from https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model for vl-cluster.
func TestVlclusterKeyConcepts(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlcluster()

	type opts struct {
		ingestRecords   []string
		ingestQueryArgs apptest.IngestOpts
		wantResponse    *apptest.LogsQLQueryResponse
		query           string
		selectQueryArgs apptest.QueryOpts
	}

	f := func(opts *opts) {
		t.Helper()
		sut.JSONLineWrite(t, opts.ingestRecords, opts.ingestQueryArgs)
		sut.ForceFlush(t)
		got := sut.LogsQLQuery(t, opts.query, opts.selectQueryArgs)
		assertLogsQLResponseEqual(t, got, opts.wantResponse)
	}

	// nested objects flatten
	f(&opts{
		ingestRecords: []string{
			`{"_msg":"case 1","_time": "2025-06-05T14:30:19.088007Z", "host": {"name": "foobar","os": {"version": "1.2.3"}}}`,
			`{"_msg":"case 1","_time": "2025-06-05T14:30:19.088007Z", "tags": ["foo", "bar"], "offset": 12345, "is_error": false}`,
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 1","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","host.name":"foobar","host.os.version":"1.2.3"}`,
				`{"_msg":"case 1","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","is_error":"false","offset":"12345","tags":"[\"foo\",\"bar\"]"}`,
			},
		},
		query: "case 1",
	})

	// obtain _msg value from non-default field
	f(&opts{
		ingestRecords: []string{
			`{"my_msg":"case 2","_time": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
			`{"my_msg_other":"case 2","_time": "2025-06-05T14:30:19.088007Z", "bar":"foo"}`,
		},
		ingestQueryArgs: apptest.IngestOpts{
			MessageField: "my_msg,my_msg_other",
		},
		query: "case 2",
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 2","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
				`{"_msg":"case 2","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo"}`,
			},
		},
	})

	// populate stream fields
	f(&opts{
		ingestRecords: []string{
			`{"my_msg":"case 3","_time": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
			`{"my_msg":"case 3","_time": "2025-06-05T14:30:19.088007Z", "bar":"foo"}`,
			`{"my_msg":"case 3","_time": "2025-06-05T14:30:19.088007Z", "bar":"foo","foo":"bar","baz":"bar"}`,
		},
		ingestQueryArgs: apptest.IngestOpts{
			MessageField: "my_msg",
			StreamFields: "foo,bar,baz",
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 3","_stream":"{foo=\"bar\"}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
				`{"_msg":"case 3","_stream":"{bar=\"foo\"}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo"}`,
				`{"_msg":"case 3","_stream":"{bar=\"foo\",baz=\"bar\",foo=\"bar\"}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo","foo":"bar","baz":"bar"}`,
			},
		},
		query: "case 3",
	})

	// obtain _time value from non-default field
	f(&opts{
		ingestRecords: []string{
			`{"_msg":"case 4","my_time_field": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
			`{"_msg":"case 4","my_other_time_field": "2025-06-05T14:30:19.088007Z", "bar":"foo"}`,
		},
		ingestQueryArgs: apptest.IngestOpts{
			TimeField: "my_time_field,my_other_time_field",
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 4","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
				`{"_msg":"case 4","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo"}`,
			},
		},
		query: "case 4",
	})

	// use global_filter option
	f(&opts{
		ingestRecords: []string{
			`{"_msg":"case 5","_time": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
			`{"_msg":"case 5","_time": "2025-06-05T14:30:19.088007Z", "foo":"abc", "x":"y"}`,
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"_msg":"case 5","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar","x":"y"}`,
			},
		},
		query: "options(global_filter=('case 5')) foo:=bar | join by (_msg) (foo:=abc)",
	})

	// use field_max, field_min, row_max and row_min on _time column
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1294
	f(&opts{
		ingestRecords: []string{
			`{"_time":"2025-06-05T14:30:19Z","a":"b1","_msg":"issue 1294"}`,
			`{"_time":"2025-06-05T14:30:20Z","a":"b2","_msg":"issue 1294"}`,
			`{"_time":"2025-06-05T14:30:21Z","a":"b3","_msg":"issue 1294"}`,
		},
		wantResponse: &apptest.LogsQLQueryResponse{
			LogLines: []string{
				`{"a_max":"b3","a_min":"b1","a_max_row":"{\"a\":\"b3\"}","a_min_row":"{\"a\":\"b1\"}"}`,
			},
		},
		query: "'issue 1294' | field_max(_time, a) a_max, field_min(_time, a) a_min, row_max(_time, a) a_max_row, row_min(_time, a) a_min_row",
	})
}

func assertLogsQLResponseEqual(t *testing.T, got, want *apptest.LogsQLQueryResponse) {
	t.Helper()
	sort.Strings(got.LogLines)
	sort.Strings(want.LogLines)
	if len(got.LogLines) != len(want.LogLines) {
		t.Errorf("unexpected response len: -%d: +%d\ngot\n%s\nwant\n%s", len(want.LogLines), len(got.LogLines), strings.Join(got.LogLines, "\n"), strings.Join(want.LogLines, "\n"))
		return
	}
	for i := range len(want.LogLines) {
		gotLine, wantLine := got.LogLines[i], want.LogLines[i]
		var gotLineJSON map[string]any
		var wantLineJSON map[string]any
		if err := json.Unmarshal([]byte(gotLine), &gotLineJSON); err != nil {
			t.Errorf("cannot parse got line=%q: %s", gotLine, err)
			return
		}
		if err := json.Unmarshal([]byte(wantLine), &wantLineJSON); err != nil {
			t.Errorf("cannot parse want line=%q: %s", wantLine, err)
			return
		}
		// stream_id is always unique, remove it from comparison
		delete(gotLineJSON, "_stream_id")
		delete(wantLineJSON, "_stream_id")
		if diff := cmp.Diff(gotLineJSON, wantLineJSON); diff != "" {
			t.Errorf("unexpected response (-want, +got):\n%s\n%s\n%s", diff, wantLine, gotLine)
			return
		}
	}
}

// assertLogsQLResponseOrdered is like assertLogsQLResponseEqual, but compares the responses
// in the order they were returned by the server — no pre-sort. Use this when the test needs
// to verify ordering (e.g. sort_direction=asc vs desc).
func assertLogsQLResponseOrdered(t *testing.T, got, want *apptest.LogsQLQueryResponse) {
	t.Helper()
	if len(got.LogLines) != len(want.LogLines) {
		t.Errorf("unexpected response len: -%d: +%d\ngot\n%s\nwant\n%s", len(want.LogLines), len(got.LogLines), strings.Join(got.LogLines, "\n"), strings.Join(want.LogLines, "\n"))
		return
	}
	for i := range len(want.LogLines) {
		gotLine, wantLine := got.LogLines[i], want.LogLines[i]
		var gotLineJSON map[string]any
		var wantLineJSON map[string]any
		if err := json.Unmarshal([]byte(gotLine), &gotLineJSON); err != nil {
			t.Errorf("cannot parse got line=%q: %s", gotLine, err)
			return
		}
		if err := json.Unmarshal([]byte(wantLine), &wantLineJSON); err != nil {
			t.Errorf("cannot parse want line=%q: %s", wantLine, err)
			return
		}
		delete(gotLineJSON, "_stream_id")
		delete(wantLineJSON, "_stream_id")
		if diff := cmp.Diff(gotLineJSON, wantLineJSON); diff != "" {
			t.Errorf("unexpected response at index %d (-want, +got):\n%s\n%s\n%s", i, diff, wantLine, gotLine)
			return
		}
	}
}
