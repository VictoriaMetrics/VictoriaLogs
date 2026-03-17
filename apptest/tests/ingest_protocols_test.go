package tests

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestVlsingleIngestionProtocols(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()
	type opts struct {
		query        string
		wantLogLines []string
	}

	f := func(opts *opts) {
		t.Helper()
		sut.ForceFlush(t)
		got := sut.LogsQLQuery(t, opts.query, apptest.QueryOpts{})
		assertLogsQLResponseEqual(t, got, &apptest.LogsQLQueryResponse{LogLines: opts.wantLogLines})
	}

	// json line ingest
	sut.JSONLineWrite(t, []string{
		`{"_msg":"ingest jsonline","_time": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
		`{"_msg":"ingest jsonline","_time": "2025-06-05T14:30:19.088007Z", "bar":"foo"}`,
	}, apptest.IngestOpts{})
	f(&opts{
		query: `"ingest jsonline"`,
		wantLogLines: []string{
			`{"_msg":"ingest jsonline","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo"}`,
			`{"_msg":"ingest jsonline","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
		},
	})

	// json line with _stream field
	sut.JSONLineWrite(t, []string{
		`{"_msg":"ingest _stream jsonline","_time": "2025-06-05T14:30:19.088007Z", "foo":"bar", "_stream":"{foo=\"bar\"}", "_stream_id":"abcdefd"}`,
		`{"_msg":"ingest _stream jsonline","_time": "2025-06-05T14:30:20.088007Z", "bar":"foo", "host": "x", "_stream":"{host=\"x\"}"}`,
	}, apptest.IngestOpts{})
	f(&opts{
		query: `"ingest _stream jsonline"`,
		wantLogLines: []string{
			`{"_msg":"ingest _stream jsonline","_stream":"{foo=\"bar\"}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
			`{"_msg":"ingest _stream jsonline","_stream":"{host=\"x\"}","_time":"2025-06-05T14:30:20.088007Z","bar":"foo","host":"x"}`,
		},
	})

	// native format ingest
	sut.NativeWrite(t, []logstorage.InsertRow{
		{
			StreamTagsCanonical: canonicalStreamTagsFromSet(map[string]string{"foo": "bar"}),
			Timestamp:           1749141697409000000,
			Fields: []logstorage.Field{
				{
					Name:  "_msg",
					Value: "ingest native",
				},
				{
					Name:  "qwe",
					Value: "rty",
				},
				{
					Name:  "foo",
					Value: "bar",
				},
			},
		},
	}, apptest.QueryOpts{})
	f(&opts{
		query: `"ingest native"`,
		wantLogLines: []string{
			`{"_msg":"ingest native","_time":"2025-06-05T16:41:37.409Z", "_stream":"{foo=\"bar\"}", "foo": "bar", "qwe": "rty"}`,
		},
	})

}

func canonicalStreamTagsFromSet(set map[string]string) string {
	var st logstorage.StreamTags
	for key, value := range set {
		st.Add(key, value)
	}
	return string(st.MarshalCanonical(nil))
}
