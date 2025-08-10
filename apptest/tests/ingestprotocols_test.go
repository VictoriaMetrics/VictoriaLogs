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
		got := sut.LogsQLQuery(t, opts.query, apptest.QueryOptsLogs{})
		assertLogsQLResponseEqual(t, got, &apptest.LogsQLQueryResponse{LogLines: opts.wantLogLines})
	}
	// json line ingest
	sut.JSONLineWrite(t, []string{
		`{"_msg":"ingest jsonline","_time": "2025-06-05T14:30:19.088007Z", "foo":"bar"}`,
		`{"_msg":"ingest jsonline","_time": "2025-06-05T14:30:19.088007Z", "bar":"foo"}`,
	}, apptest.QueryOptsLogs{})
	f(&opts{
		query: "ingest jsonline",
		wantLogLines: []string{
			`{"_msg":"ingest jsonline","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","bar":"foo"}`,
			`{"_msg":"ingest jsonline","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
		},
	})
	// native format ingest
	sut.NativeWrite(t, []logstorage.InsertRow{
		{
			StreamTagsCanonical: canonicalStreamTagsFromSet(map[string]string{"foo": "bar"}),
			Timestamp:           1749141697409000000, // 2025-06-05T:18:41:37.000000Z
			Fields: []logstorage.Field{
				{
					Name:  "_msg",
					Value: "ingest native",
				},
				{
					Name:  "qwe",
					Value: "rty",
				},
			},
		},
	}, apptest.QueryOpts{})
	f(&opts{
		query: "ingest native",
		wantLogLines: []string{
			`{"_msg":"ingest native","_time":"2025-06-05T16:41:37.409Z", "_stream":"{foo=\"bar\"}", "qwe": "rty"}`,
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
