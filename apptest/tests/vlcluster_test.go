package tests

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlclusterIngestAndQuery verifies that logs are correctly ingested and queried from cluster.
func TestVlclusterIngestAndQuery(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlcluster()

	ingestRecords := []string{
		`{"_msg":"abc","x":"y","_time":"2025-01-01T01:00:00Z"}`,
		`{"_msg":"def","x":"y","_time":"2025-01-01T01:00:00Z"}`,
		`{"_msg":"gh","x":"y","_time":"2025-01-01T01:00:00Z"}`,
		`{"_msg":"aa","x":"z","_time":"2025-01-01T01:00:00Z"}`,
		`{"_msg":"aa","x":"y","_time":"2025-01-01T01:00:00Z"}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{
		StreamFields: "x",
	})
	sut.ForceFlush(t)

	f := func(query string, responseExpected []string) {
		t.Helper()

		got := sut.LogsQLQuery(t, query, apptest.QueryOpts{})
		wantResponse := &apptest.LogsQLQueryResponse{
			LogLines: responseExpected,
		}
		assertLogsQLResponseEqual(t, got, wantResponse)
	}

	// Verify the number of streams
	f("* | count_uniq(_stream) as streams", []string{
		`{"streams":"2"}`,
	})

	// Verify the number of logs
	f("* | count() as logs", []string{
		`{"logs":"5"}`,
	})

	// Verify facets pipe.
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/940
	f("* | facets | filter field_name:=x", []string{
		`{"field_name":"x","field_value":"y","hits":"4"}`,
		`{"field_name":"x","field_value":"z","hits":"1"}`,
	})

	// Verify /select/logsql/facets endpoint
	facetsGot := sut.Facets(t, "*", apptest.FacetsOpts{})
	facetsWant := `{"facets":[{"field_name":"_msg","values":[{"field_value":"aa","hits":2},{"field_value":"abc","hits":1},{"field_value":"def","hits":1},{"field_value":"gh","hits":1}]},{"field_name":"_stream","values":[{"field_value":"{x=\"y\"}","hits":4},{"field_value":"{x=\"z\"}","hits":1}]},{"field_name":"_stream_id","values":[{"field_value":"00000000000000002ad05e686f093d33c2870f9717572b26","hits":4},{"field_value":"0000000000000000a721e815b30ad0d7ddf4ef8814d74251","hits":1}]},{"field_name":"x","values":[{"field_value":"y","hits":4},{"field_value":"z","hits":1}]}]}`
	if facetsGot != facetsWant {
		t.Fatalf("unexpected facets\ngot\n%s\nwant\n%s", facetsGot, facetsWant)
	}
}
