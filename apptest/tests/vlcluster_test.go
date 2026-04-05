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

	// Verify stats pipe with switch
	f("* | stats count() total, count() switch(case (aa) x, case (gh) y, case (aaa) yy, default z), count() if (abc) q", []string{
		`{"total":"5","x":"2","y":"1","yy":"0","z":"2","q":"1"}`,
	})

	// Verify in(items)
	f("x:in(y,aaa) | count() as logs", []string{
		`{"logs":"4"}`,
	})

	// Verify in(subquery) in filters
	f("x:in(def | keep x) | count() as logs", []string{
		`{"logs":"4"}`,
	})
	f("x:in(aa | keep x) x:-=y | count() as logs", []string{
		`{"logs":"1"}`,
	})

	// Verify in(items) in if() filters
	f("* | format if (x:in(x,z)) 'foo <_msg>' | filter ~'^foo .+' | keep _msg", []string{
		`{"_msg":"foo aa"}`,
	})

	// Verify in(subquery) in if() filters
	f("* | format if (x:in(aa | keep x) -x:=y) 'foo <_msg>' | filter ~'^foo .+' | keep _msg", []string{
		`{"_msg":"foo aa"}`,
	})

	// Verify join rows()
	f("* | join by (x) rows({x=z,q=a}) | filter q:* | keep _msg, q", []string{
		`{"_msg":"aa","q":"a"}`,
	})

	// Verify join(subquery)
	f("* | join by (x) (x:z) inner prefix abc | keep _msg, abc_msg", []string{
		`{"_msg":"aa","abc_msg":"aa"}`,
	})

	// Verify union rows()
	f("x:z | keep _msg | union rows({qwe=rty})", []string{
		`{"_msg":"aa"}`,
		`{"qwe":"rty"}`,
	})

	// Verify union(subquery)
	f("x:z | keep _msg | union (gh | keep x)", []string{
		`{"_msg":"aa"}`,
		`{"x":"y"}`,
	})

	// Verify union(rows) inside filters
	f("x:in(foo:bar | union rows({x=z}) | keep x) | x:z | keep _msg", []string{
		`{"_msg":"aa"}`,
	})

	// Verify union(subquery) inside filters
	f("x:in(foo:bar | union (aa) | keep x) | x:z | keep _msg", []string{
		`{"_msg":"aa"}`,
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
