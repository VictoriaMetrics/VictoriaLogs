package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// Verifies that `/select/logsql/stats_query_range` allows `total_stats` with `by (...)` fields,
// which are a subset of labels from the preceding `stats` pipe.
//
// The test also verifies that `total_stats by (_time)` returns the same per-step total for every original series.
func TestStatsQueryRangeTotalStatsBySubsetLabels(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	records := []string{
		`{"_msg":"a1","_time":"2025-01-01T00:00:10Z","service.name":"prefixA"}`,
		`{"_msg":"a2","_time":"2025-01-01T00:00:20Z","service.name":"prefixA"}`,
		`{"_msg":"b1","_time":"2025-01-01T00:00:30Z","service.name":"prefixB"}`,
		`{"_msg":"a3","_time":"2025-01-01T00:01:10Z","service.name":"prefixA"}`,
		`{"_msg":"b2","_time":"2025-01-01T00:01:20Z","service.name":"prefixB"}`,
		`{"_msg":"b3","_time":"2025-01-01T00:01:30Z","service.name":"prefixB"}`,
		`{"_msg":"b4","_time":"2025-01-01T00:01:40Z","service.name":"prefixB"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	query := `* | stats by (service.name) count() as count | total_stats by (_time) sum(count) as total_count`
	responseExpected := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"count","service.name":"prefixA"},"values":[[1735689600,"2"],[1735689660,"1"]]},{"metric":{"__name__":"count","service.name":"prefixB"},"values":[[1735689600,"1"],[1735689660,"3"]]},{"metric":{"__name__":"total_count","service.name":"prefixA"},"values":[[1735689600,"3"],[1735689660,"4"]]},{"metric":{"__name__":"total_count","service.name":"prefixB"},"values":[[1735689600,"3"],[1735689660,"4"]]}]}}`

	opts := apptest.StatsQueryRangeOpts{
		Start: "2025-01-01T00:00:00Z",
		End:   "2025-01-01T00:02:00Z",
		Step:  "1m",
	}
	response, status := sut.StatsQueryRangeRaw(t, query, opts)
	if status != http.StatusOK {
		t.Fatalf("unexpected HTTP status=%d; response=%q", status, response)
	}
	if response != responseExpected {
		t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
	}
}
