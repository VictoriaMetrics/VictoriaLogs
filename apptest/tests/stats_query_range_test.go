package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestVlsingleStatsQueryRange_Success(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	records := []string{
		`{"_msg":"a","_time":"2025-01-01T00:00:00Z","x":"1"}`,
		`{"_msg":"b","_time":"2025-01-01T00:00:01Z","x":"2"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	f := func(query, resultExpected string) {
		t.Helper()

		opts := apptest.StatsQueryRangeOpts{
			Start: "2025-01-01T00:00:00Z",
			End:   "2025-01-01T00:05:00Z",
			Step:  "1m",
		}
		result, status := sut.StatsQueryRangeRaw(t, query, opts)
		if status != http.StatusOK {
			t.Fatalf("unexpected status; got %d; want %d", status, http.StatusOK)
		}
		if result != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}

	// a single metric
	f(`* | stats count() q`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"q"},"values":[[1735689600,"2"]]}]}}`)

	// multiple metrics
	f(`* | stats count() q, max(x) xmax`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"q"},"values":[[1735689600,"2"]]},{"metric":{"__name__":"xmax"},"values":[[1735689600,"2"]]}]}}`)

	// filter
	f(`* | stats count() q | filter q:>0`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"q"},"values":[[1735689600,"2"]]}]}}`)
	f(`* | stats count() q | fiter q:>5`, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)

	// math with keep
	f(`* | stats by (x) count() q, max(x) xmax | math q / xmax as y | sort by (y desc) | keep x, y`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"y","x":"1"},"values":[[1735689900,"1"]]},{"metric":{"__name__":"y","x":"2"},"values":[[1735689900,"0.5"]]}]}}`)
	f(`* | stats by (x) count() q, max(x) xmax | math q / xmax as y | sort by (y desc) | keep y, x`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"y","x":"1"},"values":[[1735689900,"1"]]},{"metric":{"__name__":"y","x":"2"},"values":[[1735689900,"0.5"]]}]}}`)

	// sort
	f(`* | stats by (x) count() q | sort by (q desc) limit 1`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"q","x":"1"},"values":[[1735689600,"1"]]}]}}`)
	f(`* | stats by (x) count() q | first 1 by (q desc)`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"q","x":"1"},"values":[[1735689600,"1"]]}]}}`)
	f(`* | stats by (x) count() q | last 1 by (q)`, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"q","x":"1"},"values":[[1735689600,"1"]]}]}}`)
}

func TestVlsingleStatsQueryRange_Failure(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	records := []string{
		`{"_msg":"a","_time":"2025-01-01T00:00:00Z","x":"1"}`,
		`{"_msg":"b","_time":"2025-01-01T00:00:01Z","x":"2"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	f := func(query, step string) {
		t.Helper()

		opts := apptest.StatsQueryRangeOpts{
			Start: "2025-01-01T00:00:00Z",
			End:   "2025-01-01T00:05:00Z",
			Step:  step,
		}
		response, status := sut.StatsQueryRangeRaw(t, query, opts)
		if status == http.StatusOK {
			t.Fatalf("unexpected status %d; response:\n%s", http.StatusOK, response)
		}
	}

	// missing stats
	f(`*`, "1m")
	f(`* | last 1`, "1m")

	// modifying/removing _time is NOT OK
	f(`* | fields x | stats count()`, "1m")
	f(`* | delete _time | stats count()`, "1m")

	// step must be > 0
	f(`* | stats count()`, "0s")
	f(`* | stats count()`, "-5m")

	// limit and offset aren't allowed for range queries
	f(`* | stats count() q | limit 10`, "5m")
	f(`* | stats count() q | offset 10`, "5m")
}

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

func TestStatsQueryRangeHistogram(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	records := []string{
		`{"_time":"2025-01-01T00:00:01Z","size":1,"x":"a"}`,
		`{"_time":"2025-01-01T00:00:02Z","size":2,"x":"a"}`,
		`{"_time":"2025-01-01T00:00:03Z","size":3,"x":"b"}`,
		`{"_time":"2025-01-01T00:00:04Z","size":4,"x":"a"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	// check histogram without by (...)
	query := "* | stats histogram(size) as size"
	responseExpected := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"size_bucket","vmrange":"1.896e+00...2.154e+00"},"values":[[1735689600,"1"]]},{"metric":{"__name__":"size_bucket","vmrange":"2.783e+00...3.162e+00"},"values":[[1735689603,"1"]]},{"metric":{"__name__":"size_bucket","vmrange":"3.594e+00...4.084e+00"},"values":[[1735689603,"1"]]},{"metric":{"__name__":"size_bucket","vmrange":"8.799e-01...1.000e+00"},"values":[[1735689600,"1"]]}]}}`

	queryOpts := apptest.StatsQueryRangeOpts{
		Start: "2025-01-01T00:00:00Z",
		End:   "2025-01-01T00:00:06Z",
		Step:  "3s",
	}
	response, statusCode := sut.StatsQueryRangeRaw(t, query, queryOpts)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected statusCode when executing query %q; got %d; want %d", query, statusCode, http.StatusOK)
	}
	if response != responseExpected {
		t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
	}

	// check histogram wit by (...)
	query = "* | stats by (x) histogram(size) as size"
	responseExpected = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"size_bucket","x":"a","vmrange":"1.896e+00...2.154e+00"},"values":[[1735689600,"1"]]},{"metric":{"__name__":"size_bucket","x":"a","vmrange":"3.594e+00...4.084e+00"},"values":[[1735689603,"1"]]},{"metric":{"__name__":"size_bucket","x":"a","vmrange":"8.799e-01...1.000e+00"},"values":[[1735689600,"1"]]},{"metric":{"__name__":"size_bucket","x":"b","vmrange":"2.783e+00...3.162e+00"},"values":[[1735689603,"1"]]}]}}`

	queryOpts = apptest.StatsQueryRangeOpts{
		Start: "2025-01-01T00:00:00Z",
		End:   "2025-01-01T00:00:06Z",
		Step:  "3s",
	}
	response, statusCode = sut.StatsQueryRangeRaw(t, query, queryOpts)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected statusCode when executing query %q; got %d; want %d", query, statusCode, http.StatusOK)
	}
	if response != responseExpected {
		t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
	}
}
