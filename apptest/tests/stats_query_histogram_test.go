package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestStatsQueryHistogram(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	records := []string{
		`{"_time":"2025-01-01T00:00:01Z","size":1,"x":"a"}`,
		`{"_time":"2025-01-01T00:00:02Z","size":2,"x":"b"}`,
		`{"_time":"2025-01-01T00:00:03Z","size":3,"x":"a"}`,
		`{"_time":"2025-01-01T00:00:04Z","size":4,"x":"b"}`,
		`{"_time":"2025-01-01T00:00:05Z","size":5,"x":"b"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	// check histogram without by (...)
	query := "* | stats histogram(size) as size"
	responseExpected := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"size_bucket","vmrange":"1.896e+00...2.154e+00"},"value":[1735689600,"1"]},{"metric":{"__name__":"size_bucket","vmrange":"2.783e+00...3.162e+00"},"value":[1735689600,"1"]},{"metric":{"__name__":"size_bucket","vmrange":"3.594e+00...4.084e+00"},"value":[1735689600,"1"]},{"metric":{"__name__":"size_bucket","vmrange":"4.642e+00...5.275e+00"},"value":[1735689600,"1"]},{"metric":{"__name__":"size_bucket","vmrange":"8.799e-01...1.000e+00"},"value":[1735689600,"1"]}]}}`

	queryOpts := apptest.StatsQueryOpts{
		Time: "2025-01-01T00:00:00Z",
	}
	response, statusCode := sut.StatsQueryRaw(t, query, queryOpts)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected statusCode when executing query %q; got %d; want %d", query, statusCode, http.StatusOK)
	}
	if response != responseExpected {
		t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
	}

	// check histogram with by (...)
	query = "* | stats by (x) histogram(size) as size | sort by (x)"
	responseExpected = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"size_bucket","x":"a","vmrange":"2.783e+00...3.162e+00"},"value":[1735689600,"1"]},{"metric":{"__name__":"size_bucket","x":"a","vmrange":"8.799e-01...1.000e+00"},"value":[1735689600,"1"]},{"metric":{"__name__":"size_bucket","x":"b","vmrange":"1.896e+00...2.154e+00"},"value":[1735689600,"1"]},{"metric":{"__name__":"size_bucket","x":"b","vmrange":"3.594e+00...4.084e+00"},"value":[1735689600,"1"]},{"metric":{"__name__":"size_bucket","x":"b","vmrange":"4.642e+00...5.275e+00"},"value":[1735689600,"1"]}]}}`

	queryOpts = apptest.StatsQueryOpts{
		Time: "2025-01-01T00:00:00Z",
	}
	response, statusCode = sut.StatsQueryRaw(t, query, queryOpts)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected statusCode when executing query %q; got %d; want %d", query, statusCode, http.StatusOK)
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
