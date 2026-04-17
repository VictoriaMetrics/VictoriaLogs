package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestVlsingleStatsQuery_Success(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	records := []string{
		`{"_msg":"a","_time":"2025-01-01T00:00:00Z","x":"1"}`,
		`{"_msg":"b","_time":"2025-01-01T00:00:01Z","x":"5"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	f := func(query, resultExpected string) {
		t.Helper()

		opts := apptest.StatsQueryOpts{
			Time: "2025-01-01T00:05:00Z",
		}
		result, status := sut.StatsQueryRaw(t, query, opts)
		if status != http.StatusOK {
			t.Fatalf("unexpected status; got %d; want %d", status, http.StatusOK)
		}
		if result != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}

	// a single metric
	f(`* | stats count() q`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"q"},"value":[1735689900,"2"]}]}}`)

	// multiple metrics
	f(`* | stats count() q, max(x) xmax`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"q"},"value":[1735689900,"2"]},{"metric":{"__name__":"xmax"},"value":[1735689900,"5"]}]}}`)

	// filter
	f(`* | stats count() q | filter q:>0`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"q"},"value":[1735689900,"2"]}]}}`)
	f(`* | stats count() q | fiter q:>5`, `{"status":"success","data":{"resultType":"vector","result":[]}}`)

	// math with keep
	f(`* | stats by (x) count() q, max(x) xmax | math q / xmax as y | sort by (y desc) | keep x, y`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"y","x":"1"},"value":[1735689900,"1"]},{"metric":{"__name__":"y","x":"5"},"value":[1735689900,"0.2"]}]}}`)
	f(`* | stats by (x) count() q, max(x) xmax | math q / xmax as y | sort by (y desc) | keep y, x`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"y","x":"1"},"value":[1735689900,"1"]},{"metric":{"__name__":"y","x":"5"},"value":[1735689900,"0.2"]}]}}`)

	// sort
	f(`* | stats by (x) count() q | sort by (q desc) limit 1`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"q","x":"5"},"value":[1735689900,"1"]}]}}`)
	f(`* | stats by (x) count() q | first 1 by (q desc)`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"q","x":"5"},"value":[1735689900,"1"]}]}}`)
	f(`* | stats by (x) count() q | last 1 by (q)`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"q","x":"5"},"value":[1735689900,"1"]}]}}`)

	// limit
	f(`* | stats count() q | limit 10`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"q"},"value":[1735689900,"2"]}]}}`)

	// offset
	f(`* | stats count() q | offset 1`, `{"status":"success","data":{"resultType":"vector","result":[]}}`)

	// it is OK to drop _time when calculating instant stats
	f(`* | fields x | stats count() q`, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"q"},"value":[1735689900,"2"]}]}}`)
	f(`* | delete _time | stats count() q `, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"q"},"value":[1735689900,"2"]}]}}`)
}

func TestVlsingleStatsQuery_Failure(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	records := []string{
		`{"_msg":"a","_time":"2025-01-01T00:00:00Z","x":"1"}`,
		`{"_msg":"b","_time":"2025-01-01T00:00:01Z","x":"5"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	f := func(query string) {
		t.Helper()

		opts := apptest.StatsQueryOpts{
			Time: "2025-01-01T00:05:00Z",
		}
		result, status := sut.StatsQueryRaw(t, query, opts)
		if status == http.StatusOK {
			t.Fatalf("unexpected status %d; result:\n%s", http.StatusOK, result)
		}
	}

	// missing stats pipe
	f(`*`)
	f(`* | last 1`)
}

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

func TestStatsQueryRelativeTime(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	records := []string{
		`{"app":"foo","ts":"2026-03-27T11:54:59.999999999Z","msg":"11:54:59.999999999"}`,
		`{"app":"foo","ts":"2026-03-27T11:55:00.000000000Z","msg":"11:55:00.000000000"}`,
		`{"app":"foo","ts":"2026-03-27T11:55:00.000000001Z","msg":"11:55:00.000000001"}`,
		`{"app":"foo","ts":"2026-03-27T11:59:59.999999999Z","msg":"11:59:59.999999999"}`,
		`{"app":"foo","ts":"2026-03-27T12:00:00.000000000Z","msg":"12:00:00.000000000"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{
		MessageField: "msg",
		StreamFields: "app",
		TimeField:    "ts",
	})
	sut.ForceFlush(t)

	// The _time:5m must take into account logs on the [Time-5m ... Time) time range.
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1226
	query := `{app="foo"} AND _time:5m | min(_time) tmin, max(_time) tmax, count() hits`
	responseExpected := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"tmin"},"value":[1774612800,"2026-03-27T11:55:00Z"]},{"metric":{"__name__":"tmax"},"value":[1774612800,"2026-03-27T11:59:59.999999999Z"]},{"metric":{"__name__":"hits"},"value":[1774612800,"3"]}]}}`

	queryOpts := apptest.StatsQueryOpts{
		Time: "2026-03-27T12:00:00Z",
	}
	response, statusCode := sut.StatsQueryRaw(t, query, queryOpts)
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected statusCode when executing query %q; got %d; want %d", query, statusCode, http.StatusOK)
	}
	if response != responseExpected {
		t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
	}
}
