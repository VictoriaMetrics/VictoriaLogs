package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

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
