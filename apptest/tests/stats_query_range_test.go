package tests

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

type statsQueryResponse struct {
	Data struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
		} `json:"result"`
	} `json:"data"`
}

func getMetricNamesFromStatsResponse(t *testing.T, s string) []string {
	t.Helper()

	var resp statsQueryResponse
	if err := json.Unmarshal([]byte(s), &resp); err != nil {
		t.Fatalf("cannot unmarshal stats response %q: %s", s, err)
	}

	names := make([]string, 0, len(resp.Data.Result))
	for _, r := range resp.Data.Result {
		names = append(names, r.Metric["__name__"])
	}
	return names
}

func TestStatsQueryRangeMetricOrderConsistentWithStatsQuery(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	records := []string{
		`{"_time":"2025-01-01T00:00:01Z","source.bytes":1,"destination.bytes":10}`,
		`{"_time":"2025-01-01T00:00:02Z","source.bytes":2,"destination.bytes":20}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	query := "* | stats count() sessions, sum(source.bytes) source.bytes, sum(destination.bytes) destination.bytes"

	instantResponse, statusCode := sut.StatsQueryRaw(t, query, apptest.StatsQueryOpts{
		Time: "2025-01-01T00:01:00Z",
	})
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected statusCode when executing instant query %q; got %d; want %d", query, statusCode, http.StatusOK)
	}

	rangeResponse, statusCode := sut.StatsQueryRangeRaw(t, query, apptest.StatsQueryRangeOpts{
		Start: "2025-01-01T00:00:00Z",
		End:   "2025-01-01T00:01:00Z",
		Step:  "1m",
	})
	if statusCode != http.StatusOK {
		t.Fatalf("unexpected statusCode when executing range query %q; got %d; want %d", query, statusCode, http.StatusOK)
	}

	instantMetricNames := getMetricNamesFromStatsResponse(t, instantResponse)
	rangeMetricNames := getMetricNamesFromStatsResponse(t, rangeResponse)
	if !reflect.DeepEqual(rangeMetricNames, instantMetricNames) {
		t.Fatalf("unexpected metric order for stats_query_range\ngot\n%q\nwant\n%q", rangeMetricNames, instantMetricNames)
	}
}
