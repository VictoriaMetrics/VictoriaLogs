package tests

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestVlsingleElasticsearchBulkTimestampParsing(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	io := apptest.IngestOpts{
		TimeField:    "@timestamp",
		MessageField: "message",
		StreamFields: "host",
	}
	uv := make(url.Values)
	uv.Set("_time_field", io.TimeField)
	uv.Set("_msg_field", io.MessageField)
	uv.Set("_stream_fields", io.StreamFields)
	ingestURL := fmt.Sprintf("http://%s/insert/elasticsearch/_bulk?%s", sut.HTTPAddr(), uv.Encode())

	f := func(timestamp, msg, wantTime string) {
		t.Helper()

		payload := fmt.Sprintf("{\"create\":{}}\n{\"@timestamp\":%q,\"message\":%q,\"host\":\"host123\"}\n", timestamp, msg)
		body, statusCode := tc.Client().Post(t, ingestURL, "application/x-ndjson", []byte(payload))
		if statusCode != http.StatusOK {
			t.Fatalf("unexpected status code: got %d; want %d; response=%q", statusCode, http.StatusOK, body)
		}

		sut.ForceFlush(t)
		got := sut.LogsQLQuery(t, msg, apptest.QueryOpts{})
		assertLogsQLResponseEqual(t, got, &apptest.LogsQLQueryResponse{
			LogLines: []string{
				fmt.Sprintf(`{"_msg":%q,"_stream":"{host=\"host123\"}","_time":%q,"host":"host123"}`, msg, wantTime),
			},
		})
	}

	f("2025-12-15T02:12:34.977Z", "case_tz_Z", "2025-12-15T02:12:34.977Z")
	f("2025-12-15T02:12:34.977+01:00", "case_tz_offset_hh_mm", "2025-12-15T01:12:34.977Z")
	// +0100 isn't RFC3339, but it is commonly used by Elasticsearch/Logstash clients.
	f("2025-12-15T02:12:34.977+0100", "case_tz_offset_hhmm", "2025-12-15T01:12:34.977Z")
}
