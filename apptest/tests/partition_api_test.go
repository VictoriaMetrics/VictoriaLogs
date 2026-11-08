package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlsingleReadOnlyPartition verifies that the partition attached via /internal/partition/attach is read-only:
// it can be queried, it doesn't accept new logs and it remains read-only after the restart.
//
// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1744
func TestVlsingleReadOnlyPartition(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	const instance = "vlsingle"
	sutFlags := []string{
		"-storageDataPath=" + t.TempDir(),
	}
	sut := tc.MustStartVlsingle(instance, sutFlags)
	cli := tc.Client()

	const partitionName = "20250605"

	postOK := func(path string) string {
		t.Helper()
		url := "http://" + sut.HTTPAddr() + path
		body, statusCode := cli.Post(t, url, "", nil)
		if statusCode != http.StatusOK {
			t.Fatalf("unexpected status code for POST %s: got %d; want %d; body\n%s", url, statusCode, http.StatusOK, body)
		}
		return body
	}
	checkPartitionList := func(wantNames, wantDetails string) {
		t.Helper()
		if body := postOK("/internal/partition/list"); body != wantNames {
			t.Fatalf("unexpected partition list; got %s; want %s", body, wantNames)
		}
		if body := postOK("/internal/partition/list?details=1"); body != wantDetails {
			t.Fatalf("unexpected partition list details; got %s; want %s", body, wantDetails)
		}
	}
	checkLogs := func(wantLogLines []string) {
		t.Helper()
		assertLogsQLResponseEventually(tc, func() *apptest.LogsQLQueryResponse {
			sut.ForceFlush(t)
			return sut.LogsQLQuery(t, "*", apptest.QueryOpts{})
		}, &apptest.LogsQLQueryResponse{LogLines: wantLogLines})
	}
	checkMetric := func(metricName string, want int) {
		t.Helper()
		// The /metrics output is cached for a second, so the metric value may lag behind.
		tc.Assert(&apptest.AssertOptions{
			Msg: "unexpected value for " + metricName + " metric",
			Got: func() any {
				return sut.GetIntMetric(t, metricName)
			},
			Want:    want,
			FailNow: true,
		})
	}

	sut.JSONLineWrite(t, []string{
		`{"_msg":"before attach","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
	}, apptest.IngestOpts{})
	wantLogLines := []string{
		`{"_msg":"before attach","_stream":"{}","_time":"2025-06-05T14:30:19.088007Z","foo":"bar"}`,
	}
	checkLogs(wantLogLines)
	checkPartitionList(`["20250605"]`, `[{"name":"20250605","read_only":false}]`)
	checkMetric("vl_read_only_partitions", 0)

	// The re-attached partition must become read-only.
	postOK("/internal/partition/detach?name=" + partitionName)
	checkPartitionList(`[]`, `[]`)
	postOK("/internal/partition/attach?name=" + partitionName)
	checkPartitionList(`["20250605"]`, `[{"name":"20250605","read_only":true}]`)
	checkLogs(wantLogLines)
	checkMetric("vl_read_only_partitions", 1)

	// Logs ingested into the read-only partition must be dropped.
	sut.JSONLineWrite(t, []string{
		`{"_msg":"after attach","_time":"2025-06-05T15:30:19.088007Z","foo":"bar"}`,
	}, apptest.IngestOpts{})
	checkLogs(wantLogLines)
	checkMetric(`vl_rows_dropped_total{reason="read_only_partition"}`, 1)

	// Force merge must not fail on the read-only partition.
	postOK("/internal/force_merge?partition_prefix=" + partitionName)

	// The partition must remain read-only after the restart.
	tc.StopApp(instance)
	sut = tc.MustStartVlsingle(instance, sutFlags)
	checkPartitionList(`["20250605"]`, `[{"name":"20250605","read_only":true}]`)
	checkLogs(wantLogLines)
	checkMetric("vl_read_only_partitions", 1)

	sut.JSONLineWrite(t, []string{
		`{"_msg":"after restart","_time":"2025-06-05T16:30:19.088007Z","foo":"bar"}`,
	}, apptest.IngestOpts{})
	checkLogs(wantLogLines)
	checkMetric(`vl_rows_dropped_total{reason="read_only_partition"}`, 1)

	// The detached partition must be opened in read-write mode after the restart.
	postOK("/internal/partition/detach?name=" + partitionName)
	tc.StopApp(instance)
	sut = tc.MustStartVlsingle(instance, sutFlags)
	checkPartitionList(`["20250605"]`, `[{"name":"20250605","read_only":false}]`)
	checkMetric("vl_read_only_partitions", 0)

	sut.JSONLineWrite(t, []string{
		`{"_msg":"after detach","_time":"2025-06-05T17:30:19.088007Z","foo":"bar"}`,
	}, apptest.IngestOpts{})
	wantLogLines = append(wantLogLines, `{"_msg":"after detach","_stream":"{}","_time":"2025-06-05T17:30:19.088007Z","foo":"bar"}`)
	checkLogs(wantLogLines)
}
