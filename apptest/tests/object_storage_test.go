package tests

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestVlsingleObjectStorageOffloadAndQuery(t *testing.T) {
	fs.MustRemoveDir(t.Name())

	rustfs, ok := apptest.TryStartRustfs(t, "rustfs")
	if !ok {
		t.Skip("skipping: ../../bin/rustfs not found")
	}

	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	twoDaysAgo := time.Now().UTC().AddDate(0, 0, -2)
	ts := twoDaysAgo.Format("2006-01-02T15:04:05Z")

	records := []string{
		fmt.Sprintf(`{"_msg":"offload-test","_time":%q,"host":"web1"}`, ts),
		fmt.Sprintf(`{"_msg":"offload-test","_time":%q,"host":"web2"}`, ts),
	}

	// Phase 1: ingest data into a local-only VL instance.
	vl1 := tc.MustStartVlsingle("vl1", []string{"-retentionPeriod=100y"})
	vl1.JSONLineWrite(t, records, apptest.IngestOpts{})
	vl1.ForceFlush(t)
	tc.StopApp("vl1")

	// Phase 2: restart with S3 enabled on the same storage path.
	vl1StoragePath := filepath.Join(t.Name(), "vl1")
	vl2CachePath := filepath.Join(t.Name(), "cache2")
	vl2Flags := append(rustfs.OffloadFlags(),
		fmt.Sprintf("-storageDataPath=%s", vl1StoragePath),
		fmt.Sprintf("-offload.cachePath=%s", vl2CachePath),
		"-retentionPeriod=100y",
	)
	vl2 := tc.MustStartVlsingle("vl2", vl2Flags)

	wantLines := []string{
		fmt.Sprintf(`{"_msg":"offload-test","_stream":"{}","_time":%q,"host":"web1"}`, ts),
		fmt.Sprintf(`{"_msg":"offload-test","_stream":"{}","_time":%q,"host":"web2"}`, ts),
	}
	want := &apptest.LogsQLQueryResponse{LogLines: wantLines}

	// Wait for offload and verify data is still queryable (may be local or remote).
	var got *apptest.LogsQLQueryResponse
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got = vl2.LogsQLQuery(t, "offload-test", apptest.QueryOpts{})
		if len(got.LogLines) == len(records) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	assertLogsQLResponseEqual(t, got, want)

	tc.StopApp("vl2")

	// Phase 3: start a fresh instance with no local data but the same S3.
	vl3CachePath := filepath.Join(t.Name(), "cache3")
	vl3Flags := append(rustfs.OffloadFlags(),
		fmt.Sprintf("-offload.cachePath=%s", vl3CachePath),
		"-retentionPeriod=100y",
	)
	vl3 := tc.MustStartVlsingle("vl3", vl3Flags)

	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got = vl3.LogsQLQuery(t, "offload-test", apptest.QueryOpts{})
		if len(got.LogLines) == len(records) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	assertLogsQLResponseEqual(t, got, want)
}

func TestVlsingleObjectStorageReadAfterRestart(t *testing.T) {
	fs.MustRemoveDir(t.Name())

	rustfs, ok := apptest.TryStartRustfs(t, "rustfs")
	if !ok {
		t.Skip("skipping: ../../bin/rustfs not found")
	}

	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	twoDaysAgo := time.Now().UTC().AddDate(0, 0, -2)
	ts := twoDaysAgo.Format("2006-01-02T15:04:05Z")

	records := []string{
		fmt.Sprintf(`{"_msg":"restart-test","_time":%q,"svc":"a"}`, ts),
		fmt.Sprintf(`{"_msg":"restart-test","_time":%q,"svc":"b"}`, ts),
		fmt.Sprintf(`{"_msg":"restart-test","_time":%q,"svc":"c"}`, ts),
	}

	// Ingest into local-only instance, then offload by restarting with S3.
	vl1 := tc.MustStartVlsingle("vl1", []string{"-retentionPeriod=100y"})
	vl1.JSONLineWrite(t, records, apptest.IngestOpts{})
	vl1.ForceFlush(t)
	tc.StopApp("vl1")

	vl1StoragePath := filepath.Join(t.Name(), "vl1")
	offloadFlags := rustfs.OffloadFlags()

	vl2Flags := append(offloadFlags,
		fmt.Sprintf("-storageDataPath=%s", vl1StoragePath),
		fmt.Sprintf("-offload.cachePath=%s", filepath.Join(t.Name(), "cache2")),
		"-retentionPeriod=100y",
	)
	vl2 := tc.MustStartVlsingle("vl2", vl2Flags)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := vl2.LogsQLQuery(t, "restart-test", apptest.QueryOpts{}); len(got.LogLines) == len(records) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	tc.StopApp("vl2")

	for i := range 3 {
		vl3Flags := append(offloadFlags,
			fmt.Sprintf("-offload.cachePath=%s", filepath.Join(t.Name(), fmt.Sprintf("cache3-%d", i))),
			"-retentionPeriod=100y",
		)
		instance := fmt.Sprintf("vl3-%d", i)
		vl3 := tc.MustStartVlsingle(instance, vl3Flags)

		wantLines := []string{
			fmt.Sprintf(`{"_msg":"restart-test","_stream":"{}","_time":%q,"svc":"a"}`, ts),
			fmt.Sprintf(`{"_msg":"restart-test","_stream":"{}","_time":%q,"svc":"b"}`, ts),
			fmt.Sprintf(`{"_msg":"restart-test","_stream":"{}","_time":%q,"svc":"c"}`, ts),
		}
		want := &apptest.LogsQLQueryResponse{LogLines: wantLines}

		var got *apptest.LogsQLQueryResponse
		deadline = time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			got = vl3.LogsQLQuery(t, "restart-test", apptest.QueryOpts{})
			if len(got.LogLines) == len(records) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		assertLogsQLResponseEqual(t, got, want)
		tc.StopApp(instance)
	}
}
