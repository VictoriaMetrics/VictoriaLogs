package tests

import (
	"math"
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

// TestVlclusterRerouting verifies the rerouting uniformity, to avoid uneven ingestion load.
func TestVlclusterRerouting(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	// start a special cluster. it runs with 2 fake addresses, plus the auto-attached 3 real addresses.
	// it should reroute data as some storage nodes are unavailable.
	sut := tc.MustStartVlclusterWithFlags([]string{"-storageNode=0.0.0.0:99999,0.0.0.0:99998"}, nil, nil)

	// ingest 1000 * 100 logs across storages.
	logPerBatch := 1000
	BatchCount := 100
	for i := 0; i < BatchCount; i++ {
		ingestRecords := make([]string, 0, logPerBatch)
		for j := 0; j < logPerBatch; j++ {
			ingestRecords = append(ingestRecords, `{"_msg":"abc","x":"`+strconv.Itoa(j)+`","_time":"2025-01-01T01:00:00Z"}`)
		}
		sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{
			StreamFields: "x",
		})
		sut.ForceFlush(t)
	}

	// check the distribution. it should be 1000 * 100 -> 3 healthy storages, with 10% threshold.
	//
	// there's no need to complicate the test code so just hardcode the server count to 3 when verifying the result.
	expectCountPerNode := float64(logPerBatch*BatchCount) / 3
	for i := 0; i < 3; i++ {
		sn := sut.StorageNode(i)
		count := sn.GetIntMetrics(tc.T(), `vl_rows_ingested_total{type="internalinsert"}`)

		deviation := math.Abs(1.0 - float64(count)/expectCountPerNode)
		if deviation > 0.1 {
			t.Fatalf("uneven distribution from rerouting when some nodes are unhealthy. expect count: %.1f, actual count: %d, deviation: %.3f",
				expectCountPerNode, count, deviation)
		}
	}
}
