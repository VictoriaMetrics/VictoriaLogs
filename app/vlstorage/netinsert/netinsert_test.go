package netinsert

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestStorageDrainsPendingDataOnShutdown(t *testing.T) {
	oldTimeout := *drainTimeout
	*drainTimeout = 2 * time.Second
	defer func() { *drainTimeout = oldTimeout }()

	var insertRequests atomic.Int64
	var receivedBytes atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/insert" {
			body, _ := io.ReadAll(r.Body)
			receivedBytes.Add(int64(len(body)))
			insertRequests.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	ac, err := (&promauth.Options{}).NewConfig()
	if err != nil {
		t.Fatalf("cannot create auth config: %s", err)
	}

	addr := strings.TrimPrefix(ts.URL, "http://")
	s := NewStorage([]string{addr}, []*promauth.Config{ac}, []bool{false}, 1, true)

	// Buffer a single small row. It stays pending (it doesn't reach maxInsertBlockSize),
	// so it is only sent by the final flush performed during shutdown.
	r := &logstorage.InsertRow{
		Timestamp: 1,
		Fields:    []logstorage.Field{{Name: "foo", Value: "bar"}},
	}
	s.AddRow(0, r)

	// MustStop must drain the buffered row to the storage node instead of dropping it.
	s.MustStop()

	if n := insertRequests.Load(); n == 0 {
		t.Fatalf("expected the buffered data to be drained to the storage node on shutdown; got no insert requests")
	}
	if n := receivedBytes.Load(); n == 0 {
		t.Fatalf("expected the storage node to receive non-empty buffered data on shutdown")
	}
}

func TestStreamRowsTracker(t *testing.T) {
	f := func(rowsCount, streamsCount, nodesCount int) {
		t.Helper()

		// generate stream hashes
		streamHashes := make([]uint64, streamsCount)
		for i := range streamHashes {
			streamHashes[i] = xxhash.Sum64(fmt.Appendf(nil, "stream %d.", i))
		}

		srt := newStreamRowsTracker(nodesCount)

		rng := rand.New(rand.NewSource(0))
		rowsPerNode := make([]uint64, nodesCount)
		for range rowsCount {
			streamIdx := rng.Intn(streamsCount)
			h := streamHashes[streamIdx]
			nodeIdx := srt.getNodeIdx(h)
			rowsPerNode[nodeIdx]++
		}

		// Verify that rows are uniformly distributed among nodes.
		expectedRowsPerNode := float64(rowsCount) / float64(nodesCount)
		for nodeIdx, nodeRows := range rowsPerNode {
			if math.Abs(float64(nodeRows)-expectedRowsPerNode)/expectedRowsPerNode > 0.15 {
				t.Fatalf("non-uniform distribution of rows among nodes; node %d has %d rows, while it must have %v rows; rowsPerNode=%d",
					nodeIdx, nodeRows, expectedRowsPerNode, rowsPerNode)
			}
		}
	}

	rowsCount := 10000
	streamsCount := 9
	nodesCount := 2
	f(rowsCount, streamsCount, nodesCount)

	rowsCount = 10000
	streamsCount = 100
	nodesCount = 2
	f(rowsCount, streamsCount, nodesCount)

	rowsCount = 100000
	streamsCount = 1000
	nodesCount = 9
	f(rowsCount, streamsCount, nodesCount)
}
