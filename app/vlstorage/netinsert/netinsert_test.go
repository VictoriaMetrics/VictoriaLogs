package netinsert

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/valyala/fastrand"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/cespare/xxhash/v2"
)

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

func TestSendInsertRequestToAnyNode(t *testing.T) bool {
	// build the default nodes
	nodeCount := 21
	sns := make([]mockStorageNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		sns[i] = mockStorageNode{
			addr:      strconv.Itoa(i),
			available: true,
			count:     0,
		}
	}

	mockSendInsertRequestToAnyNode()
}

func mockSendInsertRequestToAnyNode(sns []mockStorageNode) bool {
	startIdx := int(fastrand.Uint32n(uint32(len(sns))))
	for i := range sns {
		idx := (startIdx + i) % len(sns)
		sn := sns[idx]
		err := sn.sendInsertRequest()
		if err == nil {
			return true
		}
		if !errors.Is(err, errTemporarilyDisabled) {
			logger.Warnf("cannot send pending data to the storage node %q: %s; trying to send it to another storage node", sn.addr, err)
		}
	}
	return false
}

type mockStorageNode struct {
	addr      string
	available bool

	// count represents the ingested log count
	count int
}

func (m *mockStorageNode) sendInsertRequest() error {
	if m.available {
		m.count++
	}
	return errTemporarilyDisabled
}
