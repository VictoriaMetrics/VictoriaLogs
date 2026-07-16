package netinsert

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/cespare/xxhash/v2"
	"github.com/valyala/fastrand"
	"math"
	"math/rand"
	"testing"
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

// TestSendInsertRequestToAnyNode test the uniformity when rerouting happen.
func TestSendInsertRequestToAnyNode(t *testing.T) {
	f := func(nodeCount int, unhealthyNodes []int, threshold float64) {
		// build the default nodes
		sns, unhealthyNodeIdx := buildStorageNodeSlice(nodeCount, unhealthyNodes)

		// do re-routing test
		total := 100000
		for i := 0; i < total; i++ {
			if !mockSendInsertRequestToAnyNode(sns) {
				t.Fatalf("fail to reroute data to any node")
			}
		}

		checkUniformity(t, sns, unhealthyNodeIdx, total, threshold)
	}

	// 2 nodes, 1 down, all should go to the other one, so deviation should be 0
	f(2, []int{0}, 0)

	// 10 nodes, random unhealthy nodes
	f(10, []int{1, 4, 7, 8}, 0.1)

	// 10 nodes, all unhealthy nodes are at the tail
	f(10, []int{6, 7, 8, 9}, 0.1)

	// 10 nodes, 8 down
	f(10, []int{0, 1, 2, 3, 4, 5, 6, 7}, 0.1)

	// 50 nodes
	f(50, []int{0, 1, 2, 3, 4, 5, 6, 7}, 0.1)
}

type mockStorageNode struct {
	addr        string
	isReachable bool

	// count represents the ingested log count
	count int
}

func (m *mockStorageNode) sendInsertRequest() error {
	if m.isReachable {
		m.count++
		return nil
	}
	return errTemporarilyDisabled
}

// mockSendInsertRequestToAnyNode is to test sendInsertRequestToAnyNode and their logic must be in sync.
func mockSendInsertRequestToAnyNode(sns []*mockStorageNode) bool {
	// availableBuf holds the index of reachable storage nodes. e.g. [0,1,3,4,5]
	availableBuf := getAvailableBuf()
	defer putAvailableBuf(availableBuf)

	for idx, sn := range sns {
		if sn.isReachable {
			availableBuf.idx = append(availableBuf.idx, idx)
		}
	}

	// pick a starting point from the availableBuf randomly, and try to reroute to this node.
	// if failed, reroute to the next node.
	//
	// e.g. [0, 1, 3, 4, 5]
	// 1. picked `4` as starting point.
	// 2. try to send to `4`.
	// 3. if failed, try `5`, `0`, `1`, `3`.
	start := int(fastrand.Uint32n(uint32(len(availableBuf.idx))))
	for i := range availableBuf.idx {
		idx := availableBuf.idx[(start+i)%len(availableBuf.idx)]
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

func buildStorageNodeSlice(nodeCount int, unhealthyNodes []int) ([]*mockStorageNode, map[int]bool) {
	unhealthyNodeIdx := make(map[int]bool, len(unhealthyNodes))
	for _, idx := range unhealthyNodes {
		unhealthyNodeIdx[idx] = true
	}

	sns := make([]*mockStorageNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		sns[i] = &mockStorageNode{
			addr:        fmt.Sprintf("node-%d", i),
			isReachable: !unhealthyNodeIdx[i],
			count:       0,
		}
	}

	return sns, unhealthyNodeIdx
}

func checkUniformity(t *testing.T, sns []*mockStorageNode, unhealthyNodeIdx map[int]bool, total int, threshold float64) {
	// check uniformity
	expectCountPerNode := float64(total) / float64(len(sns)-len(unhealthyNodeIdx))
	for _, sn := range sns {
		if !sn.isReachable {
			if sn.count != 0 {
				t.Fatalf("unhealthy node %s shouldn't ingest data, but ingested %d", sn.addr, sn.count)
			}
			continue
		}

		deviation := math.Abs(1.0 - float64(sn.count)/expectCountPerNode)
		if deviation > threshold {
			t.Fatalf("uneven distribution from rerouting when some nodes are unhealthy. total: %d, healthy node: %d, expect count: %.1f, actual count: %d, deviation: %.3f",
				total, len(sns)-len(unhealthyNodeIdx), expectCountPerNode, sn.count, deviation)
		}
	}
}
