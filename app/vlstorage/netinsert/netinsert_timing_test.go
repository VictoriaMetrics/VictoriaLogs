package netinsert

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/valyala/fastrand"
	"math/rand"
	"testing"
)

// BenchmarkSendInsertRequestToAnyNode compare the performance difference of shuffled/unshuffled rerouting.
func BenchmarkSendInsertRequestToAnyNode(b *testing.B) {
	for _, nodeCount := range []int{1, 5, 20, 100} {
		sns := make([]*mockStorageNode, nodeCount)
		for i := range sns {
			sns[i] = &mockStorageNode{addr: fmt.Sprintf("node-%d", i), isReachable: true}
		}

		b.Run(fmt.Sprintf("available/nodeCount=%d", nodeCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mockSendInsertRequestToAnyNode(sns)
			}
		})

		b.Run(fmt.Sprintf("available-without-pooling/nodeCount=%d", nodeCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mockSendInsertRequestToAnyNodeWithoutPool(sns)
			}
		})

		b.Run(fmt.Sprintf("shuffle-without-pooling/nodeCount=%d", nodeCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mockSendInsertRequestToAnyNodeShuffle(sns)
			}
		})

		b.Run(fmt.Sprintf("rand/nodeCount=%d", nodeCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mockSendInsertRequestToAnyNodeRand(sns)
			}
		})
	}
}

// mockSendInsertRequestToAnyNodeRand is for comparison with the original implementation.
func mockSendInsertRequestToAnyNodeRand(sns []*mockStorageNode) bool {
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

// mockSendInsertRequestToAnyNodeWithoutPool is for comparison with pooling implementation.
func mockSendInsertRequestToAnyNodeWithoutPool(sns []*mockStorageNode) bool {
	availableIdx := make([]int, 0, len(sns))

	for idx, sn := range sns {
		if sn.isReachable {
			availableIdx = append(availableIdx, idx)
		}
	}
	startIdx := int(fastrand.Uint32n(uint32(len(availableIdx))))
	for i := range availableIdx {
		idxOfIdx := (startIdx + i) % len(availableIdx)
		sn := sns[availableIdx[idxOfIdx]]
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

// mockSendInsertRequestToAnyNodeShuffle is for comparison with available implementation.
func mockSendInsertRequestToAnyNodeShuffle(sns []*mockStorageNode) bool {
	availableIdx := make([]int, 0, len(sns))
	for i := 0; i < len(sns); i++ {
		availableIdx = append(availableIdx, i)
	}
	rand.Shuffle(len(availableIdx), func(i, j int) {
		availableIdx[i], availableIdx[j] = availableIdx[j], availableIdx[i]
	})

	for _, idx := range availableIdx {
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
