package netinsert

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/valyala/fastrand"
	"testing"
)

// BenchmarkSendInsertRequestToAnyNode compare the performance difference of shuffled/unshuffled rerouting.
func BenchmarkSendInsertRequestToAnyNode(b *testing.B) {
	for _, nodeCount := range []int{1, 5, 20, 100} {
		sns := make([]*mockStorageNode, nodeCount)
		for i := range sns {
			sns[i] = &mockStorageNode{addr: fmt.Sprintf("node-%d", i), available: true}
		}

		b.Run(fmt.Sprintf("shuffle/nodeCount=%d", nodeCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mockSendInsertRequestToAnyNode(sns)
			}
		})

		b.Run(fmt.Sprintf("noShuffle/nodeCount=%d", nodeCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mockSendInsertRequestToAnyNodeNoShuffle(sns)
			}
		})
	}
}

// mockSendInsertRequestToAnyNodeNoShuffle is build for comparison with the shuffled implementation of rerouting.
// it's not in used.
func mockSendInsertRequestToAnyNodeNoShuffle(sns []*mockStorageNode) bool {
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
