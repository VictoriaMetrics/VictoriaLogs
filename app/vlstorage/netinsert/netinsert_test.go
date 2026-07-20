package netinsert

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
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

func TestStorageReroutesDataFromUnresponsiveNode(t *testing.T) {
	// The unresponsive node accepts the request, then blocks until the test releases it.
	releaseCh := make(chan struct{})
	reachedCh := make(chan struct{})
	unresponsive := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		reachedCh <- struct{}{}
		<-releaseCh
	}))
	defer unresponsive.Close()

	// The healthy node responds immediately.
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()

	addrs := []string{unresponsive.Listener.Addr().String(), healthy.Listener.Addr().String()}
	authCfgs := make([]*promauth.Config, len(addrs))
	for i := range authCfgs {
		ac, err := (&promauth.Options{}).NewConfig()
		if err != nil {
			t.Fatalf("cannot create auth config: %s", err)
		}
		authCfgs[i] = ac
	}

	s := NewStorage(addrs, authCfgs, []bool{false, false}, 2, true)
	defer s.MustStop()

	data := &bytesutil.ByteBuffer{}
	data.MustWrite([]byte("log data block"))

	// Saturate the unresponsive node by filling all its in-flight slots with blocked sends.
	snUnresponsive := s.sns[0]
	var wg sync.WaitGroup
	defer func() {
		close(releaseCh)
		wg.Wait()
	}()
	for range cap(snUnresponsive.concurrencyCh) {
		wg.Go(func() {
			_ = snUnresponsive.sendInsertRequest(data)
		})
		select {
		case <-reachedCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for the unresponsive node to become saturated")
		}
	}

	// A further request to the saturated node must be rejected, not block.
	if err := snUnresponsive.sendInsertRequest(data); !errors.Is(err, errConcurrencyLimitReached) {
		t.Fatalf("unexpected error when sending to the saturated node; got %v; want %v", err, errConcurrencyLimitReached)
	}

	// The data must still reach the healthy node while the other node is unresponsive.
	if !s.sendInsertRequestToAnyNode(data) {
		t.Fatalf("cannot send data to any storage node while one node is unresponsive")
	}
}
