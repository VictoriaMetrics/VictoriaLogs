package logstorage

import (
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/metrics"
)

type memoryLimiter struct {
	MaxSize uint64

	mu    sync.Mutex
	usage uint64
}

func (ml *memoryLimiter) Get(n uint64) bool {
	ml.mu.Lock()
	ok := n <= ml.MaxSize && ml.MaxSize-n >= ml.usage
	if ok {
		ml.usage += n
	}
	ml.mu.Unlock()

	if !ok {
		// Count every denied reservation. A single rejected query may bump this more than once,
		// since its workers keep trying to reserve until cancellation stops them.
		queryMemoryLimitReached.Inc()
	}
	return ok
}

func (ml *memoryLimiter) getUsage() uint64 {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	return ml.usage
}

func (ml *memoryLimiter) Put(n uint64) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if n > ml.usage {
		logger.Panicf("BUG: n=%d cannot exceed %d", n, ml.usage)
	}
	ml.usage -= n
}

var (
	queryMemoryLimiter     memoryLimiter
	queryMemoryLimiterOnce sync.Once
)

var (
	queryMemoryLimitReached = metrics.NewCounter(`vl_query_memory_limit_reached_total`)

	_ = metrics.NewGauge(`vl_query_memory_limit_bytes`, func() float64 {
		return float64(getQueryMemoryLimiter().MaxSize)
	})
	_ = metrics.NewGauge(`vl_query_memory_usage_bytes`, func() float64 {
		return float64(getQueryMemoryLimiter().getUsage())
	})
)

func getQueryMemoryLimiter() *memoryLimiter {
	queryMemoryLimiterOnce.Do(func() {
		queryMemoryLimiter.MaxSize = uint64(float64(memory.Allowed()) * 0.5)
	})
	return &queryMemoryLimiter
}

// memoryReservation tracks the memory reserved from the global query memory limiter.
//
// reserve can be called from concurrently running goroutines.
type memoryReservation struct {
	usage atomic.Uint64
}

func (mr *memoryReservation) reserve(n uint64) bool {
	if !getQueryMemoryLimiter().Get(n) {
		return false
	}
	mr.usage.Add(n)
	return true
}

// release releases all the memory reserved via mr.
func (mr *memoryReservation) release() {
	getQueryMemoryLimiter().Put(mr.usage.Swap(0))
}
