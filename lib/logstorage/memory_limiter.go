package logstorage

import (
	"sync"

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
		// Allow concurrent queries to use up to 50% of memory.Allowed() for their execution state.
		//
		// Notes on other parts of the system that also consume the heap:
		// - indexdb block caches (lib/mergeset): assume ~5%, as VictoriaLogs has much fewer streams than VictoriaMetrics
		// - in-memory parts buffering: ~10% per active partition (usually 1 partition unless it's backfilling)
		// - per-block scratch buffers for decoding column values during search: ~3%
		// Total = 18%
		//
		// The Go runtime keeps the heap at roughly twice the live set under the default GOGC=100,
		// so keeping the peak heap within memory.Allowed() would call for a ~32% query share ((32% + 18%) * 2 = 100%).
		//
		// That 32% is conservative in practice: these subsystems rarely reach their limits at the same time, and the
		// OS page cache (the ~40% of RAM left by -memory.allowedPercent) is evicted under memory pressure, so the
		// peak heap can borrow that headroom without an OOM.
		//
		// We pick 50% instead: before this limiter, a single stateful pipe was already allowed up to 40% of
		// memory.Allowed() (0.4 for stats/uniq/top/running_stats, 0.2 for sort/facets/stream_context/...), so the
		// shared pool must stay comfortably above 40% to avoid rejecting a single heavy pipe that used to succeed.
		queryMemoryLimiter.MaxSize = uint64(float64(memory.Allowed()) * 0.5)
	})
	return &queryMemoryLimiter
}
