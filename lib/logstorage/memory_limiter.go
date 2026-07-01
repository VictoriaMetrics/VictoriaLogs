package logstorage

import (
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
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
	return ok
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

func getQueryMemoryLimiter() *memoryLimiter {
	queryMemoryLimiterOnce.Do(func() {
		// Allow concurrent queries to use up to 50% of memory.Allowed() for their execution state.
		//
		// The other ~25% of memory.Allowed() goes to subsystems this limiter cannot account for:
		//   - indexdb block caches (lib/mergeset): ~10%. They are capped higher, but VictoriaLogs
		//     keeps the number of streams low, so in practice they stay small.
		//   - in-memory parts buffering freshly ingested logs before they are flushed to disk:
		//     ~10% per active partition (see getMaxInmemoryPartSize).
		//   - per-block scratch buffers for decoding column values during search: ~3%, bounded by
		//     the number of concurrent block searches (see partitionSearchConcurrencyLimitCh).
		//
		// That leaves the live set around 75% of memory.Allowed(). The Go runtime keeps the heap at
		// roughly twice the live set under the default GOGC=100, so a higher query share would risk
		// OOM when heavy queries and ingestion run at the same time.
		queryMemoryLimiter.MaxSize = uint64(float64(memory.Allowed()) * 0.5)
	})
	return &queryMemoryLimiter
}
