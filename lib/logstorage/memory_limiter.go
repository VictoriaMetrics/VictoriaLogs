package logstorage

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
)

type memoryLimiter struct {
	MaxSize uint64

	mu    sync.Mutex
	usage uint64

	// limitReached is the number of denied Get calls.
	//
	// A single rejected query may increase it more than once,
	// since its workers keep trying to reserve memory until cancellation stops them.
	limitReached uint64
}

func (ml *memoryLimiter) Get(n uint64) bool {
	ml.mu.Lock()
	ok := n <= ml.MaxSize && ml.MaxSize-n >= ml.usage
	if ok {
		ml.usage += n
	} else {
		ml.limitReached++
	}
	ml.mu.Unlock()

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

func getQueryMemoryLimiter() *memoryLimiter {
	queryMemoryLimiterOnce.Do(func() {
		queryMemoryLimiter.MaxSize = uint64(float64(memory.Allowed()) * 0.5)
	})
	return &queryMemoryLimiter
}

// QueryMemoryStats contains stats for the memory used by queries. It may be obtained by calling UpdateQueryMemoryStats().
type QueryMemoryStats struct {
	// MaxSize is the maximum memory size in bytes, which can be reserved by queries.
	MaxSize uint64

	// Usage is the memory size in bytes, which is currently reserved by the running queries.
	Usage uint64

	// LimitReached is the number of times queries couldn't reserve memory because of the MaxSize limit.
	LimitReached uint64
}

// UpdateQueryMemoryStats updates s with the stats for the memory used by queries.
func UpdateQueryMemoryStats(s *QueryMemoryStats) {
	ml := getQueryMemoryLimiter()

	ml.mu.Lock()
	s.MaxSize = ml.MaxSize
	s.Usage = ml.usage
	s.LimitReached = ml.limitReached
	ml.mu.Unlock()
}

// queryMemoryReservation tracks the memory reserved from the global query memory limiter.
//
// reserve and refill can be called concurrently.
type queryMemoryReservation struct {
	usage atomic.Uint64

	// failed is set when reserve fails. release doesn't reset it.
	failed atomic.Bool
}

func (qmr *queryMemoryReservation) reserve(n uint64) bool {
	if !getQueryMemoryLimiter().Get(n) {
		qmr.failed.Store(true)
		return false
	}
	qmr.usage.Add(n)
	return true
}

// Bigger chunks lock the limiter less often, but leave more unused memory reserved per worker.
const queryMemoryChunkSize = 1 << 20

// refill reserves queryMemoryChunkSize chunks until the budget becomes non-negative, or returns false if it cannot.
func (qmr *queryMemoryReservation) refill(budget *int) bool {
	if *budget >= 0 {
		return true
	}

	// Round the negative budget up to whole chunks.
	chunks := (-*budget + queryMemoryChunkSize - 1) / queryMemoryChunkSize
	size := chunks * queryMemoryChunkSize
	if !qmr.reserve(uint64(size)) {
		return false
	}
	*budget += size
	return true
}

// release releases all the memory reserved via qmr.
func (qmr *queryMemoryReservation) release() {
	getQueryMemoryLimiter().Put(qmr.usage.Swap(0))
}

// queryMemoryLimitError returns an error for the case when a query cannot reserve more memory.
func queryMemoryLimitError() error {
	return fmt.Errorf("not enough memory: the running queries may use up to %dMB together; possible solutions: retry the query later; "+
		"if it fails even when it runs alone, then narrow down the time range or add filters, or increase -memory.allowedPercent or -memory.allowedBytes",
		getQueryMemoryLimiter().MaxSize>>20)
}
