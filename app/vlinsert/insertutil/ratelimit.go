package insertutil

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
)

// rateLimiter limits the per-second rate of the registered resources.
//
// It is based on a budget, which is replenished by perSecondLimit every second.
type rateLimiter struct {
	// perSecondLimit is the per-second limit of resources.
	perSecondLimit int64

	// limitReached is incremented every time the limit is reached.
	limitReached *metrics.Counter

	// mu protects budget and deadline from concurrent access.
	mu sync.Mutex

	// budget is the number of resources, which may be registered before the deadline.
	budget int64

	// deadline is the time when the budget is replenished by perSecondLimit.
	deadline time.Time
}

// newRateLimiter returns rate limiter with the given perSecondLimit.
//
// It returns nil if perSecondLimit <= 0, e.g. if the rate limiting is disabled.
// Methods of the returned limiter are safe to call on the nil limiter - they do nothing in this case.
func newRateLimiter(perSecondLimit int64, limitReached *metrics.Counter) *rateLimiter {
	if perSecondLimit <= 0 {
		return nil
	}
	return &rateLimiter{
		perSecondLimit: perSecondLimit,
		limitReached:   limitReached,
	}
}

// replenishLocked replenishes rl.budget if the deadline has been reached.
//
// It must be called under locked rl.mu.
func (rl *rateLimiter) replenishLocked(now time.Time) {
	if now.Before(rl.deadline) {
		return
	}

	// Add the per-second limit to the budget instead of overwriting it,
	// so the debt accumulated by the previously registered resources isn't lost.
	rl.budget += rl.perSecondLimit
	if rl.budget > rl.perSecondLimit {
		// Do not allow accumulating the budget for more than a single second,
		// so the ingestion cannot burst above the configured limit after an idle period.
		rl.budget = rl.perSecondLimit
	}
	rl.deadline = now.Add(time.Second)
}

// hasBudget returns true if resources may be registered at rl without exceeding the per-second limit.
//
// hasBudget doesn't block and doesn't consume the budget.
// It increments rl.limitReached if the per-second limit is already exceeded.
func (rl *rateLimiter) hasBudget() bool {
	if rl == nil {
		return true
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.replenishLocked(time.Now())
	if rl.budget > 0 {
		return true
	}
	rl.limitReached.Inc()
	return false
}

// register registers n resources at rl.
//
// It blocks until the per-second limit allows registering the resources.
func (rl *rateLimiter) register(n int64) {
	if rl == nil || n <= 0 {
		return
	}

	for {
		rl.mu.Lock()
		now := time.Now()
		rl.replenishLocked(now)
		if rl.budget > 0 {
			// The budget may go below zero if n exceeds the remaining budget.
			// This is OK - the debt is subtracted from the budget on the next replenishment.
			// This also guarantees that a single batch bigger than perSecondLimit doesn't block forever.
			rl.budget -= n
			rl.mu.Unlock()
			return
		}
		d := rl.deadline.Sub(now)
		rl.mu.Unlock()

		// Wait for the budget replenishment outside the lock,
		// so concurrent hasBudget() calls could return without blocking.
		rl.limitReached.Inc()
		if d > 0 {
			time.Sleep(d)
		}
	}
}

// logsRateLimiter and bytesRateLimiter limit the global data ingestion rate.
//
// They are nil if the corresponding limits are disabled.
// They are set by InitRateLimiters() before the data ingestion starts.
var (
	logsRateLimiter  *rateLimiter
	bytesRateLimiter *rateLimiter
)

// InitRateLimiters initializes the global data ingestion rate limiters
// according to -insert.maxLogsPerSecond and -insert.maxBytesPerSecond command-line flags.
//
// This function must be called after the command-line flags are parsed
// and before using RegisterIngestedData and IsIngestRateLimitExceeded from this package.
func InitRateLimiters() {
	logsRateLimiter = newRateLimiter(*maxLogsPerSecond, rateLimitReachedLogs)
	if logsRateLimiter != nil {
		logger.Infof("applying %d log entries per second data ingestion rate limit according to -insert.maxLogsPerSecond", *maxLogsPerSecond)
	}

	bytesRateLimiter = newRateLimiter(maxBytesPerSecond.N, rateLimitReachedBytes)
	if bytesRateLimiter != nil {
		logger.Infof("applying %d bytes per second data ingestion rate limit according to -insert.maxBytesPerSecond", maxBytesPerSecond.N)
	}
}

var (
	rateLimitReachedLogs  = metrics.NewCounter(`vl_insert_rate_limit_reached_total{type="logs"}`)
	rateLimitReachedBytes = metrics.NewCounter(`vl_insert_rate_limit_reached_total{type="bytes"}`)

	_ = metrics.NewGauge(`vl_insert_rate_limit{type="logs"}`, func() float64 { return float64(*maxLogsPerSecond) })
	_ = metrics.NewGauge(`vl_insert_rate_limit{type="bytes"}`, func() float64 { return float64(maxBytesPerSecond.N) })
)

// RegisterIngestedData registers the given number of ingested rows and bytes at the global ingestion rate limiters.
//
// It blocks until the limits set via -insert.maxLogsPerSecond and -insert.maxBytesPerSecond allow ingesting the data.
// It returns immediately if both limits are disabled.
//
// See also IsIngestRateLimitExceeded.
func RegisterIngestedData(rows, bytes int) {
	logsRateLimiter.register(int64(rows))
	bytesRateLimiter.register(int64(bytes))
}

// IsIngestRateLimitExceeded returns true if the limits set via -insert.maxLogsPerSecond
// or -insert.maxBytesPerSecond are already exceeded at the moment.
//
// It doesn't block. It is intended for rejecting new data ingestion requests early
// instead of throttling them, e.g. for returning HTTP 429 to HTTP-based data ingestion protocols.
//
// It always returns false if both limits are disabled.
func IsIngestRateLimitExceeded() bool {
	return !logsRateLimiter.hasBudget() || !bytesRateLimiter.hasBudget()
}
