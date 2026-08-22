package insertutil

import (
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
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

// retryAfter returns the duration to wait until rl.budget becomes positive.
//
// It returns zero duration if resources may be registered at rl right now.
func (rl *rateLimiter) retryAfter() time.Duration {
	if rl == nil {
		return 0
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	rl.replenishLocked(now)
	if rl.budget > 0 {
		return 0
	}

	// The budget is replenished by perSecondLimit at rl.deadline and every second after that,
	// so calculate the number of replenishments needed for paying off the accumulated debt.
	replenishments := 1 + -rl.budget/rl.perSecondLimit
	d := rl.deadline.Sub(now) + time.Duration(replenishments-1)*time.Second
	if d < time.Second {
		d = time.Second
	}
	return d
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

// RejectOnIngestRateLimit responds with the 429 status code and returns true
// if the limits set via -insert.maxLogsPerSecond or -insert.maxBytesPerSecond are already exceeded.
//
// It must be called by HTTP-based data ingestion protocols before processing the ingested data.
// Data ingestion protocols, which do not run on top of HTTP, are throttled at RegisterIngestedData instead,
// since they have no way to report the rate limit back to the client.
//
// It doesn't block and always returns false if both limits are disabled.
func RejectOnIngestRateLimit(w http.ResponseWriter, r *http.Request) bool {
	if logsRateLimiter.hasBudget() && bytesRateLimiter.hasBudget() {
		return false
	}

	d := max(logsRateLimiter.retryAfter(), bytesRateLimiter.retryAfter())
	retryAfterSeconds := int(d.Round(time.Second) / time.Second)
	if retryAfterSeconds < 1 {
		retryAfterSeconds = 1
	}

	// The Retry-After header must be set before writing the response status code.
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	err := &httpserver.ErrorWithStatusCode{
		Err: errors.New("cannot ingest data, since the ingestion rate limit set via -insert.maxLogsPerSecond and/or -insert.maxBytesPerSecond is exceeded; " +
			"retry the request later; see https://docs.victoriametrics.com/victorialogs/data-ingestion/#rate-limiting"),
		StatusCode: http.StatusTooManyRequests,
	}
	httpserver.Errorf(w, r, "%s", err)
	return true
}
