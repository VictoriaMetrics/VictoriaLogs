package insertutil

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/ratelimiter"
	"github.com/VictoriaMetrics/metrics"
)

var (
	logsRateLimiter  *ratelimiter.RateLimiter
	bytesRateLimiter *ratelimiter.RateLimiter

	rateLimiterStopCh chan struct{}
)

var (
	rateLimitReachedLogs  = metrics.NewCounter(`vl_insert_rate_limit_reached_total{type="logs"}`)
	rateLimitReachedBytes = metrics.NewCounter(`vl_insert_rate_limit_reached_total{type="bytes"}`)

	_ = metrics.NewGauge(`vl_insert_rate_limit{type="logs"}`, func() float64 { return float64(*maxLogsPerSecond) })
	_ = metrics.NewGauge(`vl_insert_rate_limit{type="bytes"}`, func() float64 { return float64(maxBytesPerSecond.N) })
)

// InitRateLimiters initializes the data ingestion rate limiters
// according to -insert.maxLogsPerSecond and -insert.maxBytesPerSecond command-line flags.
//
// It must be called after the command-line flags are parsed.
// StopRateLimiters must be called when the data ingestion is stopped.
func InitRateLimiters() {
	rateLimiterStopCh = make(chan struct{})

	if n := *maxLogsPerSecond; n > 0 {
		logsRateLimiter = ratelimiter.New(n, rateLimitReachedLogs, rateLimiterStopCh)
		logger.Infof("applying %d log entries per second data ingestion rate limit according to -insert.maxLogsPerSecond", n)
	}
	if n := maxBytesPerSecond.N; n > 0 {
		bytesRateLimiter = ratelimiter.New(n, rateLimitReachedBytes, rateLimiterStopCh)
		logger.Infof("applying %d bytes per second data ingestion rate limit according to -insert.maxBytesPerSecond", n)
	}
}

// StopRateLimiters unblocks the data ingestion, which waits for the rate limiter budget replenishment.
func StopRateLimiters() {
	close(rateLimiterStopCh)
}
