package insertutil

import (
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func TestNewRateLimiter_Disabled(t *testing.T) {
	f := func(perSecondLimit int64) {
		t.Helper()

		rl := newRateLimiter(perSecondLimit, metrics.GetOrCreateCounter(`test_rate_limit_reached_total{case="disabled"}`))
		if rl != nil {
			t.Fatalf("unexpected non-nil rate limiter for perSecondLimit=%d", perSecondLimit)
		}

		// Methods of the disabled rate limiter must be no-ops.
		if !rl.hasBudget() {
			t.Fatalf("unexpected hasBudget() result; got false; want true")
		}
		startTime := time.Now()
		rl.register(1_000_000)
		if d := time.Since(startTime); d > time.Second {
			t.Fatalf("unexpected register() duration; got %s; want less than 1s", d)
		}
	}

	f(0)
	f(-1)
}

func TestRateLimiter_HasBudget(t *testing.T) {
	f := func(perSecondLimit int64, registerCounts []int64, resultExpected bool) {
		t.Helper()

		rl := newRateLimiter(perSecondLimit, metrics.GetOrCreateCounter(`test_rate_limit_reached_total{case="has_budget"}`))
		for _, count := range registerCounts {
			rl.register(count)
		}

		result := rl.hasBudget()
		if result != resultExpected {
			t.Fatalf("unexpected hasBudget() result; got %v; want %v", result, resultExpected)
		}
	}

	// no resources are registered yet
	f(10, nil, true)

	// the per-second limit isn't reached yet
	f(100, []int64{5, 5, 5}, true)

	// the per-second limit is reached exactly
	f(10, []int64{4, 6}, false)

	// a single batch exceeds the per-second limit
	f(10, []int64{100}, false)
}

func TestRateLimiter_HasBudgetDoesNotConsumeBudget(t *testing.T) {
	rl := newRateLimiter(10, metrics.GetOrCreateCounter(`test_rate_limit_reached_total{case="no_consume"}`))

	for i := range 100 {
		if !rl.hasBudget() {
			t.Fatalf("unexpected hasBudget() result at iteration %d; got false; want true", i)
		}
	}
}

func TestRateLimiter_HasBudgetUpdatesLimitReached(t *testing.T) {
	limitReached := metrics.GetOrCreateCounter(`test_rate_limit_reached_total{case="limit_reached"}`)
	rl := newRateLimiter(10, limitReached)
	rl.register(10)

	nBefore := limitReached.Get()
	if rl.hasBudget() {
		t.Fatalf("unexpected hasBudget() result; got true; want false")
	}
	if n := limitReached.Get(); n != nBefore+1 {
		t.Fatalf("unexpected limitReached counter; got %d; want %d", n, nBefore+1)
	}
}

func TestRateLimiter_RegisterBlocksOnExceededLimit(t *testing.T) {
	rl := newRateLimiter(10, metrics.GetOrCreateCounter(`test_rate_limit_reached_total{case="blocks"}`))

	// Exhaust the budget for the current second.
	rl.register(10)

	// The next register() must block until the budget is replenished.
	startTime := time.Now()
	rl.register(1)
	d := time.Since(startTime)
	if d < 500*time.Millisecond {
		t.Fatalf("unexpected register() duration; got %s; want at least 500ms", d)
	}
	if d > 3*time.Second {
		t.Fatalf("unexpected register() duration; got %s; want less than 3s", d)
	}

	if !rl.hasBudget() {
		t.Fatalf("unexpected hasBudget() result after the budget replenishment; got false; want true")
	}
}

func TestRateLimiter_RegisterDoesNotBlockOnTooBigBatch(t *testing.T) {
	rl := newRateLimiter(10, metrics.GetOrCreateCounter(`test_rate_limit_reached_total{case="big_batch"}`))

	// A single batch bigger than the per-second limit must be registered instead of blocking forever.
	startTime := time.Now()
	rl.register(100)
	if d := time.Since(startTime); d > time.Second {
		t.Fatalf("unexpected register() duration; got %s; want less than 1s", d)
	}
}

func TestRateLimiter_BudgetIsNotAccumulated(t *testing.T) {
	rl := newRateLimiter(10, metrics.GetOrCreateCounter(`test_rate_limit_reached_total{case="no_burst"}`))

	// Emulate an idle period of a few seconds.
	rl.mu.Lock()
	rl.budget = 10
	rl.deadline = time.Now().Add(-5 * time.Second)
	rl.mu.Unlock()

	// The budget must be capped by the per-second limit, so the ingestion cannot burst after the idle period.
	rl.register(10)
	if rl.hasBudget() {
		t.Fatalf("unexpected hasBudget() result; got true; want false")
	}
}
