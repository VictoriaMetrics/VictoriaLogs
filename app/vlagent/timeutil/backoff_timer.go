package timeutil

import (
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
)

// BackoffTimer implements an exponential backoff timer with jitter.
type BackoffTimer struct {
	min     time.Duration
	max     time.Duration
	current time.Duration

	timer *time.Timer
}

// NewBackoffTimer returns a new BackoffTimer initialized with the given minDelay and maxDelay.
// The caller must call Stop() when the BackoffTimer is no longer needed.
func NewBackoffTimer(minDelay, maxDelay time.Duration) BackoffTimer {
	return BackoffTimer{
		min:     minDelay,
		max:     maxDelay,
		current: minDelay,
	}
}

// Wait sleeps for the current delay with jitter, doubling the delay for the next Wait.
// Use CurrentDelay to get the current backoff duration.
//
// Wait returns false if stopCh is closed.
func (bt *BackoffTimer) Wait(stopCh <-chan struct{}) bool {
	v := timeutil.AddJitterToDuration(bt.current)
	bt.current *= 2
	if bt.current > bt.max {
		bt.current = bt.max
	}

	if bt.timer == nil {
		bt.timer = timerpool.Get(v)
	} else {
		bt.timer.Reset(v)
	}

	select {
	case <-stopCh:
		bt.timer.Stop()
		return false
	case <-bt.timer.C:
		return true
	}
}

// CurrentDelay returns the current backoff duration.
func (bt *BackoffTimer) CurrentDelay() time.Duration {
	return bt.current
}

// SetDelay overrides the current delay. Useful for respecting Retry-After headers.
func (bt *BackoffTimer) SetDelay(d time.Duration) {
	if d < bt.min {
		d = bt.min
	}
	if d > bt.max {
		d = bt.max
	}
	bt.current = d
}

// Reset sets the backoff delay to its minimum.
func (bt *BackoffTimer) Reset() {
	bt.current = bt.min
}

// Stop releases internal resources.
func (bt *BackoffTimer) Stop() {
	if bt.timer != nil {
		timerpool.Put(bt.timer)
		bt.timer = nil
	}
}
