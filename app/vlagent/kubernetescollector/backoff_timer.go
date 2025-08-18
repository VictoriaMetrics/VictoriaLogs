package kubernetescollector

import (
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
)

// backoffTimer implements an exponential backoff timer with jitter.
type backoffTimer struct {
	min     time.Duration
	max     time.Duration
	current time.Duration

	timer *time.Timer
}

// newBackoffTimer returns a new backoffTimer initialized with the given minDelay and maxDelay.
// The caller must call stop() when the backoffTimer is no longer needed.
func newBackoffTimer(minDelay, maxDelay time.Duration) backoffTimer {
	return backoffTimer{
		min:     minDelay,
		max:     maxDelay,
		current: minDelay,
	}
}

// wait sleeps for the current delay with jitter, doubling the delay for the next wait.
// Use currentDelay to get the current backoff duration.
func (bt *backoffTimer) wait(stopCh <-chan struct{}) {
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
	case <-bt.timer.C:
	}
}

// currentDelay returns the current backoff duration.
func (bt *backoffTimer) currentDelay() time.Duration {
	return bt.current
}

// reset sets the backoff delay to its minimum.
func (bt *backoffTimer) reset() {
	bt.current = bt.min
}

// stop releases internal resources.
func (bt *backoffTimer) stop() {
	if bt.timer != nil {
		timerpool.Put(bt.timer)
		bt.timer = nil
	}
}
