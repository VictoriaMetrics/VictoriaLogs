package logstorage

import (
	"testing"
	"time"
)

func TestFilterWeekRange(t *testing.T) {
	t.Parallel()

	sunday := time.Date(2024, 6, 9, 1, 0, 0, 0, time.UTC).UnixNano()
	timestamps := []int64{
		sunday,
		sunday + 1*nsecsPerDay,
		sunday + 2*nsecsPerDay,
		sunday + 4*nsecsPerDay,
		sunday + 6*nsecsPerDay,
	}

	// match
	ft := newFilterWeekRange(time.Sunday, time.Sunday, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0})

	ft = newFilterWeekRange(time.Sunday, time.Monday, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0, 1})

	ft = newFilterWeekRange(time.Monday, time.Monday, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{1})

	ft = newFilterWeekRange(time.Monday, time.Monday, -3*nsecsPerDay, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{3})

	ft = newFilterWeekRange(time.Monday, time.Monday, 2*nsecsPerDay, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{4})

	ft = newFilterWeekRange(time.Sunday, time.Saturday, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0, 1, 2, 3, 4})

	// mismatch
	ft = newFilterWeekRange(time.Friday, time.Friday, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, nil)

	ft = newFilterWeekRange(time.Thursday, time.Thursday, -2*nsecsPerHour, "")
	testFilterMatchForTimestamps(t, timestamps, ft, nil)

	ft = newFilterWeekRange(time.Friday, time.Friday, 1*nsecsPerHour, "")
	testFilterMatchForTimestamps(t, timestamps, ft, nil)
}
