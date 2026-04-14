package logstorage

import (
	"testing"
)

func TestFilterDayRange(t *testing.T) {
	t.Parallel()

	timestamps := []int64{
		1,
		9,
		123,
		456,
		789,
	}

	// match
	ft := newFilterDayRange(0, 1, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0})

	ft = newFilterDayRange(0, 10, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0, 1})

	ft = newFilterDayRange(1, 1, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0})

	ft = newFilterDayRange(1, 1, -8, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{1})

	ft = newFilterDayRange(10, 10, 9, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{0})

	ft = newFilterDayRange(2, 456, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{1, 2, 3})

	ft = newFilterDayRange(2, 457, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{1, 2, 3})

	ft = newFilterDayRange(120, 788, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{2, 3})

	ft = newFilterDayRange(120, 789, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{2, 3, 4})

	ft = newFilterDayRange(120, 10000, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{2, 3, 4})

	ft = newFilterDayRange(789, 1000, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, []int{4})

	// mismatch
	ft = newFilterDayRange(1, 1, -10, "")
	testFilterMatchForTimestamps(t, timestamps, ft, nil)

	ft = newFilterDayRange(0, 1000, -10_000, "")
	testFilterMatchForTimestamps(t, timestamps, ft, nil)

	ft = newFilterDayRange(790, 1000, 0, "")
	testFilterMatchForTimestamps(t, timestamps, ft, nil)
}
