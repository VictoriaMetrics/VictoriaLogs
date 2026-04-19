package logstorage

import (
	"testing"
)

func TestFilterNot(t *testing.T) {
	t.Parallel()

	columns := []column{
		{
			name: "foo",
			values: []string{
				"a foo",
				"a foobar",
				"aa abc a",
				"ca afdf a,foobar baz",
				"a fddf foobarbaz",
				"",
				"a foobar",
				"a kjlkjf dfff",
				"a ТЕСТЙЦУК НГКШ ",
				"a !!,23.(!1)",
			},
		},
	}

	// match
	fn := newFilterNot(newFilterPhrase("foo", ""))
	testFilterMatchForColumns(t, columns, fn, "foo", []int{0, 1, 2, 3, 4, 6, 7, 8, 9})

	fn = newFilterNot(newFilterPhrase("foo", "a"))
	testFilterMatchForColumns(t, columns, fn, "foo", []int{5})

	fn = newFilterNot(newFilterPhrase("non-existing-field", "foobar"))
	testFilterMatchForColumns(t, columns, fn, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	fn = newFilterNot(newFilterPrefix("non-existing-field", ""))
	testFilterMatchForColumns(t, columns, fn, "foo", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	fn = newFilterNot(newFilterPrefix("foo", ""))
	testFilterMatchForColumns(t, columns, fn, "foo", []int{5})

	// mismatch
	fn = newFilterNot(newFilterPhrase("non-existing-field", ""))
	testFilterMatchForColumns(t, columns, fn, "foo", nil)
}
