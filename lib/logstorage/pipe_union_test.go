package logstorage

import (
	"testing"
)

func TestParsePipeUnionSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`union (*)`)
	f(`union (foo)`)
	f(`union (foo | union (bar | stats count(*) as x))`)

	// inline rows
	f(`union rows({})`)
	f(`union rows({"foo":"bar","baz":"123"},{"q":"w"})`)
}

func TestParsePipeUnionFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`union`)
	f(`union()`)
	f(`union(foo | count)`)
	f(`union (foo) bar`)

	f(`union rows`)
	f(`union rows(`)
}

func TestPipeUnionUpdateNeededFields(t *testing.T) {
	f := func(s string, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f("union (abc)", "*", "", "*", "")

	// all the needed fields, non-empty unneeded fields
	f("union (abc)", "*", "f1,f2", "*", "f1,f2")

	// non-empty needed fields
	f("union (abc)", "f1,f2", "", "f1,f2", "")
}
