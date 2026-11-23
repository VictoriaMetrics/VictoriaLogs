package logstorage

import (
	"fmt"
	"strings"
	"testing"
)

func TestMemoryLimiter(t *testing.T) {
	var ml memoryLimiter
	ml.MaxSize = 100

	// Allocate memory
	if !ml.Get(10) {
		t.Fatalf("cannot get 10 out of %d bytes", ml.MaxSize)
	}
	if ml.usage != 10 {
		t.Fatalf("unexpected usage; got %d; want %d", ml.usage, 10)
	}

	if !ml.Get(20) {
		t.Fatalf("cannot get 20 out of 90 bytes")
	}
	if ml.usage != 30 {
		t.Fatalf("unexpected usage; got %d; want %d", ml.usage, 30)
	}

	if ml.Get(1000) {
		t.Fatalf("unexpected get for 1000 bytes")
	}
	if ml.usage != 30 {
		t.Fatalf("unexpected usage; got %d; want %d", ml.usage, 30)
	}
	if ml.limitReached != 1 {
		t.Fatalf("unexpected limitReached; got %d; want %d", ml.limitReached, 1)
	}

	if ml.Get(71) {
		t.Fatalf("unexpected get for 71 bytes")
	}
	if ml.usage != 30 {
		t.Fatalf("unexpected usage; got %d; want %d", ml.usage, 30)
	}
	if ml.limitReached != 2 {
		t.Fatalf("unexpected limitReached; got %d; want %d", ml.limitReached, 2)
	}

	if !ml.Get(70) {
		t.Fatalf("cannot get 70 bytes")
	}
	if ml.usage != 100 {
		t.Fatalf("unexpected usage; got %d; want %d", ml.usage, 100)
	}

	// Return memory back
	ml.Put(10)
	ml.Put(70)
	if ml.usage != 20 {
		t.Fatalf("unexpected usage; got %d; want %d", ml.usage, 20)
	}

	if !ml.Get(30) {
		t.Fatalf("cannot get 30 bytes")
	}

	ml.Put(50)
	if ml.usage != 0 {
		t.Fatalf("unexpected usage; got %d; want %d", ml.usage, 0)
	}
}

func TestQueryMemoryReservation(t *testing.T) {
	// This test changes the global query memory limiter, so it must not run in parallel.
	ml := getQueryMemoryLimiter()
	origMaxSize := ml.MaxSize
	ml.MaxSize = 100
	defer func() {
		ml.MaxSize = origMaxSize
	}()

	var mem queryMemoryReservation
	defer mem.release()

	if !mem.reserve(30) {
		t.Fatalf("cannot reserve 30 out of %d bytes", ml.MaxSize)
	}
	if !mem.reserve(70) {
		t.Fatalf("cannot reserve 70 out of 70 bytes")
	}
	if n := ml.getUsage(); n != 100 {
		t.Fatalf("unexpected usage; got %d; want %d", n, 100)
	}
	if mem.failed.Load() {
		t.Fatalf("unexpected failed reservation")
	}

	// A denied reservation must not be counted
	if mem.reserve(1) {
		t.Fatalf("unexpected reservation of 1 byte from the exhausted limiter")
	}
	if n := mem.usage.Load(); n != 100 {
		t.Fatalf("unexpected reserved size; got %d; want %d", n, 100)
	}
	if !mem.failed.Load() {
		t.Fatalf("the denied reservation must be marked as failed")
	}

	mem.release()
	if n := ml.getUsage(); n != 0 {
		t.Fatalf("unexpected usage after release; got %d; want %d", n, 0)
	}
	if !mem.failed.Load() {
		t.Fatalf("release must not reset the failed reservation")
	}
}

func TestQueryMemoryReservationRefill(t *testing.T) {
	// This test changes the global query memory limiter, so it must not run in parallel.
	ml := getQueryMemoryLimiter()
	origMaxSize := ml.MaxSize
	ml.MaxSize = 2 * queryMemoryChunkSize
	defer func() {
		ml.MaxSize = origMaxSize
	}()

	f := func(budget int, resultExpected bool, budgetExpected int, usageExpected uint64) {
		t.Helper()

		var mem queryMemoryReservation
		defer mem.release()

		result := mem.refill(&budget)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
		if budget != budgetExpected {
			t.Fatalf("unexpected budget; got %d; want %d", budget, budgetExpected)
		}
		if n := mem.usage.Load(); n != usageExpected {
			t.Fatalf("unexpected reserved size; got %d; want %d", n, usageExpected)
		}
		if failed := mem.failed.Load(); failed == resultExpected {
			t.Fatalf("unexpected failed state; got %v; want %v", failed, !resultExpected)
		}
	}

	// non-negative budget
	f(0, true, 0, 0)

	// the shortage is rounded up to whole chunks
	f(-1, true, queryMemoryChunkSize-1, queryMemoryChunkSize)
	f(-queryMemoryChunkSize, true, 0, queryMemoryChunkSize)
	f(-queryMemoryChunkSize-1, true, queryMemoryChunkSize-1, 2*queryMemoryChunkSize)

	// nothing is reserved if the limiter cannot provide all the needed chunks
	f(-2*queryMemoryChunkSize-1, false, -2*queryMemoryChunkSize-1, 0)
}

func TestPipesQueryMemoryLimit(t *testing.T) {
	// This test changes the global query memory limiter, so it must not run in parallel.
	ml := getQueryMemoryLimiter()
	origMaxSize := ml.MaxSize
	ml.MaxSize = queryMemoryChunkSize
	defer func() {
		ml.MaxSize = origMaxSize
	}()

	f := func(pipeStr string) {
		t.Helper()

		lex := newLexer(pipeStr, 0)
		p, err := parsePipe(lex)
		if err != nil {
			t.Fatalf("cannot parse [%s]: %s", pipeStr, err)
		}
		pp := p.newPipeProcessor(1, make(chan struct{}), func() {}, newTestPipeProcessor())

		// Make the state of a single worker bigger than the limit.
		for i := range 20 {
			rows := make([][]Field, 2000)
			for j := range rows {
				rows[j] = []Field{{Name: "x", Value: fmt.Sprintf("%0100d", i*len(rows)+j)}}
			}
			var br blockResult
			br.mustInitFromRows(rows)
			pp.writeBlock(0, &br)
		}

		if n := ml.getUsage(); n == 0 {
			t.Fatalf("pipe [%s] must reserve memory before reaching the limit", pipeStr)
		}

		err = pp.flush()
		if err == nil || !strings.Contains(err.Error(), "not enough memory:") {
			t.Fatalf("unexpected error for pipe [%s]; got %v; want the query memory limit error", pipeStr, err)
		}
		if n := ml.getUsage(); n != 0 {
			t.Fatalf("unexpected query memory usage after pipe [%s]; got %d bytes; want 0", pipeStr, n)
		}
	}

	f("stats by (x) count() hits")
	f("running_stats count() hits")
	f("sort by (x)")
	f("sort by (x) limit 100000")
	f("top 10 by (x)")
	f("uniq by (x)")
	f("facets max_values_per_field 100000")
}
