package logstorage

import (
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
	if ml.Get(71) {
		t.Fatalf("unexpected get for 71 bytes")
	}
	if ml.usage != 30 {
		t.Fatalf("unexpected usage; got %d; want %d", ml.usage, 30)
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
	// This test mutates the process-global query memory limiter, so it cannot run in parallel.
	ml := getQueryMemoryLimiter()
	origMaxSize := ml.MaxSize
	ml.MaxSize = 100
	defer func() {
		ml.MaxSize = origMaxSize
	}()

	var mem queryMemoryReservation
	if !mem.reserve(30) {
		t.Fatalf("cannot reserve 30 out of %d bytes", ml.MaxSize)
	}
	if !mem.reserve(70) {
		t.Fatalf("cannot reserve 70 out of 70 bytes")
	}
	if n := ml.getUsage(); n != 100 {
		t.Fatalf("unexpected usage; got %d; want %d", n, 100)
	}

	// A denied reservation must not be accounted
	if mem.reserve(1) {
		t.Fatalf("unexpected reservation of 1 byte from the exhausted limiter")
	}
	if n := mem.usage.Load(); n != 100 {
		t.Fatalf("unexpected reserved size; got %d; want %d", n, 100)
	}

	mem.release()
	if n := ml.getUsage(); n != 0 {
		t.Fatalf("unexpected usage after release; got %d; want %d", n, 0)
	}
}
