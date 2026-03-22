package logstorage

import (
	"sync/atomic"
	"testing"
)

func TestQueryStats_IsPartial_Default(t *testing.T) {
	qs := &QueryStats{}
	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 0 {
		t.Fatalf("expected default IsPartial to be 0 (false), got %d", isPartial)
	}
}

func TestQueryStats_IsPartial_SetFalse(t *testing.T) {
	qs := &QueryStats{}
	atomic.StoreUint32(&qs.IsPartial, 0)
	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 0 {
		t.Fatalf("expected IsPartial to be 0 (false), got %d", isPartial)
	}
}

func TestQueryStats_IsPartial_SetTrue(t *testing.T) {
	qs := &QueryStats{}
	atomic.StoreUint32(&qs.IsPartial, 1)
	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 1 {
		t.Fatalf("expected IsPartial to be 1 (true), got %d", isPartial)
	}
}

func TestQueryStats_IsPartial_SetUnknown(t *testing.T) {
	qs := &QueryStats{}
	atomic.StoreUint32(&qs.IsPartial, 2)
	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 2 {
		t.Fatalf("expected IsPartial to be 2 (unknown), got %d", isPartial)
	}
}

func TestQueryStats_UpdateAtomic_IsPartial_Propagation(t *testing.T) {
	f := func(srcIsPartial, dstIsPartialBefore, dstIsPartialAfter uint32) {
		t.Helper()

		dst := &QueryStats{}
		atomic.StoreUint32(&dst.IsPartial, dstIsPartialBefore)

		src := &QueryStats{}
		atomic.StoreUint32(&src.IsPartial, srcIsPartial)

		dst.UpdateAtomic(src)

		result := atomic.LoadUint32(&dst.IsPartial)
		if result != dstIsPartialAfter {
			t.Fatalf("UpdateAtomic: src.IsPartial=%d, dst.IsPartial(before)=%d, expected dst.IsPartial(after)=%d, got %d",
				srcIsPartial, dstIsPartialBefore, dstIsPartialAfter, result)
		}
	}

	// Source is full (0), destination is full (0) -> should remain full (0)
	f(0, 0, 0)

	// Source is partial (1), destination is full (0) -> should become partial (1)
	f(1, 0, 1)

	// Source is unknown (2), destination is full (0) -> should become unknown (2)
	f(2, 0, 2)

	// Source is full (0), destination is partial (1) -> should remain partial (1)
	// (once partial, stays partial - we don't downgrade)
	f(0, 1, 1)

	// Source is partial (1), destination is partial (1) -> should remain partial (1)
	f(1, 1, 1)

	// Source is unknown (2), destination is partial (1) -> should become unknown (2)
	f(2, 1, 2)

	// Source is full (0), destination is unknown (2) -> should remain unknown (2)
	f(0, 2, 2)

	// Source is partial (1), destination is unknown (2) -> should become partial (1)
	f(1, 2, 1)

	// Source is unknown (2), destination is unknown (2) -> should remain unknown (2)
	f(2, 2, 2)
}

func TestQueryStats_UpdateAtomic_PreservesOtherFields(t *testing.T) {
	dst := &QueryStats{
		BytesReadColumnsHeaders: 100,
		BlocksProcessed:         10,
		RowsFound:               50,
	}
	atomic.StoreUint32(&dst.IsPartial, 0)

	src := &QueryStats{
		BytesReadColumnsHeaders: 200,
		BlocksProcessed:         20,
		RowsFound:               100,
	}
	atomic.StoreUint32(&src.IsPartial, 1)

	dst.UpdateAtomic(src)

	// Check that other fields were updated
	if dst.BytesReadColumnsHeaders != 300 {
		t.Fatalf("expected BytesReadColumnsHeaders=300, got %d", dst.BytesReadColumnsHeaders)
	}
	if dst.BlocksProcessed != 30 {
		t.Fatalf("expected BlocksProcessed=30, got %d", dst.BlocksProcessed)
	}
	if dst.RowsFound != 150 {
		t.Fatalf("expected RowsFound=150, got %d", dst.RowsFound)
	}

	// Check that IsPartial was updated
	isPartial := atomic.LoadUint32(&dst.IsPartial)
	if isPartial != 1 {
		t.Fatalf("expected IsPartial=1, got %d", isPartial)
	}
}
