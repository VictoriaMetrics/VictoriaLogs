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

	// Priority: 1 (partial) > 2 (unknown) > 0 (full)
	// Commutative: order doesn't matter

	// Source is full (0), destination is full (0) -> should remain full (0)
	f(0, 0, 0)

	// Source is partial (1), destination is full (0) -> should become partial (1)
	f(1, 0, 1)

	// Source is unknown (2), destination is full (0) -> should become unknown (2)
	f(2, 0, 2)

	// Source is full (0), destination is partial (1) -> should remain partial (1)
	f(0, 1, 1)

	// Source is partial (1), destination is partial (1) -> should remain partial (1)
	f(1, 1, 1)

	// Source is unknown (2), destination is partial (1) -> should remain partial (1) [partial has higher priority]
	f(2, 1, 1)

	// Source is full (0), destination is unknown (2) -> should remain unknown (2)
	f(0, 2, 2)

	// Source is partial (1), destination is unknown (2) -> should become partial (1) [partial has higher priority]
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

func TestQueryStats_UpdateFromDataBlock_BackwardCompatibility(t *testing.T) {
	// Test that UpdateFromDataBlock works with older payloads that don't have IsPartial field
	qs := &QueryStats{}
	atomic.StoreUint32(&qs.IsPartial, 0)

	// Create a DataBlock without IsPartial field (simulating older version)
	db := &DataBlock{
		columns: []BlockColumn{
			{Name: "BytesReadColumnsHeaders", Values: []string{"100"}},
			{Name: "BytesReadColumnsHeaderIndexes", Values: []string{"50"}},
			{Name: "BytesReadBloomFilters", Values: []string{"30"}},
			{Name: "BytesReadValues", Values: []string{"200"}},
			{Name: "BytesReadTimestamps", Values: []string{"40"}},
			{Name: "BytesReadBlockHeaders", Values: []string{"20"}},
			{Name: "BlocksProcessed", Values: []string{"10"}},
			{Name: "RowsProcessed", Values: []string{"100"}},
			{Name: "RowsFound", Values: []string{"50"}},
			{Name: "ValuesRead", Values: []string{"150"}},
			{Name: "TimestampsRead", Values: []string{"100"}},
			{Name: "BytesProcessedUncompressedValues", Values: []string{"500"}},
			// Note: IsPartial field is missing (backward compatibility)
		},
	}

	err := qs.UpdateFromDataBlock(db)
	if err != nil {
		t.Fatalf("UpdateFromDataBlock should not fail on missing optional IsPartial field: %v", err)
	}

	// Check that stats were updated
	if qs.BytesReadColumnsHeaders != 100 {
		t.Fatalf("expected BytesReadColumnsHeaders=100, got %d", qs.BytesReadColumnsHeaders)
	}

	// Check that IsPartial remains at default value (0) when field is missing
	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 0 {
		t.Fatalf("expected IsPartial=0 when field is missing, got %d", isPartial)
	}
}

func TestQueryStats_UpdateFromDataBlock_WithIsPartial(t *testing.T) {
	// Test that UpdateFromDataBlock correctly merges IsPartial with priority
	qs := &QueryStats{}
	atomic.StoreUint32(&qs.IsPartial, 2) // Start with unknown

	// Create a DataBlock with IsPartial=1 (partial)
	db := &DataBlock{
		columns: []BlockColumn{
			{Name: "BytesReadColumnsHeaders", Values: []string{"100"}},
			{Name: "BytesReadColumnsHeaderIndexes", Values: []string{"50"}},
			{Name: "BytesReadBloomFilters", Values: []string{"30"}},
			{Name: "BytesReadValues", Values: []string{"200"}},
			{Name: "BytesReadTimestamps", Values: []string{"40"}},
			{Name: "BytesReadBlockHeaders", Values: []string{"20"}},
			{Name: "BlocksProcessed", Values: []string{"10"}},
			{Name: "RowsProcessed", Values: []string{"100"}},
			{Name: "RowsFound", Values: []string{"50"}},
			{Name: "ValuesRead", Values: []string{"150"}},
			{Name: "TimestampsRead", Values: []string{"100"}},
			{Name: "BytesProcessedUncompressedValues", Values: []string{"500"}},
			{Name: "IsPartial", Values: []string{"1"}},
		},
	}

	err := qs.UpdateFromDataBlock(db)
	if err != nil {
		t.Fatalf("UpdateFromDataBlock failed: %v", err)
	}

	// Check that IsPartial was updated to 1 (partial has higher priority than unknown)
	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 1 {
		t.Fatalf("expected IsPartial=1 (partial has priority over unknown), got %d", isPartial)
	}
}
