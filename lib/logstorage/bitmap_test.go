package logstorage

import (
	"slices"
	"testing"
)

func TestBitmap(t *testing.T) {
	for i := range 100 {
		bitsLen := i

		bm := getBitmap(i)
		if bm.bitsLen != i {
			t.Fatalf("unexpected bits length: %d; want %d", bm.bitsLen, i)
		}

		if !bm.isZero() {
			t.Fatalf("all the bits must be zero for bitmap with %d bits", i)
		}
		if i == 0 && !bm.areAllBitsSet() {
			t.Fatalf("areAllBitsSet() must return true for bitmap with 0 bits")
		}
		if i > 0 && bm.areAllBitsSet() {
			t.Fatalf("areAllBitsSet() must return false on new bitmap with %d bits; %#v", i, bm)
		}
		if n := bm.onesCount(); n != 0 {
			t.Fatalf("unexpected number of set bits; got %d; want %d", n, 0)
		}

		bm.setBits()
		if n := bm.onesCount(); n != i {
			t.Fatalf("unexpected number of set bits; got %d; want %d", n, i)
		}

		// Make sure that all the bits are set.
		nextIdx := 0
		bm.forEachSetBitReadonly(func(idx int) {
			if idx >= i {
				t.Fatalf("index must be smaller than %d", i)
			}
			if idx != nextIdx {
				t.Fatalf("unexpected idx; got %d; want %d", idx, nextIdx)
			}
			nextIdx++
		})
		if nextIdx != bitsLen {
			t.Fatalf("unexpected number of bits set; got %d; want %d", nextIdx, bitsLen)
		}

		if !bm.areAllBitsSet() {
			t.Fatalf("all bits must be set for bitmap with %d bits", i)
		}

		// Clear a part of bits
		bm.forEachSetBit(func(idx int) bool {
			return idx%2 != 0
		})

		if i <= 1 && !bm.isZero() {
			t.Fatalf("bm.isZero() must return true for bitmap with %d bits", i)
		}
		if i > 1 && bm.isZero() {
			t.Fatalf("bm.isZero() must return false, since some bits are set for bitmap with %d bits", i)
		}
		if i == 0 && !bm.areAllBitsSet() {
			t.Fatalf("areAllBitsSet() must return true for bitmap with 0 bits")
		}
		if i > 0 && bm.areAllBitsSet() {
			t.Fatalf("some bits mustn't be set for bitmap with %d bits", i)
		}

		nextIdx = 1
		bm.forEachSetBitReadonly(func(idx int) {
			if idx != nextIdx {
				t.Fatalf("unexpected idx; got %d; want %d", idx, nextIdx)
			}
			nextIdx += 2
		})
		if nextIdx < bitsLen {
			t.Fatalf("unexpected number of bits visited; got %d; want %d", nextIdx, bitsLen)
		}

		// Clear all the bits
		bm.forEachSetBit(func(_ int) bool {
			return false
		})

		if !bm.isZero() {
			t.Fatalf("all the bits must be reset for bitmap with %d bits", i)
		}
		if i == 0 && !bm.areAllBitsSet() {
			t.Fatalf("allAllBitsSet() must return true for bitmap with 0 bits")
		}
		if i > 0 && bm.areAllBitsSet() {
			t.Fatalf("areAllBitsSet() must return false for bitmap with %d bits", i)
		}
		if n := bm.onesCount(); n != 0 {
			t.Fatalf("unexpected number of set bits; got %d; want %d", n, 0)
		}

		bitsCount := 0
		bm.forEachSetBitReadonly(func(_ int) {
			bitsCount++
		})
		if bitsCount != 0 {
			t.Fatalf("unexpected non-zero number of set bits remained: %d", bitsCount)
		}

		// Set bits via setBit() call
		for i := range bitsLen {
			if n := bm.onesCount(); n != i {
				t.Fatalf("unexpected number of ones set; got %d; want %d", n, i)
			}
			if bm.isSetBit(i) {
				t.Fatalf("the bit %d mustn't be set", i)
			}
			bm.setBit(i)
			if !bm.isSetBit(i) {
				t.Fatalf("the bit %d must be set", i)
			}
			if n := bm.onesCount(); n != i+1 {
				t.Fatalf("unexpected number of ones set; got %d; want %d", n, i+1)
			}
		}

		putBitmap(bm)
	}
}

func TestBitmapForEachSetBitClear(t *testing.T) {
	for _, bitsLen := range []int{0, 1, 63, 64, 65, 1000, 1024} {
		for _, step := range []int{1, 2, 7, 64, 100} {
			var bm bitmap
			bm.init(bitsLen)
			var want []int
			for i := 0; i < bitsLen; i += step {
				bm.setBit(i)
				want = append(want, i)
			}
			var got []int
			bm.forEachSetBitClear(func(idx int) {
				got = append(got, idx)
			})
			if !slices.Equal(got, want) {
				t.Fatalf("bitsLen=%d, step=%d: unexpected bits visited\ngot\n%v\nwant\n%v", bitsLen, step, got, want)
			}
			if !bm.isZero() {
				t.Fatalf("bitsLen=%d, step=%d: the bitmap must be cleared", bitsLen, step)
			}
		}
	}
}
