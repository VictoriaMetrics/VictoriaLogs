//go:build goexperiment.simd && go1.27

package logstorage

import (
	"math/rand/v2"
	"slices"
	"testing"

	"simd/archsimd"
)

func TestAppendHashesHashesSIMD(t *testing.T) {
	if !archsimd.X86.AVX512() {
		t.Skip("the CPU doesn't support AVX-512")
	}
	r := rand.New(rand.NewPCG(7, 8))
	for n := range 70 {
		hashes := make([]uint64, n)
		for i := range hashes {
			hashes[i] = r.Uint64()
		}
		if n := initHashesHashesSIMD(make([]uint64, n*bloomFilterHashesCount), hashes); n != len(hashes)-len(hashes)%4 {
			t.Fatalf("SIMD processed %d hashes out of %d", n, len(hashes))
		}
		got := appendHashesHashes(nil, hashes)
		want := appendHashesHashesReference(nil, hashes)
		if !slices.Equal(got, want) {
			t.Fatalf("unexpected hashes for %d input hashes\ngot\n%X\nwant\n%X", n, got, want)
		}
	}
}

func BenchmarkAppendHashesHashesSIMD(b *testing.B) {
	defer func(v bool) {
		bloomFilterSIMD512 = v
	}(bloomFilterSIMD512)

	b.Run("off", func(b *testing.B) {
		bloomFilterSIMD512 = false
		BenchmarkAppendHashesHashes(b)
	})
	if archsimd.X86.AVX512() {
		b.Run("512", func(b *testing.B) {
			bloomFilterSIMD512 = true
			BenchmarkAppendHashesHashes(b)
		})
	}
}
