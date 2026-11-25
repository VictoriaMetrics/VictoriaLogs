package logstorage

import (
	"math/rand/v2"
	"testing"
)

// benchBloomTokens is the number of distinct tokens in a column block of the benchmarks.
const benchBloomTokens = 4096

func benchBloomHashes() []uint64 {
	r := rand.New(rand.NewPCG(1, 2))
	hashes := make([]uint64, benchBloomTokens)
	for i := range hashes {
		hashes[i] = r.Uint64()
	}
	return hashes
}

func BenchmarkAppendHashesHashes(b *testing.B) {
	hashes := benchBloomHashes()
	b.ReportAllocs()
	b.SetBytes(int64(len(hashes)))
	b.RunParallel(func(pb *testing.PB) {
		var dst []uint64
		for pb.Next() {
			dst = appendHashesHashes(dst[:0], hashes)
		}
	})
}

func BenchmarkInitBloomFilter(b *testing.B) {
	hashes := appendHashesHashes(nil, benchBloomHashes())
	b.ReportAllocs()
	b.SetBytes(int64(len(hashes)))
	b.RunParallel(func(pb *testing.PB) {
		// The size is derived from len(hashes), as in bloomFilter.mustInitHashes, so the compiler
		// cannot turn h % maxBits into a mask after inlining initBloomFilter, as it could with a constant size.
		bits := make([]uint64, len(hashes)/bloomFilterHashesCount*bloomFilterBitsPerItem/64)
		for pb.Next() {
			clear(bits)
			initBloomFilter(bits, hashes)
		}
	})
}
