package logstorage

import (
	"math/rand/v2"
	"testing"
)

func BenchmarkInitBloomFilter(b *testing.B) {
	// 4096 distinct tokens in a column block
	r := rand.New(rand.NewPCG(1, 2))
	tokenHashes := make([]uint64, 4096)
	for i := range tokenHashes {
		tokenHashes[i] = r.Uint64()
	}
	hashes := appendHashesHashes(nil, tokenHashes)

	b.ReportAllocs()
	b.SetBytes(int64(len(hashes)))
	b.RunParallel(func(pb *testing.PB) {
		// The size is derived from len(hashes), as in bloomFilter.mustInitHashes, so the size isn't
		// a compile-time constant. With a constant size, the compiler could turn the previous
		// h % maxBits code into a mask after inlining, and the benchmark wouldn't reflect production.
		bits := make([]uint64, len(hashes)/bloomFilterHashesCount*bloomFilterBitsPerItem/64)
		for pb.Next() {
			clear(bits)
			initBloomFilter(bits, hashes)
		}
	})
}
