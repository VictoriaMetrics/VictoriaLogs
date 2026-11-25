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
		// The size is derived from len(hashes), as in bloomFilter.mustInitHashes, so the compiler
		// cannot turn h % maxBits into a mask after inlining initBloomFilter, as it could with a constant size.
		bits := make([]uint64, len(hashes)/bloomFilterHashesCount*bloomFilterBitsPerItem/64)
		for pb.Next() {
			clear(bits)
			initBloomFilter(bits, hashes)
		}
	})
}
