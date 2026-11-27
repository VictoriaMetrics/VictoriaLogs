package logstorage

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestFastMod(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	values := []uint64{0, 1, 2, 63, 64, 65, math.MaxUint32, math.MaxUint64 - 1, math.MaxUint64}
	for range 10000 {
		values = append(values, r.Uint64())
	}
	divisors := []uint64{2, 3, 7, 64, 128, 192, 1000, 64 * 1001, 1 << 32, 1<<32 + 1, math.MaxUint64 - 1, math.MaxUint64}
	for range 100 {
		divisors = append(divisors, 64*(1+r.Uint64N(1<<20)), 2+r.Uint64N(math.MaxUint64-1))
	}
	for _, d := range divisors {
		fm := newFastMod(d)
		vs := append(values, d-1, d, d+1, 2*d-1, 2*d, math.MaxUint64-math.MaxUint64%d)
		for _, v := range vs {
			if got, want := fm.mod(v), v%d; got != want {
				t.Fatalf("%d %% %d: got %d; want %d", v, d, got, want)
			}
		}
	}
}

func TestInitBloomFilter(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	hashes := make([]uint64, 6000)
	for i := range hashes {
		hashes[i] = r.Uint64()
	}
	for _, words := range []int{1, 2, 3, 7, 16, 100, 1000, 1023, 1024, 1500} {
		got := make([]uint64, words)
		initBloomFilter(got, hashes)
		want := make([]uint64, words)
		maxBits := uint64(words) * 64
		for _, h := range hashes {
			idx := h % maxBits
			want[idx/64] |= 1 << (idx % 64)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("unexpected bloom filter with %d words", words)
		}
	}
	initBloomFilter(nil, nil)
}
