package logstorage

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
)

func TestHashUint64(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	values := []uint64{0, 1, 2, math.MaxUint32, math.MaxUint64 - 1, math.MaxUint64}
	for range 10000 {
		values = append(values, r.Uint64())
	}
	for _, v := range values {
		// xxhash over the 8 bytes of v in memory, as the bloom filter always hashed them
		b := unsafe.Slice((*byte)(unsafe.Pointer(&v)), 8)
		if got, want := hashUint64(v), xxhash.Sum64(b); got != want {
			t.Fatalf("hashUint64(%d): got %X; want %X", v, got, want)
		}
	}
}

// appendHashesHashesReference is the original appendHashesHashes: xxhash over the 8 bytes of h, h+1, ... in memory.
func appendHashesHashesReference(dst, hashes []uint64) []uint64 {
	var buf [8]byte
	hp := (*uint64)(unsafe.Pointer(&buf[0]))
	for _, h := range hashes {
		*hp = h
		for range bloomFilterHashesCount {
			dst = append(dst, xxhash.Sum64(buf[:]))
			(*hp)++
		}
	}
	return dst
}

func TestAppendHashesHashes(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for n := range 70 {
		hashes := make([]uint64, n)
		for i := range hashes {
			hashes[i] = r.Uint64()
		}
		if n > 0 {
			hashes[0] = math.MaxUint64 - 2 // h+k wraps around
		}
		got := appendHashesHashes([]uint64{42}, hashes)
		want := appendHashesHashesReference([]uint64{42}, hashes)
		if !slices.Equal(got, want) {
			t.Fatalf("unexpected hashes for %d input hashes\ngot\n%X\nwant\n%X", n, got, want)
		}
	}
}

func TestAppendTokensHashes(t *testing.T) {
	tokens := []string{"", "a", "foo", "bar_baz", "1234567890", "привет"}
	hashes := make([]uint64, len(tokens))
	for i, token := range tokens {
		hashes[i] = xxhash.Sum64(bytesutil.ToUnsafeBytes(token))
	}
	got := appendTokensHashes([]uint64{42}, tokens)
	want := appendHashesHashesReference([]uint64{42}, hashes)
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected hashes\ngot\n%X\nwant\n%X", got, want)
	}
}
