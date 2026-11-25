package logstorage

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

func TestBloomFilter(t *testing.T) {
	f := func(tokens []string) {
		t.Helper()
		dataTokens := bloomFilterMarshalTokens(nil, tokens)
		hashes := tokenizeHashes(nil, tokens)
		dataHashes := bloomFilterMarshalHashes(nil, hashes)
		if string(dataTokens) != string(dataHashes) {
			t.Fatalf("unexpected marshaled bloom filters from hashes\ngot\n%X\nwant\n%X", dataHashes, dataTokens)
		}

		bf := getBloomFilter()
		defer putBloomFilter(bf)
		if err := bf.unmarshal(dataTokens); err != nil {
			t.Fatalf("unexpected error when unmarshaling bloom filter: %s", err)
		}
		tokensHashes := appendTokensHashes(nil, tokens)
		if !bf.containsAll(tokensHashes) {
			t.Fatalf("containsAll must return true for the added tokens")
		}
	}
	f(nil)
	f([]string{"foo"})
	f([]string{"foo", "bar", "baz"})

	// 10k tokens
	tokens := make([]string, 10000)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("token_%d", i)
	}
	f(tokens)
}

func TestBloomFilterUnmarshalFailure(t *testing.T) {
	f := func(data []byte) {
		t.Helper()
		bf := getBloomFilter()
		defer putBloomFilter(bf)
		if err := bf.unmarshal(data); err == nil {
			t.Fatalf("expecting non-nil error")
		}
	}
	f([]byte("a"))
	f([]byte("foo"))
}

func TestBloomFilterUnmarshalGarbage(t *testing.T) {
	data := []byte("01234567")
	var bf bloomFilter
	if err := bf.unmarshal(data); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestBloomFilterFalsePositive(t *testing.T) {
	tokens := make([]string, 20000)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("token_%d", i)
	}
	data := bloomFilterMarshalTokens(nil, tokens)
	bf := getBloomFilter()
	defer putBloomFilter(bf)
	if err := bf.unmarshal(data); err != nil {
		t.Fatalf("unexpected error when unmarshaling bloom filter: %s", err)
	}

	// count the number of false positives on 20K non-existing tokens
	falsePositives := 0
	for i := range tokens {
		token := fmt.Sprintf("non-existing-token_%d", i)
		tokensHashes := appendTokensHashes(nil, []string{token})
		if bf.containsAll(tokensHashes) {
			falsePositives++
		}
	}
	p := float64(falsePositives) / float64(len(tokens))
	maxFalsePositive := 0.0011
	if p > maxFalsePositive {
		t.Fatalf("too high false positive rate; got %.4f; want %.4f max", p, maxFalsePositive)
	}
}

func TestBloomFilterMarshal_TokensVSHashes(t *testing.T) {
	tokens := make([]string, 100)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("token_%d", i)
	}

	dataTokens := bloomFilterMarshalTokens(nil, tokens)

	hashes := tokenizeHashes(nil, tokens)
	dataHashes := bloomFilterMarshalHashes(nil, hashes)

	if string(dataTokens) != string(dataHashes) {
		t.Fatalf("unexpected bloom filter obtained from hashes\ngot\n%X\nwant\n%X", dataHashes, dataTokens)
	}
}

func TestBloomFilterMarshalTokens(t *testing.T) {
	f := func(tokens []string, resultExpected string) {
		t.Helper()

		result := bloomFilterMarshalTokens(nil, tokens)
		if string(result) != resultExpected {
			t.Fatalf("unexpected result\ngot\n%X\nwant\n%X", result, resultExpected)
		}
	}

	f([]string{}, "")
	f([]string{"foo"}, "\x00\x00\x00\x82\x40\x18\x00\x04")
	f([]string{"foo", "bar", "baz"}, "\x00\x00\x81\xA3\x48\x5C\x10\x26")
	f([]string{"foo", "bar", "baz", "foo"}, "\x00\x00\x81\xA3\x48\x5C\x10\x26")
}

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
		prefix := []uint64{42}
		got := appendHashesHashes(prefix, hashes)
		want := appendHashesHashesReference([]uint64{42}, hashes)
		if !slices.Equal(got, want) {
			t.Fatalf("unexpected hashes for %d input hashes\ngot\n%X\nwant\n%X", n, got, want)
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
