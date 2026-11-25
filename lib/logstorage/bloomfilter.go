package logstorage

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"
)

// bloomFilterHashesCount is the number of different hashes to use for bloom filter.
const bloomFilterHashesCount = 6

// bloomFilterBitsPerItem is the number of bits to use per each token.
const bloomFilterBitsPerItem = 16

// bloomFilterMarshalTokens appends marshaled bloom filter for tokens to dst and returns the result.
func bloomFilterMarshalTokens(dst []byte, tokens []string) []byte {
	bf := getBloomFilter()
	bf.mustInitTokens(tokens)
	dst = bf.marshal(dst)
	putBloomFilter(bf)
	return dst
}

// bloomFilterMarshalHashes appends marshaled bloom filter for hashes to dst and returns the result.
func bloomFilterMarshalHashes(dst []byte, hashes []uint64) []byte {
	bf := getBloomFilter()
	bf.mustInitHashes(hashes)
	dst = bf.marshal(dst)
	putBloomFilter(bf)
	return dst
}

type bloomFilter struct {
	bits []uint64
}

func (bf *bloomFilter) reset() {
	clear(bf.bits)
	bf.bits = bf.bits[:0]
}

// marshal appends marshaled bf to dst and returns the result.
func (bf *bloomFilter) marshal(dst []byte) []byte {
	bits := bf.bits
	for _, word := range bits {
		dst = encoding.MarshalUint64(dst, word)
	}
	return dst
}

// unmarshal unmarshals bf from src.
func (bf *bloomFilter) unmarshal(src []byte) error {
	if len(src)%8 != 0 {
		return fmt.Errorf("cannot unmarshal bloomFilter from src with size not multiple by 8; len(src)=%d", len(src))
	}
	bf.reset()
	wordsCount := len(src) / 8
	bits := slicesutil.SetLength(bf.bits, wordsCount)
	for i := range bits {
		bits[i] = encoding.UnmarshalUint64(src)
		src = src[8:]
	}
	bf.bits = bits
	return nil
}

// mustInitTokens initializes bf with the given tokens
func (bf *bloomFilter) mustInitTokens(tokens []string) {
	bitsCount := len(tokens) * bloomFilterBitsPerItem
	wordsCount := (bitsCount + 63) / 64
	bits := slicesutil.SetLength(bf.bits, wordsCount)
	bloomFilterAddTokens(bits, tokens)
	bf.bits = bits
}

// mustInitHashes initializes bf with the given hashes
func (bf *bloomFilter) mustInitHashes(hashes []uint64) {
	bitsCount := len(hashes) * bloomFilterBitsPerItem
	wordsCount := (bitsCount + 63) / 64
	bits := slicesutil.SetLength(bf.bits, wordsCount)
	bloomFilterAddHashes(bits, hashes)
	bf.bits = bits
}

// bloomFilterAddTokens adds the given tokens to the bloom filter bits
func bloomFilterAddTokens(bits []uint64, tokens []string) {
	hashesCount := len(tokens) * bloomFilterHashesCount
	a := encoding.GetUint64s(hashesCount)
	a.A = appendTokensHashes(a.A[:0], tokens)
	initBloomFilter(bits, a.A)
	encoding.PutUint64s(a)
}

// bloomFilterAddHashes adds the given haehs to the bloom filter bits
func bloomFilterAddHashes(bits, hashes []uint64) {
	hashesCount := len(hashes) * bloomFilterHashesCount
	a := encoding.GetUint64s(hashesCount)
	a.A = appendHashesHashes(a.A[:0], hashes)
	initBloomFilter(bits, a.A)
	encoding.PutUint64s(a)
}

func initBloomFilter(bits, hashes []uint64) {
	if len(bits) == 0 {
		return
	}
	fm := newFastMod(uint64(len(bits)) * 64)
	for _, h := range hashes {
		idx := fm.mod(h)
		bits[idx/64] |= 1 << (idx % 64)
	}
}

// fastMod computes h % d without a division instruction.
//
// It uses the precomputed m = ceil(2^128 / d), as in Lemire, Kaser, Kurz,
// "Faster remainder by direct computation" (2019): h % d = ((m*h mod 2^128) * d) >> 128.
// The result is exactly h % d for every 64-bit h and every d > 1.
//
// A 64-bit DIV costs tens of cycles, and initBloomFilter does one per hash.
type fastMod struct {
	mHi, mLo uint64
	d        uint64
}

// newFastMod returns fastMod for the given d, which must be bigger than 1.
func newFastMod(d uint64) fastMod {
	// m = floor((2^128 - 1) / d) + 1, which equals ceil(2^128 / d) for d > 1
	mHi, r := bits.Div64(0, ^uint64(0), d)
	mLo, _ := bits.Div64(r, ^uint64(0), d)
	mLo++
	if mLo == 0 {
		mHi++
	}
	return fastMod{mHi: mHi, mLo: mLo, d: d}
}

// mod returns h % fm.d.
func (fm *fastMod) mod(h uint64) uint64 {
	// lowbits = m*h mod 2^128
	hi, lowLo := bits.Mul64(fm.mLo, h)
	lowHi := fm.mHi*h + hi
	// (lowbits * d) >> 128
	carry, _ := bits.Mul64(lowLo, fm.d)
	resHi, resLo := bits.Mul64(lowHi, fm.d)
	_, c := bits.Add64(resLo, carry, 0)
	return resHi + c
}

// appendTokensHashes appends hashes for the given tokens to dst and returns the result.
//
// The appended hashes can be then passed to bloomFilter.containsAll().
func appendTokensHashes(dst []uint64, tokens []string) []uint64 {
	dstLen := len(dst)
	hashesCount := len(tokens) * bloomFilterHashesCount

	dst = slicesutil.SetLength(dst, dstLen+hashesCount)
	dst = dst[:dstLen]

	for _, token := range tokens {
		h := xxhash.Sum64(bytesutil.ToUnsafeBytes(token))
		for k := range uint64(bloomFilterHashesCount) {
			dst = append(dst, hashUint64(h+k))
		}
	}
	return dst
}

// appendHashesHashes appends hashes for the given hashes to dst and returns the result.
//
// The hashes must be generated from tokens by tokenizeHashes().
// See also appendTokensHashes().
//
// The appended hashes can be then passed to bloomFilter.containsAll().
func appendHashesHashes(dst, hashes []uint64) []uint64 {
	dstLen := len(dst)
	hashesCount := len(hashes) * bloomFilterHashesCount

	dst = slicesutil.SetLength(dst, dstLen+hashesCount)
	out := dst[dstLen:]

	for _, h := range hashes {
		o := out[:bloomFilterHashesCount]
		for k := range o {
			o[k] = hashUint64(h + uint64(k))
		}
		out = out[bloomFilterHashesCount:]
	}
	return dst
}

// hashUint64 returns xxhash.Sum64 of the 8 bytes of v in the native byte order.
//
// It is the hash that the bloom filter has always applied to the 8 bytes of v in
// memory, written out for 8-byte inputs so it inlines and needs no buffer.
func hashUint64(v uint64) uint64 {
	if isBigEndian {
		// xxhash reads the bytes as a little-endian number
		v = bits.ReverseBytes64(v)
	}
	h := uint64(xxhPrime5 + 8)
	h ^= bits.RotateLeft64(v*xxhPrime2, 31) * xxhPrime1
	h = bits.RotateLeft64(h, 27)*xxhPrime1 + xxhPrime4
	h ^= h >> 33
	h *= xxhPrime2
	h ^= h >> 29
	h *= xxhPrime3
	h ^= h >> 32
	return h
}

// The xxHash64 primes, see https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md
const (
	xxhPrime1 = 11400714785074694791
	xxhPrime2 = 14029467366897019727
	xxhPrime3 = 1609587929392839161
	xxhPrime4 = 9650029242287828579
	xxhPrime5 = 2870177450012600261
)

var isBigEndian = binary.NativeEndian.Uint16([]byte{0, 1}) == 1

// containsAll returns true if bf contains all the given tokens hashes generated by appendTokensHashes or appendHashesHashes
func (bf *bloomFilter) containsAll(hashes []uint64) bool {
	bits := bf.bits
	if len(bits) == 0 {
		return true
	}
	maxBits := uint64(len(bits)) * 64
	for _, h := range hashes {
		idx := h % maxBits
		i := idx / 64
		j := idx % 64
		mask := uint64(1) << j
		w := bits[i]
		if (w & mask) == 0 {
			// The token is missing
			return false
		}
	}
	return true
}

func getBloomFilter() *bloomFilter {
	v := bloomFilterPool.Get()
	if v == nil {
		return &bloomFilter{}
	}
	return v.(*bloomFilter)
}

func putBloomFilter(bf *bloomFilter) {
	bf.reset()
	bloomFilterPool.Put(bf)
}

var bloomFilterPool sync.Pool
