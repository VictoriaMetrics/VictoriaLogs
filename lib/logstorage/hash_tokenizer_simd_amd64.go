//go:build goexperiment.simd && go1.27

package logstorage

import (
	"math/bits"
	"simd/archsimd"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
)

// tokenizeStringSIMD tokenizes ASCII strings in two passes:
//
//  1. Every 64 bytes of s are classified with SIMD instructions into a uint64,
//     where bit i is set if s[i] is a token char. Non-ASCII bytes are detected in the same pass.
//  2. m ^ (m<<1) has a bit set at every token start and at every token end,
//     so token boundaries are obtained by walking its set bits with bits.TrailingZeros64.
//
// It returns false if s contains non-ASCII chars or if SIMD isn't supported by the CPU.
// In this case s must be tokenized by the scalar code.
func (t *hashTokenizer) tokenizeStringSIMD(dst []uint64, s string) ([]uint64, bool) {
	b := bytesutil.ToUnsafeBytes(s)
	var ok bool
	switch hashTokenizerSIMDMode {
	case hashTokenizerSIMD512:
		t.masks, ok = appendTokenMasks512(t.masks[:0], b)
	case hashTokenizerSIMD128:
		t.masks, ok = appendTokenMasks128(t.masks[:0], b)
	default:
		return dst, false
	}
	if !ok {
		return dst, false
	}
	return t.tokenizeMasks(dst, s, t.masks), true
}

const (
	hashTokenizerSIMDOff = iota

	// hashTokenizerSIMD128 classifies 16 bytes per step with PSHUFB nibble lookups.
	hashTokenizerSIMD128

	// hashTokenizerSIMD512 classifies 64 bytes per step with a single VPERMI2B lookup.
	hashTokenizerSIMD512
)

// hashTokenizerSIMDMode is the SIMD implementation used by tokenizeStringSIMD.
//
// It may be changed in tests.
var hashTokenizerSIMDMode = func() int {
	if archsimd.X86.AVX512VBMI() {
		return hashTokenizerSIMD512
	}
	if archsimd.X86.AVX() {
		return hashTokenizerSIMD128
	}
	return hashTokenizerSIMDOff
}()

// simdTokenCharTable is the ASCII part of tokenCharTable with 0xFF for token chars.
//
// It is split into two 64-byte halves, which are loaded into two 512-bit registers for VPERMI2B.
var simdTokenCharTable [128]uint8

// simdTokenCharNibbleLo and simdTokenCharNibbleHi are 16-entry lookup tables for PSHUFB.
//
// c is a token char if simdTokenCharNibbleLo[c&0xF] & simdTokenCharNibbleHi[c>>4] != 0.
var simdTokenCharNibbleLo, simdTokenCharNibbleHi [16]uint8

func init() {
	for c := range 128 {
		if isTokenChar(byte(c)) {
			simdTokenCharTable[c] = 0xFF
		}
	}

	// Every high nibble h has a set of low nibbles, which form token chars with it.
	// Assign a distinct bit to every distinct set. Then simdTokenCharNibbleHi[h] contains the bit for the set of h,
	// while simdTokenCharNibbleLo[l] contains the bits for all the sets containing l.
	// High nibbles 8..15 (non-ASCII chars) have no token chars.
	var sets []uint16
	for h := range 8 {
		var set uint16
		for l := range 16 {
			if isTokenChar(byte(h<<4 | l)) {
				set |= 1 << l
			}
		}
		if set == 0 {
			continue
		}
		k := 0
		for k < len(sets) && sets[k] != set {
			k++
		}
		if k == len(sets) {
			sets = append(sets, set)
		}
		if k >= 8 {
			panic("BUG: token chars do not fit PSHUFB nibble lookup tables")
		}
		simdTokenCharNibbleHi[h] = 1 << k
		for l := range 16 {
			if set&(1<<l) != 0 {
				simdTokenCharNibbleLo[l] |= 1 << k
			}
		}
	}
}

// appendTokenMasks512 appends token char masks for b to masks and returns the result.
//
// It returns false if b contains non-ASCII chars.
//
// Every 64 bytes are classified with a single VPERMI2B, which looks up the lower 7 bits of every byte
// in simdTokenCharTable held in two 512-bit registers.
func appendTokenMasks512(masks []uint64, b []byte) ([]uint64, bool) {
	lo := archsimd.LoadUint8x64Array((*[64]uint8)(simdTokenCharTable[:64]))
	hi := archsimd.LoadUint8x64Array((*[64]uint8)(simdTokenCharTable[64:]))
	var zero archsimd.Uint8x64
	var zeroInt archsimd.Int8x64
	var nonASCII uint64
	for len(b) > 0 {
		v, n := archsimd.LoadUint8x64Part(b)
		nonASCII |= v.AsInt8x64().Less(zeroInt).ToBits()
		masks = append(masks, lo.ConcatPermute(hi, v).NotEqual(zero).ToBits())
		b = b[n:]
	}
	return masks, nonASCII == 0
}

// appendTokenMasks128 appends token char masks for b to masks and returns the result.
//
// It returns false if b contains non-ASCII chars.
//
// Every 16 bytes are classified with two PSHUFB lookups - one per nibble. Four steps fill a single uint64 mask.
func appendTokenMasks128(masks []uint64, b []byte) ([]uint64, bool) {
	loTable := archsimd.LoadUint8x16Array(&simdTokenCharNibbleLo)
	hiTable := archsimd.LoadUint8x16Array(&simdTokenCharNibbleHi)
	lowNibble := archsimd.BroadcastUint8x16(0x0F)
	var zero archsimd.Uint8x16
	var zeroInt archsimd.Int8x16
	var nonASCII uint16
	for len(b) > 0 {
		var m uint64
		for shift := 0; shift < 64 && len(b) > 0; shift += 16 {
			v, n := archsimd.LoadUint8x16Part(b)
			lo := v.And(lowNibble)
			// There is no 8-bit shift on amd64, so shift 16-bit lanes and drop the bits from the neighbor byte.
			hi := v.ReshapeToUint16s().ShiftAllRight(4).ReshapeToUint8s().And(lowNibble)
			c := loTable.PermuteOrZero(lo.AsInt8x16()).And(hiTable.PermuteOrZero(hi.AsInt8x16()))
			m |= uint64(c.NotEqual(zero).ToBits()) << shift
			nonASCII |= zeroInt.Greater(v.AsInt8x16()).ToBits()
			b = b[n:]
		}
		masks = append(masks, m)
	}
	return masks, nonASCII == 0
}

// tokenizeMasks registers tokens from s according to the token char masks for s.
//
// Mask bits past len(s) must be zero.
func (t *hashTokenizer) tokenizeMasks(dst []uint64, s string, masks []uint64) []uint64 {
	// Transitions alternate between token start and token end, so process them in pairs.
	// Only a token crossing the boundary between masks leaves a pending start.
	start := -1
	var carry uint64
	for i, m := range masks {
		tr := m ^ (m<<1 | carry)
		carry = m >> 63
		offset := i * 64
		if start >= 0 && tr != 0 {
			end := offset + bits.TrailingZeros64(tr)
			tr &= tr - 1
			if h, ok := t.addToken(s[start:end]); ok {
				dst = append(dst, h)
			}
			start = -1
		}
		for tr != 0 {
			tokenStart := offset + bits.TrailingZeros64(tr)
			tr &= tr - 1
			if tr == 0 {
				start = tokenStart
				break
			}
			end := offset + bits.TrailingZeros64(tr)
			tr &= tr - 1
			if h, ok := t.addToken(s[tokenStart:end]); ok {
				dst = append(dst, h)
			}
		}
	}
	if start >= 0 {
		// The last token ends at the end of s, which is a multiple of 64.
		if h, ok := t.addToken(s[start:]); ok {
			dst = append(dst, h)
		}
	}
	return dst
}
