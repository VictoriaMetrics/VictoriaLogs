package logstorage

import (
	"math/bits"
)

// fastMod computes h % d without a division instruction.
//
// It uses the precomputed m = ceil(2^128 / d), as in Lemire, Kaser, Kurz,
// "Faster remainder by direct computation" (2019): h % d = ((m*h mod 2^128) * d) >> 128.
// The result is exactly h % d for every 64-bit h and every d > 1.
//
// A 64-bit DIV costs tens of cycles, while mod needs 3 multiplications.
// It pays off when many values are divided by the same d, as in initBloomFilter.
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
