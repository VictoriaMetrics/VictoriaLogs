//go:build goexperiment.simd && go1.27

package logstorage

import (
	"simd/archsimd"
)

// bloomFilterSIMD512 is true if initHashesHashesSIMD computes the hashes with AVX-512.
//
// It can be changed in tests in order to run the scalar code.
var bloomFilterSIMD512 = archsimd.X86.AVX512()

// initHashesHashesSIMD stores bloomFilterHashesCount hashes for every hash in hashes to out,
// the same as hashUint64(h+k) for k in [0..bloomFilterHashesCount), 8 hashes per step with AVX-512.
//
// It processes hashes in groups of 4 (24 outputs, 3 vectors) and returns the number of processed hashes;
// the caller must compute the remaining ones. It returns 0 if the CPU doesn't support AVX-512.
func initHashesHashesSIMD(out, hashes []uint64) int {
	if !bloomFilterSIMD512 {
		return 0
	}
	const group = 4
	const groupOut = group * bloomFilterHashesCount
	n := len(hashes) - len(hashes)%group

	c := xxh64x8Consts{
		p1: archsimd.BroadcastUint64x8(xxhPrime1),
		p2: archsimd.BroadcastUint64x8(xxhPrime2),
		p3: archsimd.BroadcastUint64x8(xxhPrime3),
		p4: archsimd.BroadcastUint64x8(xxhPrime4),
		h0: archsimd.BroadcastUint64x8(xxhPrime5 + 8),
	}
	var in [groupOut]uint64
	for i := 0; i < n; i += group {
		for j, h := range hashes[i : i+group] {
			for k := range uint64(bloomFilterHashesCount) {
				in[j*bloomFilterHashesCount+int(k)] = h + k
			}
		}
		o := out[i*bloomFilterHashesCount : i*bloomFilterHashesCount+groupOut]
		for v := 0; v < groupOut; v += 8 {
			c.hash(archsimd.LoadUint64x8(in[v:])).Store(o[v:])
		}
	}
	return n
}

type xxh64x8Consts struct {
	p1, p2, p3, p4, h0 archsimd.Uint64x8
}

// hash returns hashUint64 for each of the 8 lanes of v. amd64 is little-endian, so no byte swap is needed.
func (c *xxh64x8Consts) hash(v archsimd.Uint64x8) archsimd.Uint64x8 {
	h := c.h0.Xor(v.Mul(c.p2).RotateAllLeft(31).Mul(c.p1))
	h = h.RotateAllLeft(27).Mul(c.p1).Add(c.p4)
	h = h.Xor(h.ShiftAllRight(33)).Mul(c.p2)
	h = h.Xor(h.ShiftAllRight(29)).Mul(c.p3)
	return h.Xor(h.ShiftAllRight(32))
}
