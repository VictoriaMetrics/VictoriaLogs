package logstorage

import (
	"encoding/binary"
	"math/bits"
)

// hashUint64 returns xxhash.Sum64 of the 8 bytes of v in the native byte order.
//
// It is the hash that the bloom filter has always applied to the 8 bytes of v in
// memory, written out for 8-byte inputs so it inlines and needs no buffer.
// See the xxh64 algorithm at https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md
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

// The xxHash64 primes.
const (
	xxhPrime1 = 11400714785074694791
	xxhPrime2 = 14029467366897019727
	xxhPrime3 = 1609587929392839161
	xxhPrime4 = 9650029242287828579
	xxhPrime5 = 2870177450012600261
)

var isBigEndian = binary.NativeEndian.Uint16([]byte{0, 1}) == 1
