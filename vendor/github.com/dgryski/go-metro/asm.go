//go:build ignore

package main

import (
	. "github.com/mmcloughlin/avo/build"
	"github.com/mmcloughlin/avo/buildtags"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

const (
	k0 = 0xD6D018F5
	k1 = 0xA2AA033B
	k2 = 0x62992FC1
	k3 = 0x30BC5B29
)

func advance(p, l Op, c uint64) {
	ADDQ(Imm(c), p)
	SUBQ(Imm(c), l)
}

func imul(k Constant, r Op) {
	t := GP64()
	MOVQ(k, t)
	IMULQ(t, r)
}

func update32(v, p Register, idx uint64, k uint64, vadd Op) {
	r := GP64()
	MOVQ(Mem{Base: p, Disp: int(idx)}, r)
	imul(Imm(k), r)
	ADDQ(r, v)
	RORQ(Imm(29), v)
	ADDQ(vadd, v)
}

func final32(v []GPVirtual, regs []int, keys []uint64) {
	r := GP64()
	MOVQ(v[regs[1]], r)
	ADDQ(v[regs[2]], r)
	imul(Imm(keys[0]), r)
	ADDQ(v[regs[3]], r)
	RORQ(Imm(37), r)
	imul(Imm(keys[1]), r)
	XORQ(r, v[regs[0]])
}

func makeHash64() {
	hash := Load(Param("seed"), GP64())
	buffer := Load(Param("buffer").Base(), GP64())
	bufferLength := Load(Param("buffer").Len(), GP64())

	imul(Imm(k0), hash)
	r := GP64()
	MOVQ(Imm(k2*k0), r)
	ADDQ(r, hash)

	CMPQ(bufferLength, Imm(32))
	JLT(LabelRef("after32"))
	var v [4]GPVirtual
	for i := range v {
		v[i] = GP64()
		MOVQ(hash, v[i])
	}

	Label("loop")
	update32(v[0], buffer, 0, k0, v[2])
	update32(v[1], buffer, 8, k1, v[3])
	update32(v[2], buffer, 16, k2, v[0])
	update32(v[3], buffer, 24, k3, v[1])

	ADDQ(Imm(32), buffer)
	SUBQ(Imm(32), bufferLength)
	CMPQ(bufferLength, Imm(32))
	JGE(LabelRef("loop"))

	final32(v[:], []int{2, 0, 3, 1}, []uint64{k0, k1})
	final32(v[:], []int{3, 1, 2, 0}, []uint64{k1, k0})
	final32(v[:], []int{0, 0, 2, 3}, []uint64{k0, k1})
	final32(v[:], []int{1, 1, 3, 2}, []uint64{k1, k0})

	XORQ(v[1], v[0])
	ADDQ(v[0], hash)

	Label("after32")

	CMPQ(bufferLength, Imm(16))
	JLT(LabelRef("after16"))

	for i := range 2 {
		MOVQ(Mem{Base: buffer}, v[i])
		imul(Imm(k2), v[i])
		ADDQ(hash, v[i])

		advance(buffer, bufferLength, 8)

		RORQ(Imm(29), v[i])
		imul(Imm(k3), v[i])
	}

	r = GP64()
	MOVQ(v[0], r)
	imul(Imm(k0), r)
	RORQ(Imm(21), r)
	ADDQ(v[1], r)
	XORQ(r, v[0])

	MOVQ(v[1], r)
	imul(Imm(k3), r)
	RORQ(Imm(21), r)
	ADDQ(v[0], r)
	XORQ(r, v[1])

	ADDQ(v[1], hash)

	Label("after16")

	CMPQ(bufferLength, Imm(8))
	JLT(LabelRef("after8"))

	r = GP64()
	MOVQ(Mem{Base: buffer}, r)
	imul(Imm(k3), r)
	ADDQ(r, hash)
	advance(buffer, bufferLength, 8)

	MOVQ(hash, r)
	RORQ(Imm(55), r)
	imul(Imm(k1), r)
	XORQ(r, hash)

	Label("after8")

	CMPQ(bufferLength, Imm(4))
	JLT(LabelRef("after4"))

	r = GP64()
	XORQ(r, r)
	MOVL(Mem{Base: buffer}, r.As32())
	imul(Imm(k3), r)
	ADDQ(r, hash)
	advance(buffer, bufferLength, 4)

	MOVQ(hash, r)
	RORQ(Imm(26), r)
	imul(Imm(k1), r)
	XORQ(r, hash)

	Label("after4")

	CMPQ(bufferLength, Imm(2))
	JLT(LabelRef("after2"))

	r = GP64()
	XORQ(r, r)
	MOVW(Mem{Base: buffer}, r.As16())
	imul(Imm(k3), r)
	ADDQ(r, hash)
	advance(buffer, bufferLength, 2)

	MOVQ(hash, r)
	RORQ(Imm(48), r)
	imul(Imm(k1), r)
	XORQ(r, hash)

	Label("after2")

	CMPQ(bufferLength, Imm(1))
	JLT(LabelRef("after1"))

	r = GP64()
	MOVBQZX(Mem{Base: buffer}, r)
	imul(Imm(k3), r)
	ADDQ(r, hash)

	MOVQ(hash, r)
	RORQ(Imm(37), r)
	imul(Imm(k1), r)
	XORQ(r, hash)

	Label("after1")

	r = GP64()
	MOVQ(hash, r)
	RORQ(Imm(28), r)
	XORQ(r, hash)

	imul(Imm(k0), hash)

	MOVQ(hash, r)
	RORQ(Imm(29), r)
	XORQ(r, hash)

	Store(hash, ReturnIndex(0))
	RET()
}

func main() {
	Constraints(
		buildtags.And(
			buildtags.Term("amd64"),
			buildtags.Term("gc"),
			buildtags.Not("purego"),
			buildtags.Not("noasm"),
		),
	)

	TEXT("Hash64", NOSPLIT, "func(buffer []byte, seed uint64) uint64")
	Pragma("noescape")
	makeHash64()

	TEXT("Hash64Str", NOSPLIT, "func(buffer string, seed uint64) uint64")
	makeHash64()

	Generate()
}
