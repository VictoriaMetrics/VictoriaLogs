//go:build goexperiment.simd && go1.27

package logstorage

import (
	"math/rand"
	"reflect"
	"simd/archsimd"
	"strings"
	"testing"
)

func TestTokenizeHashesSIMD(t *testing.T) {
	var inputs [][]string

	// Strings of all the lengths around 16-byte and 64-byte block boundaries.
	rng := rand.New(rand.NewSource(1))
	alphabet := []byte("ab_Z09 .-/:\x00\x7f")
	for n := 0; n <= 200; n++ {
		for range 20 {
			b := make([]byte, n)
			for i := range b {
				if rng.Intn(50) == 0 {
					b[i] = byte(rng.Intn(128))
				} else {
					b[i] = alphabet[rng.Intn(len(alphabet))]
				}
			}
			inputs = append(inputs, []string{string(b)})
		}
	}

	// All the byte values, including non-ASCII ones.
	for c := range 256 {
		inputs = append(inputs, []string{"x" + string(rune(c)) + "y", string([]byte{byte(c), 'q', byte(c)})})
	}

	inputs = append(inputs, strings.Split(benchLogs, "\n"))

	modes := []int{hashTokenizerSIMD128}
	if archsimd.X86.AVX512VBMI() {
		modes = append(modes, hashTokenizerSIMD512)
	}

	defer func(mode int) {
		hashTokenizerSIMDMode = mode
	}(hashTokenizerSIMDMode)

	for _, a := range inputs {
		hashTokenizerSIMDMode = hashTokenizerSIMDOff
		hashesExpected := tokenizeHashes(nil, a)
		for _, mode := range modes {
			hashTokenizerSIMDMode = mode
			hashes := tokenizeHashes(nil, a)
			if !reflect.DeepEqual(hashes, hashesExpected) {
				t.Fatalf("unexpected hashes for SIMD mode %d on %q\ngot\n%X\nwant\n%X", mode, a, hashes, hashesExpected)
			}
		}
	}
}

func BenchmarkTokenizeHashesSIMD(b *testing.B) {
	defer func(mode int) {
		hashTokenizerSIMDMode = mode
	}(hashTokenizerSIMDMode)

	f := func(name string, mode int) {
		b.Run(name, func(b *testing.B) {
			hashTokenizerSIMDMode = mode
			BenchmarkTokenizeHashes(b)
		})
	}

	f("off", hashTokenizerSIMDOff)
	f("128", hashTokenizerSIMD128)
	if archsimd.X86.AVX512VBMI() {
		f("512", hashTokenizerSIMD512)
	}
}
