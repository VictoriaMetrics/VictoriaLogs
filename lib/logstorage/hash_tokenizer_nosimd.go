//go:build !(amd64 && goexperiment.simd && go1.27)

package logstorage

// hashTokenizerSIMDState is empty, since SIMD isn't available in this build.
type hashTokenizerSIMDState struct{}

// tokenizeStringSIMD always returns false, since SIMD isn't available in this build.
//
// It is defined on hashTokenizerSIMDState, so it is promoted to hashTokenizer.
func (*hashTokenizerSIMDState) tokenizeStringSIMD(dst []uint64, _ string) ([]uint64, bool) {
	return dst, false
}
