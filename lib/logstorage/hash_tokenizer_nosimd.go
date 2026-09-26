//go:build !(amd64 && goexperiment.simd && go1.27)

package logstorage

// tokenizeStringSIMD always returns false, since SIMD isn't available in this build.
func (t *hashTokenizer) tokenizeStringSIMD(dst []uint64, _ string) ([]uint64, bool) {
	return dst, false
}
