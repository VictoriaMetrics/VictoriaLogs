//go:build !(amd64 && goexperiment.simd && go1.27)

package logstorage

// initHashesHashesSIMD returns 0, since SIMD isn't available in this build.
//
// See the amd64 version for details.
func initHashesHashesSIMD(_, _ []uint64) int {
	return 0
}
