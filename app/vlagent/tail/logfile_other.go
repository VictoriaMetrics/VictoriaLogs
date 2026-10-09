//go:build !unix

package tail

import (
	"os"
	"runtime"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

func getInode(_ os.FileInfo) uint64 {
	logger.Panicf("vlagent does not support collecting logs from files on %q", runtime.GOOS)
	return 0
}

// getNlink returns number of hard links of given fi.
func getNlink(_ os.FileInfo) uint64 {
	logger.Panicf("vlagent does not support collecting logs from files on %q", runtime.GOOS)
	return 0
}
