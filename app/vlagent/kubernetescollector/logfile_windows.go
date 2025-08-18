//go:build windows

package kubernetescollector

import (
	"os"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

func getInode(_ os.FileInfo) uint64 {
	logger.Panicf("vlagent does not support collecting logs from Kubernetes on Windows")
	return 0
}
