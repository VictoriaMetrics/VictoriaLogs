//go:build !windows

package kubernetescollector

import (
	"os"
	"syscall"
)

func getInode(fi os.FileInfo) uint64 {
	return fi.Sys().(*syscall.Stat_t).Ino
}
