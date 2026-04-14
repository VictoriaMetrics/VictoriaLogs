//go:build !windows

package tail

import (
	"os"
	"syscall"
)

func getInode(fi os.FileInfo) uint64 {
	return fi.Sys().(*syscall.Stat_t).Ino
}
