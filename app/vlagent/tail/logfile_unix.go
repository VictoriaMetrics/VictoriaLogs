//go:build unix

package tail

import (
	"os"
	"syscall"
)

// getInode returns inode number of given fi.
func getInode(fi os.FileInfo) uint64 {
	return fi.Sys().(*syscall.Stat_t).Ino
}

// getNlink returns number of hard links of given fi.
func getNlink(fi os.FileInfo) uint64 {
	return uint64(fi.Sys().(*syscall.Stat_t).Nlink)
}
