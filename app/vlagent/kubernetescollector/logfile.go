package kubernetescollector

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs/fsutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/cespare/xxhash/v2"
)

// The maximum log line size that VictoriaLogs can accept.
// See https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have
const maxLogLineSize = 2 * 1024 * 1024

type logFile struct {
	path string
	file *os.File

	// inode tracks the inode of the underlying file.
	// It is used to detect file rotations.
	//
	// It is unexpected for multiple log files in the same mount point to have the same inode while vlagent is running,
	// because vlagent keeps the current file open until Kubernetes creates a new log file to handle rotation.
	// See fingerprint to distinguish files with the same inode.
	inode uint64

	// fingerprint contains the file fingerprint.
	// It helps distinguish files with the same inode,
	// which can happen if an inode is reused while vlagent is down.
	fingerprint uint64

	// offset tracks the current read offset in the file.
	offset int64

	// commitInode tracks the inode of the last committed log entry.
	commitInode uint64
	// commitFingerprint contains the last committed fingerprint.
	commitFingerprint uint64
	// commitOffset tracks the offset of the last committed log entry.
	commitOffset int64

	// tail contains the last incomplete line read from the file.
	// Can be truncated if it exceeds maxLineSize.
	tail *bytesutil.ByteBuffer
	// tailSize tracks the actual tail size.
	tailSize int
}

func newLogFile(symlink string) *logFile {
	return &logFile{
		path: symlink,
	}
}

func newLogFileFromFile(f *os.File, fingerprint uint64, symlink string) (*logFile, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot get file info of %q: %w", f.Name(), err)
	}
	inode := getInode(fi)

	lf := newLogFile(symlink)
	lf.file = f
	lf.inode = inode
	lf.commitInode = inode
	lf.fingerprint = fingerprint
	lf.commitFingerprint = fingerprint

	return lf, nil
}

var readByteBufferPool = sync.Pool{
	New: func() any {
		return &bytesutil.ByteBuffer{B: make([]byte, 256*1024)}
	},
}

var (
	readConcurrencyCh    = fsutil.GetConcurrencyCh()
	processConcurrencyCh = make(chan struct{}, cgroup.AvailableCPUs())
)

func (lf *logFile) readLines(stopCh <-chan struct{}, proc processor) bool {
	if lf.file == nil {
		// This happens on the first read attempt.
		// File may not exist in the case of races with Container Runtime or OS.
		if !lf.tryReopen() {
			return false
		}
	}

	readBuf := readByteBufferPool.Get().(*bytesutil.ByteBuffer)
	defer readByteBufferPool.Put(readBuf)

	anyRead := false

	for {
		if needStop(stopCh) {
			return anyRead
		}

		readConcurrencyCh <- struct{}{}
		n, err := lf.file.Read(readBuf.B)
		<-readConcurrencyCh
		if err != nil {
			if err == io.EOF {
				return anyRead
			}
			logger.Panicf("FATAL: cannot read from file %q: %s", lf.path, err)
		}

		if n > 0 {
			anyRead = true
		}

		processConcurrencyCh <- struct{}{}
		lf.processLines(readBuf.B[:n], proc)
		<-processConcurrencyCh

		if n < len(readBuf.B) {
			// Read less than the buffer size.
			// Stop reading for now.
			return anyRead
		}
	}
}

func (lf *logFile) processLines(data []byte, p processor) {
	if len(data) == 0 {
		return
	}

	// Handle incomplete line from the previous read.
	data, tail, ok := lf.tryCompleteTail(data)
	if !ok {
		// Line is not completed yet.
		return
	}

	if len(tail) > 0 {
		lf.addLine(p, tail)
	}

	// Process complete lines.
	for {
		n := bytes.IndexByte(data, '\n')
		if n < 0 {
			break
		}

		line := data[:n]
		data = data[n+1:]

		lf.addLine(p, line)
	}

	// Save the new incomplete line for the next read.
	lf.setTail(data)
}

func (lf *logFile) tryCompleteTail(data []byte) ([]byte, []byte, bool) {
	if lf.tailSize == 0 {
		// Nothing to complete.
		return data, nil, true
	}

	n := bytes.IndexByte(data, '\n')
	if n < 0 {
		// Tail is not finished yet.
		lf.tailSize += len(data)
		if lf.tailSize <= maxLogLineSize {
			lf.tail.B = append(lf.tail.B, data...)
		}
		return nil, nil, false
	}

	tailEnd := data[:n]
	data = data[n+1:]

	lf.tailSize += len(tailEnd)
	if lf.tailSize > maxLogLineSize {
		// Discard the too large log line.
		//
		// This is unexpected in default Kubernetes installations since
		// containerd splits log lines into 16 KiB chunks by default (criLine.partial will be true for such lines).
		// See: https://github.com/containerd/containerd/blob/f37f951f5601b309e3b31fadf66991625370f7ba/docs/cri/config.md?plain=1#L399-L402
		logger.Warnf("log line from file %q with size %d bytes exceeds maximum allowed size of %d MiB",
			lf.path, lf.tailSize, maxLogLineSize/1024/1024)

		if lf.offset == 0 {
			// This is the first line of the current file.
			lf.fingerprint = calcFingerprint(lf.tail.B)
		}
		lf.offset += int64(lf.tailSize + len("\n"))

		lf.tailSize = 0
		lf.tail.B = lf.tail.B[:0]

		return data, nil, true
	}

	lf.tail.B = append(lf.tail.B, tailEnd...)
	tail := lf.tail.B

	lf.tailSize = 0
	lf.tail.B = lf.tail.B[:0]

	return data, tail, true
}

func (lf *logFile) setTail(tail []byte) {
	if lf.tailSize > 0 {
		logger.Panicf("BUG: cannot set tail when previous tail is not empty")
	}

	if len(tail) == 0 {
		if lf.tail != nil {
			tailByteBufferPool.Put(lf.tail)
			lf.tail = nil
		}
		lf.tailSize = 0
		return
	}

	if lf.tail == nil {
		lf.tail = tailByteBufferPool.Get()
	}

	lf.tailSize = len(tail)
	lf.tail.B = append(lf.tail.B[:0], tail...)
}

var tailByteBufferPool bytesutil.ByteBufferPool

func (lf *logFile) addLine(p processor, line []byte) {
	if lf.offset == 0 {
		// This is the first line of the current file.
		lf.fingerprint = calcFingerprint(line)
	}

	ok, err := p.tryAddLine(line)
	if err != nil {
		// Do not use lf.path here, since the file could be opened using findRenamedFile;
		// in this case lf.path will point to a rotated file.
		filePath := lf.file.Name()
		actualPath := tryResolveSymlink(filePath)
		logger.Panicf("FATAL: cannot process line from file %q (inode %d) with offset %d: %s", actualPath, lf.inode, lf.offset, err)
	}
	lf.offset += int64(len(line) + len("\n"))
	if ok {
		lf.commitInode = lf.inode
		lf.commitFingerprint = lf.fingerprint
		lf.commitOffset = lf.offset
	}
}

// maxFingerprintDataLen is the maximum length of the first line used to calculate the fingerprint.
// 64 bytes is enough because Container Runtime log lines start with a timestamp with nanosecond precision,
// so different files have unique prefixes.
const maxFingerprintDataLen = 64

func calcFingerprint(data []byte) uint64 {
	if len(data) > maxFingerprintDataLen {
		data = data[:maxFingerprintDataLen]
	}
	h := xxhash.Sum64(data)
	if h == 0 {
		// 0 hash is reserved to indicate no hash calculated.
		h = 1
	}
	return h
}

type logFileStatus byte

const (
	logFileStatusNotRotated logFileStatus = iota
	logFileStatusRotated
	logFileStatusDeleted
)

// status reports the current status of the log file.
func (lf *logFile) status() logFileStatus {
	if !symlinkExists(lf.path) {
		// The symlink itself does not exist.
		return logFileStatusDeleted
	}

	stat, exists := mustStat(lf.path)
	if !exists {
		// The symlink exists, but the target file does not.
		// Treat the file as not rotated because it can be appended to during rotation.
		return logFileStatusNotRotated
	}

	newInode := getInode(stat)
	if lf.inode == newInode {
		return logFileStatusNotRotated
	}
	if stat.Size() == 0 {
		// The file has been created, but Container Runtime hasn't switched to it yet.
		return logFileStatusNotRotated
	}

	return logFileStatusRotated
}

func (lf *logFile) setOffset(offset int64) {
	if lf.fingerprint == 0 {
		logger.Panicf("BUG: cannot set offset when no fingerprint is set")
	}

	lf.offset = offset
	if _, err := lf.file.Seek(offset, io.SeekStart); err != nil {
		logger.Panicf("FATAL: cannot seek to offset %d in file %q: %s", offset, lf.file.Name(), err)
	}

	lf.commitInode = lf.inode
	lf.commitFingerprint = lf.fingerprint
	lf.commitOffset = offset
}

func (lf *logFile) tryReopen() bool {
	newFile, newInode, exists := openFileWithInode(lf.path)
	if !exists {
		return false
	}

	lf.close()

	lf.file = newFile
	lf.fingerprint = 0
	lf.inode = newInode
	lf.offset = 0

	return true
}

func (lf *logFile) close() {
	if lf.file == nil {
		return
	}

	_ = lf.file.Close()
	lf.file = nil
}

func (lf *logFile) checkpoint() checkpoint {
	return checkpoint{
		Path:        lf.path,
		Inode:       lf.commitInode,
		Offset:      lf.commitOffset,
		Fingerprint: lf.commitFingerprint,
	}
}

func mustStat(path string) (os.FileInfo, bool) {
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false
		}
		logger.Panicf("FATAL: cannot get file info of %q: %s", path, err)
	}
	return fi, true
}

func symlinkExists(path string) bool {
	_, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		logger.Panicf("FATAL: cannot get symlink info of %q: %s", path, err)
	}
	return true
}
