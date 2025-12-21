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

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// The maximum log line size that VictoriaLogs can accept.
// See https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have
const maxLogLineSize = 2 * 1024 * 1024

type logFile struct {
	path string
	file *os.File

	// inode tracks the inode of the underlying file.
	inode uint64
	// offset tracks the current read offset in the file.
	offset int64

	// commitInode tracks the inode of the last committed log entry.
	commitInode uint64
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

func newLogFileFromFile(f *os.File, symlink string) (*logFile, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot get file info of %q: %w", f.Name(), err)
	}
	inode := getInode(fi)

	lf := newLogFile(symlink)
	lf.inode = inode
	lf.commitInode = inode
	lf.file = f

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
		lf.offset += int64(len(tail) + len("\n"))

		if p.tryAddLine(tail) {
			lf.commitOffset = lf.offset
			lf.commitInode = lf.inode
		}
	}

	// Process complete lines.
	for {
		n := bytes.IndexByte(data, '\n')
		if n < 0 {
			break
		}

		line := data[:n]
		data = data[n+1:]

		lf.offset += int64(len(line) + len("\n"))

		if p.tryAddLine(line) {
			lf.commitOffset = lf.offset
			lf.commitInode = lf.inode
		}
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
	if lf.file == nil {
		return
	}

	lf.offset = offset
	if _, err := lf.file.Seek(offset, io.SeekStart); err != nil {
		logger.Panicf("FATAL: cannot seek to offset %d in file %q: %s", offset, lf.file.Name(), err)
	}

	lf.commitOffset = offset
	lf.commitInode = lf.inode
}

func (lf *logFile) tryReopen() bool {
	newFile, newInode, exists := openFileWithInode(lf.path)
	if !exists {
		return false
	}

	lf.close()

	lf.file = newFile
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
		Path:   lf.path,
		Inode:  lf.commitInode,
		Offset: lf.commitOffset,
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

func reportLogRowSizeExceeded(streamFields []logstorage.Field, size int) {
	stream := logstorage.MarshalFieldsToJSON(nil, streamFields)
	logger.Warnf("skipping log entry from stream %s: entry size %d bytes exceeds maximum allowed size of %d MiB",
		stream, size, maxLogLineSize/1024/1024)
}
