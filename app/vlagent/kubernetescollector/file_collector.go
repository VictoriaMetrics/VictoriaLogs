package kubernetescollector

import (
	"errors"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// processor processes log lines from a single file.
// Log lines can be accumulated within a single file without committing them to the checkpointsDB.
type processor interface {
	// tryAddLine processes a log line and returns true if it should be committed to the checkpointsDB.
	// Returns true if the current line should be committed to checkpointsDB, false otherwise.
	//
	// This allows accumulating multiple lines within a file before committing, which is useful for:
	// - Multi-line log entries that span across lines.
	// - Batching multiple log lines for efficiency.
	// - Custom log parsing that needs context from multiple lines.
	//
	// Note: when a log file is rotated, no checkpoint will be written until tryAddLine returns true,
	// ensuring log entries spanning multiple files are handled correctly.
	tryAddLine(line []byte) bool

	// mustClose releases all resources associated with the processor and ensures proper cleanup of internal states.
	// It must be called after the target log file is deleted or vlagent is shutting down.
	mustClose()
}

type fileCollector struct {
	logFiles     map[string]struct{}
	logFilesLock sync.RWMutex

	newProcessor func(commonFields []logstorage.Field) processor

	checkpointsDB *checkpointsDB

	wg     sync.WaitGroup
	stopCh chan struct{}
}

// startFileCollector starts watching for new logs in a given directory.
// The caller must call stop() when the fileCollector is no longer needed.
//
// The fileCollector maintains a checkpoint file that serves as persistent state storage.
// This allows resuming log reading from the exact position where it was interrupted
// when vlagent is restarted, preventing duplication.
func startFileCollector(checkpointsPath string, newProcessor func(commonFields []logstorage.Field) processor) *fileCollector {
	checkpointsDB, err := startCheckpointsDB(checkpointsPath)
	if err != nil {
		logger.Panicf("FATAL: cannot start checkpoints DB: %s", err)
	}

	c := &fileCollector{
		logFiles:      make(map[string]struct{}),
		newProcessor:  newProcessor,
		checkpointsDB: checkpointsDB,
		stopCh:        make(chan struct{}),
	}

	c.continueFromCheckpoints()

	return c
}

func (fc *fileCollector) startRead(filepath string, commonFields []logstorage.Field) {
	fc.logFilesLock.RLock()
	_, ok := fc.logFiles[filepath]
	fc.logFilesLock.RUnlock()
	if ok {
		// Already reading from the file.
		return
	}

	fc.wg.Add(1)
	go func() {
		defer fc.wg.Done()

		lf := newLogFile(filepath, commonFields)

		fc.logFilesLock.Lock()
		if _, ok := fc.logFiles[filepath]; ok {
			// Already reading from the file.
			fc.logFilesLock.Unlock()
			lf.close()
			return
		}
		fc.logFiles[filepath] = struct{}{}
		fc.logFilesLock.Unlock()

		fc.process(lf)
	}()
}

func (fc *fileCollector) process(lf *logFile) {
	defer lf.close()

	bt := newBackoffTimer(time.Millisecond*100, time.Second*10)
	defer bt.stop()

	proc := fc.newProcessor(lf.commonFields)
	defer proc.mustClose()

	for {
		if needStop(fc.stopCh) {
			return
		}

		ok := lf.readLines(fc.stopCh, proc)
		if ok {
			// Some lines were read - update checkpoint and wait before checking again.
			fc.checkpointsDB.set(lf.checkpoint())
			bt.reset()
			bt.wait(fc.stopCh)
			continue
		}

		// No lines read - check the log file status.
		switch lf.status() {
		case logFileStatusNotRotated:
			// No more lines to read and file hasn't rotated - wait before checking again.
			bt.wait(fc.stopCh)
			continue
		case logFileStatusRotated:
			// Ensure all remaining lines are flushed to the rotated file and read from it.
			// Do not use fc.stopCh here to finish reading from the rotated file even if vlagent is shutting down.
			var neverStopCh chan struct{}
			bt.reset()
			bt.wait(neverStopCh)
			if lf.readLines(neverStopCh, proc) {
				// Double-check: if there are still new lines, it's an unexpected situation.
				bt.wait(neverStopCh)
				if lf.readLines(neverStopCh, proc) {
					logger.Panicf("BUG: log file %q was appended after rotation", lf.path)
				}
			}

			if lf.tryReopen() {
				fc.checkpointsDB.set(lf.checkpoint())
			} else {
				// Cannot reopen the file right now - wait before retrying.
				bt.wait(fc.stopCh)
			}
			continue
		case logFileStatusDeleted:
			fc.checkpointsDB.delete(lf.path)

			fc.logFilesLock.Lock()
			delete(fc.logFiles, lf.path)
			fc.logFilesLock.Unlock()

			if lf.tail != nil {
				logger.Panicf("BUG: tail must be empty when the log file no longer exists; got: %q", lf.tail.B)
			}
			return
		default:
			logger.Panicf("BUG: unexpected log file status")
		}
	}
}

func (fc *fileCollector) continueFromCheckpoints() {
	var rotatedFiles []string
	var deletedLogFiles []string

	cps := fc.checkpointsDB.getAll()
	for _, cp := range cps {
		f, inode, ok := openFileWithInode(cp.Path)
		if !ok {
			deletedLogFiles = append(deletedLogFiles, cp.Path)
			continue
		}

		if inode != cp.Inode {
			_ = f.Close()

			// When kubelet rotates log files, it keeps the previous log file uncompressed
			// in the same directory with a different name (typically with a timestamp suffix).
			// We attempt to find this renamed file to continue reading from our last offset.
			// See https://github.com/kubernetes/kubernetes/blob/f794aa12d78f5b1f9579ce8a991a116a99a2c43c/pkg/kubelet/logs/container_log_manager.go#L416
			var ok bool
			f, ok = findRenamedFile(cp.Path, cp.Inode)
			if !ok {
				// Could not find the rotated file with matching inode.
				// This means the file was rotated and potentially removed before we could process it.
				rotatedFiles = append(rotatedFiles, cp.Path)
				continue
			}
		}

		logfile, err := newLogFileFromFile(f, cp.Path, cp.CommonFields)
		if err != nil {
			logger.Panicf("FATAL: cannot create log file: %s", err)
		}

		logfile.setOffset(cp.Offset)

		fc.logFilesLock.Lock()
		fc.logFiles[cp.Path] = struct{}{}
		fc.logFilesLock.Unlock()

		fc.wg.Add(1)
		go func() {
			defer fc.wg.Done()

			fc.process(logfile)
		}()
	}

	if len(rotatedFiles) > 0 {
		logger.Warnf("%d log files were rotated before being fully read; "+
			"this is expected when pod logs rotate faster than the time vlagent was down; "+
			"an example of such file: %q; "+
			"consider increasing --container-log-max-size in the kubelet", len(rotatedFiles), rotatedFiles[0])
	}

	if len(deletedLogFiles) > 0 {
		logger.Warnf("%d log files were deleted before being fully read; "+
			"this is expected if pods were deleted while vlagent was restarting; "+
			"an example of such file: %q", len(deletedLogFiles), deletedLogFiles[0])
	}

	for _, p := range deletedLogFiles {
		fc.checkpointsDB.delete(p)
	}
}

// findRenamedFile looks for a file with the given inode in the same directory as logPath.
func findRenamedFile(logPath string, inode uint64) (*os.File, bool) {
	actualPath := tryResolveSymlink(logPath)

	dir := path.Dir(actualPath)
	des, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false
		}
		logger.Panicf("FATAL: cannot read dir %q: %s", dir, err)
	}

	for _, de := range des {
		if de.IsDir() {
			continue
		}

		fileName := de.Name()
		if strings.HasSuffix(fileName, ".gz") {
			continue
		}

		filePath := path.Join(dir, fileName)
		file, fileInode, ok := openFileWithInode(filePath)
		if !ok {
			continue
		}

		if fileInode == inode {
			return file, true
		}

		_ = file.Close()
	}

	return nil, false
}

func (fc *fileCollector) stop() {
	close(fc.stopCh)
	fc.wg.Wait()
	fc.checkpointsDB.stop()
}

func needStop(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func openFileWithInode(p string) (*os.File, uint64, bool) {
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, false
		}
		logger.Panicf("FATAL: cannot open file %q: %s", p, err)
	}

	fi, err := f.Stat()
	if err != nil {
		logger.Panicf("FATAL: cannot stat file %q: %s", p, err)
	}
	inode := getInode(fi)

	return f, inode, true
}

// tryResolveSymlink resolves symlink to its target path.
// If symlink cannot be resolved (e.g., symlink is not valid), returns the original path.
func tryResolveSymlink(symlink string) string {
	resolvedPath, err := os.Readlink(symlink)
	if err != nil {
		return symlink
	}
	return resolvedPath
}
