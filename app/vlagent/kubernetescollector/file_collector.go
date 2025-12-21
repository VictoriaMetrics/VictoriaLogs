package kubernetescollector

import (
	"errors"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
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
	logFilesLock sync.Mutex

	// excludeFilter defines the criteria for excluding log files from processing.
	// It matches against common metadata fields associated with the log source,
	// such as 'kubernetes.container_name', 'kubernetes.pod_node_name', or 'kubernetes.pod_namespace'.
	//
	// See getCommonFields for the full list of available metadata fields.
	excludeFilter *logstorage.Filter

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
func startFileCollector(checkpointsPath string, excludeFilter *logstorage.Filter, newProcessor func(commonFields []logstorage.Field) processor) *fileCollector {
	checkpointsDB, err := startCheckpointsDB(checkpointsPath)
	if err != nil {
		logger.Panicf("FATAL: cannot start checkpoints DB: %s", err)
	}

	return &fileCollector{
		logFiles:      make(map[string]struct{}),
		excludeFilter: excludeFilter,
		newProcessor:  newProcessor,
		checkpointsDB: checkpointsDB,
		stopCh:        make(chan struct{}),
	}
}

func (fc *fileCollector) startRead(filepath string, commonFields []logstorage.Field) {
	fc.logFilesLock.Lock()
	_, ok := fc.logFiles[filepath]
	fc.logFiles[filepath] = struct{}{}
	fc.logFilesLock.Unlock()
	if ok {
		// Already reading from the file.
		return
	}

	fc.wg.Add(1)
	go func() {
		defer fc.wg.Done()

		lf := fc.openLogFile(filepath)
		fc.process(lf, commonFields)
	}()
}

func (fc *fileCollector) openLogFile(filepath string) *logFile {
	cp, ok := fc.checkpointsDB.get(filepath)
	if !ok {
		// No checkpoint found - start reading from the beginning of the file.
		return newLogFile(filepath)
	}

	lf, ok := tryResumeFromCheckpoint(filepath, cp)
	if !ok {
		fc.checkpointsDB.delete(filepath)
		return newLogFile(filepath)
	}
	return lf
}

func tryResumeFromCheckpoint(filepath string, cp checkpoint) (*logFile, bool) {
	f, inode, ok := openFileWithInode(cp.Path)
	if !ok {
		// The file was deleted just after startRead was called.
		logger.Warnf("log file %q was deleted before being fully read; "+
			"this is expected if the Pod was deleted while vlagent was starting", filepath)
		return nil, false
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
			logger.Warnf("log file %q was rotated before being fully read; "+
				"this is expected when Pod logs rotate faster than the time vlagent was down; "+
				"consider increasing --container-log-max-size in the kubelet", filepath)
			return nil, false
		}
	}

	logfile, err := newLogFileFromFile(f, cp.Path)
	if err != nil {
		logger.Panicf("FATAL: cannot create log file: %s", err)
	}
	logfile.setOffset(cp.Offset)

	return logfile, true
}

func (fc *fileCollector) process(lf *logFile, commonFields []logstorage.Field) {
	defer lf.close()

	if fc.excludeFilter != nil && fc.excludeFilter.MatchRow(commonFields) {
		// Filter matches - skip this file.
		fc.forgetFile(lf.path)
		return
	}

	bt := newBackoffTimer(time.Millisecond*100, time.Second*10)
	defer bt.stop()

	proc := fc.newProcessor(commonFields)
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
			fc.forgetFile(lf.path)

			if lf.tail != nil {
				logger.Panicf("BUG: tail must be empty when the log file no longer exists; got: %q", lf.tail.B)
			}
			return
		default:
			logger.Panicf("BUG: unexpected log file status")
		}
	}
}

// forgetFile removes the given file from the tracking list and deletes its checkpoint.
// It is called when the file is not expected to reappear, so its state no longer needs to be stored.
func (fc *fileCollector) forgetFile(filePath string) {
	fc.checkpointsDB.delete(filePath)

	fc.logFilesLock.Lock()
	defer fc.logFilesLock.Unlock()
	delete(fc.logFiles, filePath)
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

// cleanupCheckpoints removes all checkpoints for files that are no longer being processed.
func (fc *fileCollector) cleanupCheckpoints() {
	unusedCheckpoints := fc.getUnusedCheckpoints()
	if len(unusedCheckpoints) == 0 {
		return
	}

	for _, cp := range unusedCheckpoints {
		fc.checkpointsDB.delete(cp.Path)
	}

	logger.Warnf("%d log files were deleted before being fully read; "+
		"this is expected if Pods were deleted while vlagent was restarting; "+
		"an example of such file: %q", len(unusedCheckpoints), unusedCheckpoints[0].Path)
}

func (fc *fileCollector) getUnusedCheckpoints() []checkpoint {
	cps := fc.checkpointsDB.getAll()

	fc.logFilesLock.Lock()
	defer fc.logFilesLock.Unlock()

	var unused []checkpoint
	for _, cp := range cps {
		if _, ok := fc.logFiles[cp.Path]; ok {
			continue
		}
		unused = append(unused, cp)
	}
	return unused
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
