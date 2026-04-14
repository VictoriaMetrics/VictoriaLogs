package filecollector

import (
	"flag"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlagent/remotewrite"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlagent/tail"
)

var (
	glob = flagutil.NewArrayString("fileCollector.glob", "Glob pattern for log files to collect. Can be specified multiple times. "+
		"The pattern must match files, not directories. "+
		`Example: -fileCollector.glob="/var/log/my_app/*.log"`)
	excludeGlob = flagutil.NewArrayString("fileCollector.excludeGlob", "Glob pattern for log files to exclude from collection. Can be specified multiple times. "+
		`Example: -fileCollector.excludeGlob="/var/log/my_app/*.gz"`)
	checkpointsPath = flag.String("fileCollector.checkpointsPath", "", "Path to the file where vlagent stores its read position for each collected file. "+
		"By default, stored in the directory specified by -tmpDataPath. "+
		"Example: -fileCollector.checkpointsPath=/var/lib/vlagent/file-checkpoints.json")

	refreshInterval = flag.Duration("fileCollector.refreshInterval", time.Second*10, "How often vlagent checks for new files matching the glob pattern")
)

var stopCh = make(chan struct{})
var wg sync.WaitGroup

func Init(tmpDataPath string) {
	if len(*glob) == 0 {
		return
	}

	if *checkpointsPath == "" {
		*checkpointsPath = filepath.Join(tmpDataPath, "vlagent-file-checkpoints.json")
	}

	// Ensure glob patterns are valid.
	for _, pattern := range *glob {
		_, err := path.Match(pattern, ".")
		if err != nil {
			logger.Panicf("FATAL: cannot start fileCollector: invalid glob pattern %q: %s", pattern, err)
		}
	}
	for _, pattern := range *excludeGlob {
		_, err := path.Match(pattern, ".")
		if err != nil {
			logger.Panicf("FATAL: cannot start fileCollector: invalid exclude glob pattern %q: %s", pattern, err)
		}
	}

	if *refreshInterval < time.Second*1 {
		logger.Warnf("fileCollector.refreshInterval=%q too small, setting to 1 second", *refreshInterval)
		*refreshInterval = time.Second * 1
	}

	initTenantIDs()
	initExtraFields()
	initHostname()

	tailer = tail.Start(*checkpointsPath)

	wg.Go(func() {
		for i, pattern := range *glob {
			processGlob(i, pattern)
		}

		ticker := time.NewTicker(*refreshInterval)
		for {
			select {
			case <-stopCh:
				ticker.Stop()
				return
			case <-ticker.C:
				for i, pattern := range *glob {
					processGlob(i, pattern)
				}
			}
		}
	})
}

func processGlob(argIdx int, pattern string) {
	if pattern == "" {
		return
	}

	// Handle regular paths as a special case with verbose logging for better UX,
	// as fs.Glob and filepath.Glob ignore I/O errors.
	if !isGlob(pattern) {
		startRead(argIdx, pattern)
		return
	}

	matches, err := filepath.Glob(pattern)
	if err != nil {
		// Pattern must be valid since we validate it in the Init function.
		logger.Panicf("BUG: pattern %q should be valid; got: %s", pattern, err)
	}
	for _, f := range matches {
		startRead(argIdx, f)
	}
}

var tailer *tail.Tailer
var storage = &remotewrite.Storage{}

func startRead(argIdx int, filePath string) {
	if tailer.IsTailing(filePath) {
		return
	}

	if excludePattern := excludeGlob.GetOptionalArg(argIdx); excludePattern != "" {
		if ok, _ := filepath.Match(excludePattern, filePath); ok {
			return
		}
	}

	if filepath.Ext(filePath) == ".gz" {
		logger.Warnf("skipping gzipped file %q; vlagent does not support reading archived files", filePath)
		return
	}

	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Warnf("cannot start reading logs from file %q: file does not exist", filePath)
			return
		}
		if os.IsPermission(err) {
			logger.Warnf("cannot start reading logs from file %q: permission denied", filePath)
			return
		}
		logger.Errorf("cannot open file %q: %s", filePath, err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		logger.Warnf("cannot stat file: %s", err)
		return
	}
	if fi.IsDir() {
		suggestedPath := filepath.Join(filePath, "*.log")
		logger.Warnf("cannot start reading logs from file %q: is a directory; probably you meant %q", filePath, suggestedPath)
		return
	}

	proc := newProcessor(argIdx, filePath, storage)
	tailer.StartRead(filePath, proc)
}

func Stop() {
	if len(*glob) == 0 {
		return
	}
	close(stopCh)
	wg.Wait()
	tailer.Stop()
}

func isGlob(pattern string) bool {
	// See https://github.com/golang/go/blob/e87b10ea2a2c6c65b80c4374af42b9c02ac9fb20/src/path/filepath/match.go#L352
	if runtime.GOOS == "windows" {
		return strings.ContainsAny(pattern, "*?[")
	}
	return strings.ContainsAny(pattern, `*?[\`)
}
