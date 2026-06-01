package remotewrite

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envtemplate"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs/fscore"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/persistentqueue"
	"github.com/VictoriaMetrics/metrics"
	"github.com/bmatcuk/doublestar/v4"
	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	remoteWriteURLs = flagutil.NewArrayString("remoteWrite.url", "Remote storage URL to write data to. "+
		"Example url: http://<victorialogs-host>:9428/insert/native. "+
		"Pass multiple -remoteWrite.url options in order to replicate the collected data to multiple remote storage systems. "+
		"See also -remoteWrite.maxDiskUsagePerURL and -remoteWrite.format")
	maxPendingBytesPerURL = flagutil.NewArrayBytes("remoteWrite.maxDiskUsagePerURL", 0, "The maximum file-based buffer size in bytes at -remoteWrite.tmpDataPath "+
		"for each -remoteWrite.url. When buffer size reaches the configured maximum, then old data is dropped when adding new data to the buffer. "+
		"Buffered data is stored in ~500MB chunks. It is recommended to set the value for this flag to a multiple of the block size 500MB. "+
		"Disk usage is unlimited if the value is set to 0")
	format = flagutil.NewArrayString("remoteWrite.format", "The data format to use for sending data to the corresponding -remoteWrite.url. "+
		"Available formats: native, jsonline. Default is native. See https://docs.victoriametrics.com/victorialogs/vlagent/#remote-write-format")

	transforms = flag.String("remoteWrite.transforms", "", "Path to transformations program, which is applied "+
		"to all the logs before sending them to -remoteWrite.url. See also -remoteWrite.urlTransforms. "+
		"The path can be a glob pattern pointing to multiple files, http url or inline content with prefix 'inline:'. "+
		"See https://docs.victoriametrics.com/victorialogs/vlagent/transformations/")
	urlTransforms = flagutil.NewArrayString("remoteWrite.urlTransforms", "Path to transformations program for the corresponding -remoteWrite.url. "+
		"See also -remoteWrite.transforms. The path can be a glob pattern pointing to multiple files, http url or inline content with prefix 'inline:'. "+
		"See https://docs.victoriametrics.com/victorialogs/vlagent/transformations/")

	remoteWriteTmpDataPath = flag.String("remoteWrite.tmpDataPath", "", "Path to directory for storing pending data, which isn't sent to the configured -remoteWrite.url. "+
		"If this flag isn't set, then pending data is stored in the vlagent-remotewrite-data subdirectory under the -tmpDataPath directory; "+
		"see also -remoteWrite.maxDiskUsagePerURL")
	queues = flag.Int("remoteWrite.queues", cgroup.AvailableCPUs()*2, "The number of concurrent queues to each -remoteWrite.url. Set more queues if default number of queues "+
		"isn't enough for sending high volume of collected data to remote storage. "+
		"Default value depends on the number of available CPU cores. It should work fine in most cases since it minimizes resource usage")

	showRemoteWriteURL = flag.Bool("remoteWrite.showURL", false, "Whether to show -remoteWrite.url in the exported metrics. "+
		"It is hidden by default, since it can contain sensitive info such as auth key")
)

// rwctxsGlobal contains statically populated entries when -remoteWrite.url is specified.
var rwctxsGlobal []*remoteWriteCtx

var globalTransformer *logstorage.Transformer

// Storage implements insertutil.LogRowsStorage interface
type Storage struct{}

// MustAddRows implements insertutil.LogRowsStorage interface
func (*Storage) MustAddRows(lr *logstorage.LogRows) {
	if tr := globalTransformer; tr != nil {
		// globalTransformer will flush the result to pushToRemoteStorages.
		tr.Transform(lr)
	} else {
		pushToRemoteStorages(lr)
	}
}

// CanWriteData implements insertutil.LogRowsStorage interface
func (*Storage) CanWriteData() error {
	return nil
}

// maxQueues limits the maximum value for `-remoteWrite.queues`. There is no sense in setting too high value,
// since it may lead to high memory usage due to big number of buffers.
var maxQueues = cgroup.AvailableCPUs() * 16

const persistentQueueDirname = "persistent-queue"

// InitSecretFlags registers secret flags defined under `remotewrite` pkg.
//
// It must be called after flag.Parse and before any logging by main function of an application (e.g. victoria-logs, vlagent).
func InitSecretFlags() {
	if !*showRemoteWriteURL {
		// remoteWrite.url can contain authentication codes, so hide it at `/metrics` output.
		flagutil.RegisterSecretFlag("remoteWrite.url")
	}
	// remoteWrite.proxyURL can contain credentials in the proxy URL, so hide it too.
	flagutil.RegisterSecretFlag("remoteWrite.proxyURL")
	// remoteWrite.headers can contain auth headers such as Authorization and API keys.
	flagutil.RegisterSecretFlag("remoteWrite.headers")
}

// Init initializes remotewrite.
//
// It must be called after flag.Parse().
//
// Stop must be called for graceful shutdown.
func Init(tmpDataPath string) {
	if len(*remoteWriteURLs) == 0 {
		logger.Fatalf("at least one `-remoteWrite.url` command-line flag must be set")
	}
	if *queues > maxQueues {
		*queues = maxQueues
	}
	if *queues <= 0 {
		*queues = 1
	}
	path := *remoteWriteTmpDataPath
	if len(path) == 0 {
		path = filepath.Join(tmpDataPath, "vlagent-remotewrite-data")
	}
	initGlobalTransformer()
	initRemoteWriteCtxs(path, *remoteWriteURLs)
	dropDanglingQueues(path)
}

// Stop stops remotewrite.
//
// It is expected that nobody calls TryPush during and after the call to this func.
func Stop() {
	for _, rwctx := range rwctxsGlobal {
		rwctx.mustStop()
	}
	rwctxsGlobal = nil
}

func initGlobalTransformer() {
	if s := *transforms; s != "" {
		globalTransformer = loadTransforms(s, pushToRemoteStorages)
	}
}

func loadTransforms(s string, flush func(lr *logstorage.LogRows)) *logstorage.Transformer {
	switch {
	case strings.HasPrefix(s, "inline:"):
		content := strings.TrimPrefix(s, "inline:")
		content = envtemplate.ReplaceString(content)
		prog, err := logstorage.ParseTransformsProgram(content)
		if err != nil {
			logger.Fatalf("FATAL: failed to parse inline transformations: %s", err)
		}
		tr := prog.NewTransformer(flush)
		return tr
	case isHTTPURL(s):
		content, err := fscore.ReadFileOrHTTP(s)
		if err != nil {
			logger.Fatalf("FATAL: cannot read transformations: %s", err)
		}
		content = envtemplate.ReplaceBytes(content)
		prog, err := logstorage.ParseTransformsProgram(string(content))
		if err != nil {
			if len(content) > 4*1024 {
				content = content[:4*1024]
				content = append(content, "..."...)
			}
			logger.Fatalf("FATAL: failed to parse transformations by URL %q: %s; content: %q", s, err, content)
		}
		tr := prog.NewTransformer(flush)
		return tr
	default:
		var globOpts = []doublestar.GlobOption{
			// Follow traditional shell glob behavior where `*` or a `?` at the start will not match dotfiles by default.
			// Users can explicitly use `.*` or `.?` syntax to collect logs from the hidden files.
			doublestar.WithNoHidden(),
		}
		files, err := doublestar.FilepathGlob(s, globOpts...)
		if err != nil {
			logger.Fatalf("FATAL: cannot process glob pattern %q: %s", s, err)
		}
		if len(files) == 0 {
			logger.Fatalf("FATAL: no files found by glob pattern %q", s)
		}
		sort.Strings(files)
		filesContent := make([]string, len(files))
		for i, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				logger.Fatalf("FATAL: cannot read file with transformations: %s", err)
			}
			data = envtemplate.ReplaceBytes(data)
			filesContent[i] = string(data)
		}

		// Handle the first file.
		firstFile := files[0]
		files = files[1:]
		firstFileContent := filesContent[0]
		filesContent = filesContent[1:]
		prog, err := logstorage.ParseTransformsProgram(firstFileContent)
		if err != nil {
			logger.Fatalf("FATAL: cannot parse transformations program by path %q: %s", firstFile, err)
		}

		// Parse additional content.
		for i, file := range files {
			content := filesContent[i]
			if err := prog.ParseAdditional(content); err != nil {
				logger.Fatalf("FATAL: cannot parse transformations program by path %q: %s", file, err)
			}
		}

		tr := prog.NewTransformer(flush)
		return tr
	}
}

// isHTTPURL checks if a given targetURL is valid and contains a valid http scheme.
// Copied from fscore.ReadFileOrHTTP.
func isHTTPURL(targetURL string) bool {
	parsed, err := url.Parse(targetURL)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}

func dropDanglingQueues(tmpDataPath string) {
	// Remove dangling persistent queues, if any.
	// This is required for the case when the number of queues has been changed or URL have been changed.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4014
	//
	// In case if there were many persistent queues with identical *remoteWriteURLs
	// the queue with the last index will be dropped.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6140
	existingQueues := make(map[string]struct{}, len(rwctxsGlobal))
	for _, rwctx := range rwctxsGlobal {
		existingQueues[rwctx.fq.Dirname()] = struct{}{}
	}

	queuesDir := filepath.Join(tmpDataPath, persistentQueueDirname)
	files := fs.MustReadDir(queuesDir)
	removed := 0
	for _, f := range files {
		dirname := f.Name()
		if _, ok := existingQueues[dirname]; !ok {
			logger.Infof("removing dangling queue %q", dirname)
			fullPath := filepath.Join(queuesDir, dirname)
			fs.MustRemoveDir(fullPath)
			removed++
		}
	}
	if removed > 0 {
		logger.Infof("removed %d dangling queues from %q, active queues: %d", removed, tmpDataPath, len(rwctxsGlobal))
	}
}

func initRemoteWriteCtxs(tmpDataPath string, urls []string) {
	if len(urls) == 0 {
		logger.Panicf("BUG: urls must be non-empty")
	}
	if len(urls) < len(*urlTransforms) {
		logger.Fatalf("FATAL: the number of specified -remoteWrite.urlTransforms flags (%d) exceeds the number of specified -remoteWrite.url flags (%d); "+
			"use glob patterns to specify multiple transformation files for a single -remoteWrite.url", len(*urlTransforms), len(urls))
	}

	maxInmemoryBlocks := memory.Allowed() / len(urls) / 10000
	if maxInmemoryBlocks / *queues > 100 {
		// There is no much sense in keeping higher number of blocks in memory,
		// since this means that the producer outperforms consumer and the queue
		// will continue growing. It is better storing the queue to file.
		maxInmemoryBlocks = 100 * *queues
	}
	if maxInmemoryBlocks < 2 {
		maxInmemoryBlocks = 2
	}
	rwctxs := make([]*remoteWriteCtx, len(urls))
	for i, remoteWriteURLRaw := range urls {
		remoteWriteURL, err := url.Parse(remoteWriteURLRaw)
		if err != nil {
			logger.Fatalf("invalid -remoteWrite.url=%q: %s", remoteWriteURL, err)
		}
		sanitizedURL := fmt.Sprintf("%d:secret-url", i+1)
		if *showRemoteWriteURL {
			sanitizedURL = fmt.Sprintf("%d:%s", i+1, remoteWriteURL)
		}
		rwctxs[i] = newRemoteWriteCtx(i, remoteWriteURL, maxInmemoryBlocks, sanitizedURL, tmpDataPath)
	}
	fs.RegisterPathFsMetrics(tmpDataPath)

	rwctxsGlobal = rwctxs
}

func pushToRemoteStorages(lr *logstorage.LogRows) {
	rwctxs := rwctxsGlobal
	if len(rwctxs) == 1 {
		// Fast path: there is only one remote storage system.
		rwctxs[0].push(lr)
		return
	}
	// Slow path: push lr to remote storage systems in parallel.
	var wg sync.WaitGroup
	for _, rwctx := range rwctxs {
		wg.Go(func() {
			rwctx.push(lr)
		})
	}
	wg.Wait()
}

type remoteWriteCtx struct {
	fq *persistentqueue.FastQueue
	c  *client

	transformer *logstorage.Transformer

	pls        []*pendingLogs
	plsNextIdx atomic.Uint64
}

func newRemoteWriteCtx(argIdx int, remoteWriteURL *url.URL, maxInmemoryBlocks int, sanitizedURL, tmpDataPath string) *remoteWriteCtx {
	dataFormat := format.GetOptionalArg(argIdx)
	if dataFormat == "" {
		dataFormat = "native"
	}
	switch dataFormat {
	case "native", "jsonline":
	default:
		logger.Fatalf("unsupported -remoteWrite.format=%q; see https://docs.victoriametrics.com/victorialogs/vlagent/#remote-write-format", dataFormat)
	}

	if dataFormat == "native" {
		// Protocol version is required by VictoriaLogs for native data ingestion protocol.
		q := remoteWriteURL.Query()
		q.Set("version", netinsert.ProtocolVersion)
		remoteWriteURL.RawQuery = q.Encode()
	}

	// strip query params, otherwise changing params resets pq
	pqURL := *remoteWriteURL
	pqURL.RawQuery = ""
	pqURL.Fragment = ""
	h := xxhash.Sum64([]byte(pqURL.String()))
	queuePath := filepath.Join(tmpDataPath, persistentQueueDirname, fmt.Sprintf("%d_%016X", argIdx+1, h))
	maxPendingBytes := maxPendingBytesPerURL.GetOptionalArg(argIdx)
	if maxPendingBytes != 0 && maxPendingBytes < persistentqueue.DefaultChunkFileSize {
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4195
		logger.Warnf("rounding the -remoteWrite.maxDiskUsagePerURL=%d to the minimum supported value: %d", maxPendingBytes, persistentqueue.DefaultChunkFileSize)
		maxPendingBytes = persistentqueue.DefaultChunkFileSize
	}

	fq := persistentqueue.MustOpenFastQueue(queuePath, sanitizedURL, maxInmemoryBlocks, maxPendingBytes, false)
	_ = metrics.GetOrCreateGauge(fmt.Sprintf(`vlagent_remotewrite_pending_data_bytes{path=%q, url=%q}`, queuePath, sanitizedURL), func() float64 {
		return float64(fq.GetPendingBytes())
	})
	_ = metrics.GetOrCreateGauge(fmt.Sprintf(`vlagent_remotewrite_pending_inmemory_blocks{path=%q, url=%q}`, queuePath, sanitizedURL), func() float64 {
		return float64(fq.GetInmemoryQueueLen())
	})
	_ = metrics.GetOrCreateGauge(fmt.Sprintf(`vlagent_remotewrite_queue_blocked{path=%q, url=%q}`, queuePath, sanitizedURL), func() float64 {
		if fq.IsWriteBlocked() {
			return 1
		}
		return 0
	})

	var c *client
	switch remoteWriteURL.Scheme {
	case "http", "https":
		c = newHTTPClient(argIdx, remoteWriteURL.String(), sanitizedURL, fq, *queues)
	default:
		logger.Fatalf("unsupported scheme: %s for remoteWriteURL: %s, want `http`, `https`", remoteWriteURL.Scheme, sanitizedURL)
	}
	c.init(argIdx, *queues, sanitizedURL)

	// Initialize pls
	plsLen := *queues
	if n := cgroup.AvailableCPUs(); plsLen > n {
		// There is no sense in running more than availableCPUs concurrent pendingLogs,
		// since every pendingLogs can saturate up to a single CPU.
		plsLen = n
	}
	pls := make([]*pendingLogs, plsLen)
	for i := range pls {
		pls[i] = newPendingLogs(fq, dataFormat)
	}

	rwctx := &remoteWriteCtx{
		fq:  fq,
		c:   c,
		pls: pls,
	}

	// Do not use the GetOptionalArg method here
	// because it returns the same value regardless of argIdx if the flag is set once.
	urlTrs := *urlTransforms
	if argIdx < len(urlTrs) && urlTrs[argIdx] != "" {
		tr := loadTransforms(urlTrs[argIdx], rwctx.pushInternal)
		rwctx.transformer = tr
	}

	return rwctx
}

func (rwctx *remoteWriteCtx) push(lr *logstorage.LogRows) {
	if tr := rwctx.transformer; tr != nil {
		// tr will flush the result to pushInternal.
		tr.Transform(lr)
	} else {
		rwctx.pushInternal(lr)
	}
}

func (rwctx *remoteWriteCtx) pushInternal(lr *logstorage.LogRows) {
	pls := rwctx.pls
	idx := rwctx.plsNextIdx.Add(1) % uint64(len(pls))
	pls[idx].add(lr)
}

func (rwctx *remoteWriteCtx) mustStop() {
	for _, pl := range rwctx.pls {
		pl.mustStop()
	}
	rwctx.pls = nil
	rwctx.fq.UnblockAllReaders()
	rwctx.c.MustStop()
	rwctx.c = nil

	rwctx.fq.MustClose()
	rwctx.fq = nil
}
