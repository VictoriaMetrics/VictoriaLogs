package localstorage

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	enabled     = flag.Bool("localStorage.enabled", false, "Whether to store collected logs in a local JSON Lines file in addition to remote storage")
	logPath     = flag.String("localStorage.logPath", "./vlagent-logs", "Directory for the local log file")
	workerCount = flag.Int("localStorage.workerCount", 1, "Number of workers writing local log entries")
	maxSizeMB   = flag.Int64("localStorage.maxSize", 50, "Maximum active local log file size in megabytes before rotation; 0 disables size-based rotation")
	maxAgeDays  = flag.Int64("localStorage.maxAge", 7, "Maximum age of rotated local log files in days; 0 disables age-based cleanup")
	maxBackups  = flag.Int("localStorage.maxBackups", 10, "Maximum number of rotated local log files to retain; 0 means unlimited")
	localTime   = flag.Bool("localStorage.localTime", true, "Use local time in rotated local log file names")
	compress    = flag.Bool("localStorage.compress", true, "Compress rotated local log files with gzip")
	routeFlags  = flagutil.NewArrayString("localStorage.route", "Kubernetes label-based local file route in JSON format. "+
		`For example: -localStorage.route='{"file":"vlagent/vlagent.log","labels":{"app.kubernetes.io/name":"vlagent"}}'. `+
		"All labels in a route must match. The route with the most labels wins; the first route wins when equally specific. Can be specified multiple times")
)

var global *Storage

type fileConfig struct {
	workerCount     int
	maxSizeBytes    int64
	maxAge          time.Duration
	maxBackups      int
	useLocalTime    bool
	compressRotated bool
}

func defaultFileConfig() fileConfig {
	return fileConfig{
		workerCount:     *workerCount,
		maxSizeBytes:    *maxSizeMB * 1024 * 1024,
		maxAge:          time.Duration(*maxAgeDays) * 24 * time.Hour,
		maxBackups:      *maxBackups,
		useLocalTime:    *localTime,
		compressRotated: *compress,
	}
}

// Storage writes normalized log rows as JSON Lines and rotates the active file.
type Storage struct {
	routesMu sync.Mutex
	routes   map[string]*routeStorage
}

type queuedRow struct {
	data []byte
}

type routeStorage struct {
	queue  chan queuedRow
	wg     sync.WaitGroup
	writer *fileWriter
}

type fileWriter struct {
	mu sync.Mutex

	dir             string
	filePath        string
	filePrefix      string
	maxSizeBytes    int64
	maxAge          time.Duration
	maxBackups      int
	useLocalTime    bool
	compressRotated bool
	rotatedExt      string

	file        *os.File
	currentSize int64
}

// Init initializes local storage when -localStorage.enabled is set.
func Init() {
	if !*enabled {
		return
	}
	if global != nil {
		return
	}
	if *workerCount <= 0 {
		logger.Fatalf("-localStorage.workerCount must be greater than 0")
	}
	if *maxSizeMB < 0 {
		logger.Fatalf("-localStorage.maxSize cannot be negative")
	}
	if *maxAgeDays < 0 {
		logger.Fatalf("-localStorage.maxAge cannot be negative")
	}
	if *maxBackups < 0 {
		logger.Fatalf("-localStorage.maxBackups cannot be negative")
	}
	initKubernetesRoutes()

	if err := os.MkdirAll(*logPath, 0o755); err != nil {
		logger.Fatalf("cannot create -localStorage.logPath=%q: %s", *logPath, err)
	}

	s := &Storage{
		routes: make(map[string]*routeStorage),
	}
	global = s
	logger.Infof("initialized local log storage in %q", *logPath)
}

// Enabled reports whether local storage was initialized.
func Enabled() bool {
	return global != nil
}

type kubernetesRoute struct {
	File   string            `json:"file"`
	Labels map[string]string `json:"labels"`
}

var parsedKubernetesRoutes []kubernetesRoute

func initKubernetesRoutes() {
	parsedKubernetesRoutes = make([]kubernetesRoute, 0, len(*routeFlags))
	for _, rawRoute := range *routeFlags {
		var route kubernetesRoute
		if err := json.Unmarshal([]byte(rawRoute), &route); err != nil {
			logger.Fatalf("cannot parse -localStorage.route=%q: %s", rawRoute, err)
		}
		if route.File == "" {
			logger.Fatalf("-localStorage.route must contain a non-empty file: %q", rawRoute)
		}
		if len(route.Labels) == 0 {
			logger.Fatalf("-localStorage.route must contain at least one label: %q", rawRoute)
		}
		cleanedFile, err := cleanRelativePath(route.File)
		if err != nil {
			logger.Fatalf("invalid file in -localStorage.route=%q: %s", rawRoute, err)
		}
		route.File = cleanedFile
		parsedKubernetesRoutes = append(parsedKubernetesRoutes, route)
	}
}

// RouteForFields returns the file from the most specific Kubernetes label route
// that matches all configured labels. If multiple matching routes have the
// same number of labels, the first one wins. An empty result disables local
// storage for the row.
func RouteForFields(fields []logstorage.Field) string {
	if len(parsedKubernetesRoutes) == 0 {
		return ""
	}

	labels := make(map[string]string)
	for _, field := range fields {
		if strings.HasPrefix(field.Name, "kubernetes.pod_labels.") {
			name := strings.TrimPrefix(field.Name, "kubernetes.pod_labels.")
			labels[name] = field.Value
		}
	}

	bestRoute := ""
	bestLabelCount := 0
	for _, route := range parsedKubernetesRoutes {
		matched := true
		for name, want := range route.Labels {
			got, ok := labels[name]
			if !ok || got != want {
				matched = false
				break
			}
		}
		if matched && len(route.Labels) > bestLabelCount {
			bestRoute = route.File
			bestLabelCount = len(route.Labels)
		}
	}
	return bestRoute
}

// MustAddRows queues rows for local JSON Lines storage.
func MustAddRows(lr *logstorage.LogRows, routedFileName string) {
	s := global
	if s == nil || routedFileName == "" {
		return
	}
	route, err := s.getRoute(routedFileName)
	if err != nil {
		logger.Fatalf("cannot initialize local log file %q: %s", routedFileName, err)
	}
	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		data := r.AppendJSON(nil)
		data = append(data, '\n')
		route.queue <- queuedRow{data: data}
	})
}

// Stop flushes queued local rows and closes the active file.
func Stop() {
	s := global
	if s == nil {
		return
	}
	s.routesMu.Lock()
	routes := make([]*routeStorage, 0, len(s.routes))
	for _, route := range s.routes {
		close(route.queue)
		routes = append(routes, route)
	}
	s.routesMu.Unlock()
	for _, route := range routes {
		route.wg.Wait()
		route.writer.close()
	}
	global = nil
	logger.Infof("stopped local log storage")
}

func (s *Storage) runWorker(route *routeStorage) {
	for row := range route.queue {
		if err := route.writer.write(row.data); err != nil {
			logger.Fatalf("cannot write local log data: %s", err)
		}
	}
}

func (s *Storage) getRoute(relativePath string) (*routeStorage, error) {
	s.routesMu.Lock()
	defer s.routesMu.Unlock()
	if route, ok := s.routes[relativePath]; ok {
		return route, nil
	}
	config := defaultFileConfig()
	queueSize := config.workerCount * 4
	if queueSize > 4096 {
		queueSize = 4096
	}
	writer, err := newFileWriter(relativePath, config)
	if err != nil {
		return nil, err
	}
	route := &routeStorage{
		queue:  make(chan queuedRow, queueSize),
		writer: writer,
	}
	for range config.workerCount {
		route.wg.Go(func() {
			s.runWorker(route)
		})
	}
	s.routes[relativePath] = route
	logger.Infof("initialized local log storage file at %q", writer.filePath)
	return route, nil
}

func newFileWriter(relativePath string, config fileConfig) (*fileWriter, error) {
	relativePath, err := cleanRelativePath(relativePath)
	if err != nil {
		return nil, err
	}
	filePath := filepath.Join(*logPath, relativePath)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(filePath)
	if ext == "" {
		ext = ".log"
	}
	fw := &fileWriter{
		dir:             dir,
		filePath:        filePath,
		filePrefix:      strings.TrimSuffix(baseName, filepath.Ext(baseName)),
		maxSizeBytes:    config.maxSizeBytes,
		maxAge:          config.maxAge,
		maxBackups:      config.maxBackups,
		useLocalTime:    config.useLocalTime,
		compressRotated: config.compressRotated,
		rotatedExt:      ext,
	}
	if err := fw.open(); err != nil {
		return nil, err
	}
	return fw, nil
}

func cleanRelativePath(relativePath string) (string, error) {
	cleaned := filepath.Clean(relativePath)
	if cleaned == "." || filepath.IsAbs(relativePath) || filepath.VolumeName(relativePath) != "" || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("local file path must stay under -localStorage.logPath, got %q", relativePath)
	}
	return cleaned, nil
}

func (fw *fileWriter) open() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	f, err := os.OpenFile(fw.filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	fw.file = f
	fw.currentSize = fi.Size()
	return nil
}

func (fw *fileWriter) write(data []byte) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return fw.writeDataLocked(data)
}

func (fw *fileWriter) writeDataLocked(data []byte) error {
	if fw.maxSizeBytes > 0 && fw.currentSize > 0 && fw.currentSize+int64(len(data)) > fw.maxSizeBytes {
		if err := fw.rotateLocked(); err != nil {
			return err
		}
	}
	n, err := fw.file.Write(data)
	fw.currentSize += int64(n)
	return err
}

func (fw *fileWriter) rotateLocked() error {
	if fw.file == nil {
		return fmt.Errorf("active local log file is not open")
	}
	if err := fw.file.Close(); err != nil {
		return err
	}
	fw.file = nil

	rotatedPath := fw.nextRotatedPathLocked()
	if err := os.Rename(fw.filePath, rotatedPath); err != nil {
		return err
	}
	if fw.compressRotated {
		if err := compressFile(rotatedPath); err != nil {
			return err
		}
	}

	if err := fw.cleanupLocked(); err != nil {
		return err
	}
	f, err := os.OpenFile(fw.filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	fw.file = f
	fw.currentSize = 0
	return nil
}

func (fw *fileWriter) nextRotatedPathLocked() string {
	now := time.Now()
	if !fw.useLocalTime {
		now = now.UTC()
	}
	ext := fw.rotatedExt
	base := filepath.Join(fw.dir, fw.filePrefix+"."+now.Format("20060102-150405.000")+ext)
	for i := 1; ; i++ {
		candidate := base
		if i > 1 {
			candidate = strings.TrimSuffix(base, ext) + fmt.Sprintf(".%d%s", i, ext)
		}
		if _, err := os.Stat(candidate); !os.IsNotExist(err) {
			continue
		}
		if fw.compressRotated {
			if _, err := os.Stat(candidate + ".gz"); !os.IsNotExist(err) {
				continue
			}
		}
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
}

func compressFile(path string) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	outPath := path + ".gz"
	out, err := os.Create(outPath)
	if err != nil {
		_ = in.Close()
		return err
	}
	gz := gzip.NewWriter(out)
	_, copyErr := io.Copy(gz, in)
	closeGzipErr := gz.Close()
	closeOutErr := out.Close()
	closeInErr := in.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeGzipErr != nil {
		return closeGzipErr
	}
	if closeOutErr != nil {
		return closeOutErr
	}
	if closeInErr != nil {
		return closeInErr
	}
	return os.Remove(path)
}

func (fw *fileWriter) cleanupLocked() error {
	entries, err := os.ReadDir(fw.dir)
	if err != nil {
		return err
	}
	type backup struct {
		path    string
		modTime time.Time
	}
	backups := make([]backup, 0)
	cutoff := time.Time{}
	if fw.maxAge > 0 {
		cutoff = time.Now().Add(-fw.maxAge)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, fw.filePrefix+".") || (!strings.HasSuffix(name, fw.rotatedExt) && !strings.HasSuffix(name, fw.rotatedExt+".gz")) {
			continue
		}
		path := filepath.Join(fw.dir, name)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !cutoff.IsZero() && info.ModTime().Before(cutoff) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		backups = append(backups, backup{path: path, modTime: info.ModTime()})
	}
	if fw.maxBackups == 0 || len(backups) <= fw.maxBackups {
		return nil
	}
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].modTime.After(backups[j].modTime)
	})
	for _, old := range backups[fw.maxBackups:] {
		if err := os.Remove(old.path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (fw *fileWriter) close() {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.file != nil {
		_ = fw.file.Close()
		fw.file = nil
	}
}
