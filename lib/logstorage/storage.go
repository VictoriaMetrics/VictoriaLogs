package logstorage

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/contextutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/objectstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/objectstorage/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/snapshot/snapshotutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
)

const (
	// minRetention is the minimum allowed retention period.
	// Partitions newer than minRetention are kept read-write regardless of the configured retention.
	minRetention = 24 * time.Hour

	// minOffload is the minimum allowed offload period.
	// Partitions newer than minOffload are never moved to object storage.
	minOffload = 24 * time.Hour
)

// StorageStats represents stats for the storage. It may be obtained by calling Storage.UpdateStats().
type StorageStats struct {
	// RowsDroppedTooBigTimestamp is the number of rows dropped during data ingestion because their timestamp is bigger than the maximum allowed.
	RowsDroppedTooBigTimestamp uint64

	// RowsDroppedTooSmallTimestamp is the number of rows dropped during data ingestion because their timestamp is smaller than the minimum allowed.
	RowsDroppedTooSmallTimestamp uint64

	// PartitionsCount is the number of partitions in the storage.
	PartitionsCount uint64

	// MaxDiskSpaceUsageBytes is the maximum disk space logs can use.
	MaxDiskSpaceUsageBytes int64

	// IsReadOnly indicates whether the storage is read-only.
	IsReadOnly bool

	// PartitionStats contains partition stats.
	PartitionStats

	// MinTimestamp is the minimum event timestamp across the entire storage (in nanoseconds).
	// It is set to math.MinInt64 if there is no data.
	MinTimestamp int64

	// MaxTimestamp is the maximum event timestamp across the entire storage (in nanoseconds).
	// It is set to math.MaxInt64 if there is no data.
	MaxTimestamp int64
}

// Reset resets s.
func (s *StorageStats) Reset() {
	*s = StorageStats{}
}

// StorageConfig is the config for the Storage.
type StorageConfig struct {
	ObjectStorage objectstorage.StorageConfig

	// Offload is the period after which data is moved to object storage
	Offload time.Duration

	// Retention is the retention for the ingested data.
	//
	// Older data is automatically deleted.
	Retention time.Duration

	// DefaultParallelReaders is the default number of parallel readers to use per each query execution.
	//
	// Higher value can help improving query performance on storage with high disk read latency such as S3.
	DefaultParallelReaders int

	// MaxDiskSpaceUsageBytes is an optional maximum disk space logs can use.
	//
	// The oldest per-day partitions are automatically dropped if the total disk space usage exceeds this limit.
	MaxDiskSpaceUsageBytes int64

	// MaxDiskUsagePercent is an optional threshold in percentage (1-100) for disk usage of the filesystem holding the storage path.
	// When the current disk usage exceeds this percentage, the oldest per-day partitions are automatically dropped.
	MaxDiskUsagePercent int

	// FlushInterval is the interval for flushing the in-memory data to disk at the Storage.
	FlushInterval time.Duration

	// FutureRetention is the allowed retention from the current time to future for the ingested data.
	//
	// Log entries with timestamps bigger than now+FutureRetention are ignored.
	FutureRetention time.Duration

	// MaxBackfillAge is the maximum allowed age for the backfilled logs.
	//
	// Log entries with timestamps older than now-MaxBackfillAge are ignored.
	MaxBackfillAge time.Duration

	// SnapshotsMaxAge is the maximum age for the created partition snapshots.
	//
	// Snapshots are automatically dropped after that duration.
	// See https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle
	SnapshotsMaxAge time.Duration

	// MinFreeDiskSpaceBytes is the minimum free disk space at storage path after which the storage stops accepting new data
	// and enters read-only mode.
	MinFreeDiskSpaceBytes int64

	// LogNewStreams indicates whether to log newly created log streams.
	//
	// This can be useful for debugging of high cardinality issues.
	// https://docs.victoriametrics.com/victorialogs/keyconcepts/#high-cardinality
	LogNewStreams bool

	// LogIngestedRows indicates whether to log the ingested log entries.
	//
	// This can be useful for debugging of data ingestion.
	LogIngestedRows bool
}

// Storage is the storage for log entries.
type Storage struct {
	rowsDroppedTooBigTimestamp   atomic.Uint64
	rowsDroppedTooSmallTimestamp atomic.Uint64

	// path is the path to the Storage directory
	path string

	// sc is a remote storage filesystem implementation
	sc common.StorageClient

	// retention is the retention for the stored data
	//
	// older data is automatically deleted
	retention time.Duration

	// offload is the period, after which data is moved to object storage
	offload time.Duration

	// defaultParallelReaders is the default number of parallel IO-bound readers to use for query execution.
	//
	// Higher number of readers may help increasing query performance on storage with high read latency such as S3.
	defaultParallelReaders int

	// maxDiskSpaceUsageBytes is an optional maximum disk space logs can use.
	//
	// The oldest per-day partitions are automatically dropped if the total disk space usage exceeds this limit.
	maxDiskSpaceUsageBytes int64

	// maxDiskUsagePercent is an optional threshold for disk usage percentage at which the oldest partitions are automatically dropped.
	maxDiskUsagePercent int

	// flushInterval is the interval for flushing in-memory data to disk
	flushInterval time.Duration

	// futureRetention is the maximum allowed interval to write data into the future
	futureRetention time.Duration

	// maxBackfillAge is the maximum age of logs with historical timestamps to accept.
	// It is atomic because it is written by the background watchOffload goroutine
	// and read concurrently by MustAddRows.
	maxBackfillAge atomic.Int64

	// snapshotsMaxAge is the maximum age for the created partition snapshots.
	//
	// Older snapshots are automatically deleted. See https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle
	snapshotsMaxAge time.Duration

	// minFreeDiskSpaceBytes is the minimum free disk space at path after which the storage stops accepting new data
	minFreeDiskSpaceBytes uint64

	// logNewStreams instructs to log new streams if it is set to true
	logNewStreams atomic.Bool

	// logIngestedRows instructs to log all the ingested log entries if it is set to true
	logIngestedRows bool

	// flockF is a file, which makes sure that the Storage is opened by a single process
	flockF *os.File

	// partitions is a list of partitions for the Storage.
	//
	// It must be accessed under partitionsLock.
	//
	// partitions are sorted by time, e.g. partitions[0] has the smallest time.
	partitions []*partitionWrapper

	// ptwHot is the "hot" partition, where the last rows were ingested.
	//
	// It must be accessed under partitionsLock.
	ptwHot *partitionWrapper

	// deletedPartitions contains days for the deleted partitions.
	//
	// It prevents from re-creating already deleted partitions.
	//
	// It must be accessed under partitionsLock.
	deletedPartitions []int64

	// partitionsLock protects partitions, ptwHot, deletedPartitions.
	partitionsLock sync.Mutex

	// stopCh is closed when the Storage must be stopped.
	stopCh chan struct{}

	// wg is used for waiting for background workers at MustClose().
	wg sync.WaitGroup

	// streamIDCache caches (partition, streamIDs) seen during data ingestion.
	//
	// It reduces the load on persistent storage during data ingestion by skipping
	// the check whether the given stream is already registered in the persistent storage.
	streamIDCache *cache

	// filterStreamCache caches streamIDs keyed by (partition, []TenantID, StreamFilter).
	//
	// It reduces the load on persistent storage during querying by _stream:{...} filter.
	filterStreamCache *cache

	// partitionCacheGeneration is incremented on partition attach and detach.
	//
	// It is used for invalidating partition-related caches after partition lifecycle changes.
	partitionCacheGeneration atomic.Uint64

	// deleteTasksLock protects deleteTasks
	deleteTasksLock sync.Mutex

	// deleteTasks contains a list of active and pending delete tasks
	deleteTasks []*DeleteTask
}

// PartitionAttach attaches the partition with the given name to s.
//
// The name must have the YYYYMMDD format.
//
// The attached partition can be detached via PartitionDetach() call.
func (s *Storage) PartitionAttach(name string) error {
	day, err := getPartitionDayFromName(name)
	if err != nil {
		return err
	}

	s.partitionsLock.Lock()
	defer s.partitionsLock.Unlock()

	if slices.Contains(s.deletedPartitions, day) {
		return fmt.Errorf("cannot attach the partition %q, since it is automatically deleted because of retention; see https://docs.victoriametrics.com/victorialogs/#retention", name)
	}

	// Verify whether the given partition already exists in the attached partitions list.
	for _, ptw := range s.partitions {
		if ptw.pt.name == name {
			return fmt.Errorf("cannot attach the partition %q, because it is already attached", name)
		}
	}

	// Open the partition and add it to the s.partitions.
	partitionsPath := filepath.Join(s.path, partitionsDirname)
	partitionPath := filepath.Join(partitionsPath, name)
	if !fs.IsPathExist(partitionPath) {
		return fmt.Errorf("cannot attach the partition %q, because there is no the corresponding directory %q; note that attaching object storage partitions is not supported", name, partitionPath)
	}

	pt := mustOpenLocalPartition(s, partitionPath)
	ptw := newPartitionWrapper(pt, day, false)

	s.partitions = append(s.partitions, ptw)
	sortPartitions(s.partitions)
	s.partitionCacheGeneration.Add(1)

	logger.Infof("successfully attached partition %q from %q", name, partitionPath)

	return nil
}

// PartitionDetach detaches the partition with the given name from s.
//
// The name must have the YYYYMMDD format.
//
// The detached partition can be attached again via PartitionAttach() call.
func (s *Storage) PartitionDetach(name string) error {
	var err error
	ptw := func() *partitionWrapper {
		s.partitionsLock.Lock()
		defer s.partitionsLock.Unlock()

		for i, ptw := range s.partitions {
			if ptw.pt.name != name {
				continue
			}
			if ptw.pt.isRemote {
				err = fmt.Errorf("cannot detach the partition %q, because detaching object storage partitions is not supported", name)
				return nil
			}

			// Found the partition to detach. Detach it.
			s.partitions = append(s.partitions[:i], s.partitions[i+1:]...)
			if ptw == s.ptwHot {
				s.ptwHot = nil
			}
			return ptw
		}
		return nil
	}()

	if err != nil {
		return err
	}
	if ptw == nil {
		return fmt.Errorf("cannot detach the partition %q, because it isn't attached", name)
	}

	partitionPath := ptw.pt.path
	ptw.releaseRefs()

	logger.Infof("waiting until the partition %q isn't accessed", name)
	<-ptw.doneCh

	// Invalidate partition-related caches after partition detach.
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/657
	s.partitionCacheGeneration.Add(1)

	logger.Infof("successfully detached partition %q from %q", name, partitionPath)

	return nil
}

// PartitionList returns the list of the names for the currently attached partitions.
//
// Every partition name has YYYYMMDD format.
func (s *Storage) PartitionList() []string {
	s.partitionsLock.Lock()
	defer s.partitionsLock.Unlock()

	ptNames := make([]string, len(s.partitions))
	for i, ptw := range s.partitions {
		ptNames[i] = ptw.pt.name
	}
	return ptNames
}

// PartitionSnapshotMustCreate creates snapshots for partitions with the given partitionPrefix
//
// The partitionPrefix must match one of the following formats:
// - YYYYMMDD - matches partitions for the given day
// - YYYYMM - matches partitions for the given month
// - YYYY - matches partitions for the given year
// - an empty string - matches all the partitions
//
// The function returns paths to created snapshots
func (s *Storage) PartitionSnapshotMustCreate(partitionPrefix string) []string {
	ptws := s.getWritePartitions()
	defer s.putWritePartitions(ptws)

	var snapshotPaths []string

	for _, ptw := range ptws {
		if strings.HasPrefix(ptw.pt.name, partitionPrefix) {
			snapshotPath := ptw.pt.mustCreateSnapshot()
			snapshotPaths = append(snapshotPaths, snapshotPath)
		}
	}

	return snapshotPaths
}

// PartitionSnapshotList returns a list of paths to all the snapshots across active partitions.
func (s *Storage) PartitionSnapshotList() []string {
	ptws := s.getWritePartitions()
	defer s.putWritePartitions(ptws)

	snapshotPaths := getSnapshotPaths(ptws)
	sort.Strings(snapshotPaths)

	return snapshotPaths
}

func getSnapshotPaths(ptws []*partitionWrapper) []string {
	var snapshotPaths []string

	for _, ptw := range ptws {
		snapshotsPath := filepath.Join(ptw.pt.path, snapshotsDirname)
		if !fs.IsPathExist(snapshotsPath) {
			continue
		}

		des := fs.MustReadDir(snapshotsPath)
		for _, de := range des {
			name := de.Name()
			if err := snapshotutil.Validate(name); err != nil {
				logger.Warnf("unsupported snapshot name %q at %q: %s", name, snapshotsPath, err)
				continue
			}

			snapshotPath := filepath.Join(snapshotsPath, name)
			snapshotPaths = append(snapshotPaths, snapshotPath)
		}
	}

	return snapshotPaths
}

// PartitionSnapshotDelete removes the snapshot located at the given snapshotPath if it belongs to an active partition.
func (s *Storage) PartitionSnapshotDelete(snapshotPath string) error {
	snapshotName := filepath.Base(snapshotPath)
	if err := snapshotutil.Validate(snapshotName); err != nil {
		return fmt.Errorf("unsupported snapshot name %q at %q: %w", snapshotName, snapshotPath, err)
	}

	snapshotDir := filepath.Dir(snapshotPath)
	if filepath.Base(snapshotDir) != snapshotsDirname {
		return fmt.Errorf("snapshot path %q must point to a directory inside %q", snapshotPath, snapshotsDirname)
	}
	partitionPath := filepath.Dir(snapshotDir)

	ptws := s.getWritePartitions()
	defer s.putWritePartitions(ptws)

	ptw := func() *partitionWrapper {
		for _, ptw := range ptws {
			if ptw.pt.path == partitionPath {
				return ptw
			}
		}
		return nil
	}()

	if ptw == nil {
		return fmt.Errorf("partition path %q cannot be found across active partitions", partitionPath)
	}

	return ptw.pt.deleteSnapshot(snapshotName)
}

// MustDeleteStalePartitionSnapshots deletes snapshots older than maxAge.
//
// The list of paths to deleted snapshots is returned from this function.
func (s *Storage) MustDeleteStalePartitionSnapshots(maxAge time.Duration) []string {
	var deletedSnapshotPaths []string

	currentTime := time.Now()

	ptws := s.getWritePartitions()
	defer s.putWritePartitions(ptws)

	snapshotPaths := getSnapshotPaths(ptws)
	for _, snapshotPath := range snapshotPaths {
		fi, err := os.Stat(snapshotPath)
		if err != nil {
			logger.Warnf("skipping snapshot at %s since cannot access it: %s", snapshotPath, err)
			continue
		}

		creationTime := fi.ModTime()
		if currentTime.Sub(creationTime) > maxAge {
			logger.Infof("deleting snapshot at %s because it became older than maxAge=%s (snapshot creation time: %s)", snapshotPath, maxAge, creationTime)
			fs.MustRemoveDir(snapshotPath)
			deletedSnapshotPaths = append(deletedSnapshotPaths, snapshotPath)
			logger.Infof("deleted snapshot at %s", snapshotPath)
		}
	}

	return deletedSnapshotPaths
}

// DeleteRunTask starts deletion of logs according to the given filter f for the given tenantIDs.
//
// The taskID must contain a unique id of the task. It is used for tracking the task at the list returned by DeleteActiveTasks().
// The timestamp must contain the timestamp in nanoseconds when the task is started.
func (s *Storage) DeleteRunTask(_ context.Context, taskID string, timestamp int64, tenantIDs []TenantID, f *Filter) error {
	minTimestamp, _ := getFilterTimeRange(f.f)
	now := time.Now().UnixNano()
	minAllowedTimestamp := now - s.offload.Nanoseconds()
	if s.sc != nil && minTimestamp < minAllowedTimestamp {
		return fmt.Errorf("deletion operation was rejected, operation's time range overlaps read-only data boundary")
	}

	// Register the task in the list of active delete tasks, so it survives application restarts and crashes.
	dt := newDeleteTask(taskID, timestamp, tenantIDs, f.String())

	s.deleteTasksLock.Lock()
	defer s.deleteTasksLock.Unlock()

	// Verify that the task with the given taskID doesn't exist yet
	for _, dt := range s.deleteTasks {
		if dt.TaskID == taskID {
			return fmt.Errorf("the delete task with task_id=%q is already registered", taskID)
		}
	}

	// Register the task and persist it to the file.
	s.deleteTasks = append(s.deleteTasks, dt)
	s.mustSaveDeleteTasksLocked()

	return nil
}

// mustSaveDeleteTasksLocked saves s.deleteTasks to file
//
// The s.deleteTaskLock must be locked while calling this function.
func (s *Storage) mustSaveDeleteTasksLocked() {
	deleteTasksPath := filepath.Join(s.path, deleteTasksFilename)
	mustWriteDeleteTasksToFile(deleteTasksPath, s.deleteTasks)
}

// DeleteStopTask stops the delete task with the given taskID.
//
// It waits until the task is stopped before returning.
// If there is no a task with the given taskID, then the function returns immediately.
func (s *Storage) DeleteStopTask(ctx context.Context, taskID string) error {
	var doneCh <-chan struct{}

	s.deleteTasksLock.Lock()

	for i, dt := range s.deleteTasks {
		if dt.TaskID != taskID {
			continue
		}

		if dt.cancel != nil {
			// Cancel the currently executed task. The task executor will remove this task from s.deleteTasks
			dt.cancel()
			doneCh = dt.doneCh
		} else {
			// The task is waiting to be executed. Drop it.
			s.deleteTasks = append(s.deleteTasks[:i], s.deleteTasks[i+1:]...)
			s.mustSaveDeleteTasksLocked()
		}
		break
	}

	s.deleteTasksLock.Unlock()

	if doneCh == nil {
		return nil
	}

	// Wait until the task is canceled.
	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DeleteActiveTasks returns currently running active delete tasks, which were started via DeleteRunTask().
func (s *Storage) DeleteActiveTasks(_ context.Context) ([]*DeleteTask, error) {
	s.deleteTasksLock.Lock()
	dts := append([]*DeleteTask{}, s.deleteTasks...)
	s.deleteTasksLock.Unlock()

	return dts, nil
}

// EnableLogNewStreams enables logging newly ingested streams during the given number of seconds
func (s *Storage) EnableLogNewStreams(seconds int) {
	if seconds <= 0 {
		// Do nothing.
		return
	}

	vPrev := s.logNewStreams.Swap(true)
	if vPrev {
		logger.Infof("logging of new streams is already enabled")
		return
	}

	logger.Infof("enabled logging of new streams for %d seconds", seconds)

	d := time.Second * time.Duration(seconds)
	time.AfterFunc(d, func() {
		s.logNewStreams.Swap(false)
		logger.Infof("disabled logging of new streams")
	})
}

type partitionWrapper struct {
	// totalRefCount is the total number of active read and write references.
	// When it reaches zero, the partition is closed.
	totalRefCount atomic.Int32

	// writeRefCount tracks active write references.
	// When it reaches zero, writeDoneCh is closed.
	writeRefCount atomic.Int32

	// mustDrop is set when the partition must be deleted after totalRefCount reaches zero.
	mustDrop atomic.Bool

	// isReadOnly is set when the partition must be excluded from ingestion.
	isReadOnly atomic.Bool

	// day is the day for the partition in the unix timestamp divided by the number of seconds in the day.
	day int64

	// pt is the wrapped partition.
	pt *partition

	// writeDoneCh is closed when writeRefCount reaches zero
	writeDoneCh chan struct{}

	// doneCh is closed when totalRefCount reaches zero, e.g. when the partitionWrapper is no longer accessed.
	doneCh chan struct{}
}

func newPartitionWrapper(pt *partition, day int64, isReadOnly bool) *partitionWrapper {
	pw := &partitionWrapper{
		day:    day,
		pt:     pt,
		doneCh: make(chan struct{}),
	}
	if isReadOnly {
		pw.isReadOnly.Store(isReadOnly)
	} else {
		// The owner write sentinel keeps writeDoneCh open until setReadOnly is called.
		// It only touches writeRefCount, not totalRefCount — partition lifetime is
		// governed solely by the read ref.
		pw.writeRefCount.Add(1)
		pw.writeDoneCh = make(chan struct{})
	}

	pw.incReadRef()
	return pw
}

func (ptw *partitionWrapper) setReadOnly() {
	if ptw.isReadOnly.Swap(true) {
		// Already marked read-only; the owner write sentinel was already dropped.
		return
	}
	// Drop the owner write sentinel. Does not touch totalRefCount — only
	// in-flight writers acquired via incWriteRef do.
	if ptw.writeRefCount.Add(-1) == 0 {
		close(ptw.writeDoneCh)
	}
}

func (ptw *partitionWrapper) incReadRef() {
	ptw.totalRefCount.Add(1)
}

func (ptw *partitionWrapper) incWriteRef() {
	ptw.writeRefCount.Add(1)
	ptw.totalRefCount.Add(1)
}

// releaseRefs drops the owner refs for a partition.
// For writable partitions it drops the owner write sentinel and the read ref.
// For read-only partitions it drops only the read ref.
func (ptw *partitionWrapper) releaseRefs() {
	ptw.setReadOnly()
	ptw.decReadRef()
}

func (ptw *partitionWrapper) decWriteRef() {
	if ptw.writeRefCount.Add(-1) == 0 {
		close(ptw.writeDoneCh)
	}
	if ptw.totalRefCount.Add(-1) == 0 {
		ptw.close()
	}
}

func (ptw *partitionWrapper) decReadRef() {
	if ptw.totalRefCount.Add(-1) == 0 {
		ptw.close()
	}
}

func (ptw *partitionWrapper) close() {
	deletePath := ""
	if ptw.mustDrop.Load() {
		deletePath = ptw.pt.path
	}

	var sc common.StorageClient
	if ptw.pt.isRemote {
		sc = ptw.pt.s.sc
	}

	// Close pw.pt, since nobody refers to it.
	mustClosePartition(ptw.pt)
	ptw.pt = nil

	// Delete partition if needed.
	if deletePath != "" {
		mustDeletePartition(deletePath, sc)
	}

	// signal that the ptw is no longer accessed.
	close(ptw.doneCh)
}

func (ptw *partitionWrapper) canAddAllRows(lr *LogRows) bool {
	minTimestamp := ptw.day * nsecsPerDay
	maxTimestamp := minTimestamp + nsecsPerDay - 1
	for _, ts := range lr.timestamps {
		if ts < minTimestamp || ts > maxTimestamp {
			return false
		}
	}
	return true
}

// mustCreateStorage creates Storage at the given path.
func mustCreateStorage(path string) {
	fs.MustMkdirFailIfExist(path)

	partitionsPath := filepath.Join(path, partitionsDirname)
	fs.MustMkdirFailIfExist(partitionsPath)

	fs.MustSyncPathAndParentDir(path)
}

// MustOpenStorage opens Storage at the given path.
//
// MustClose must be called on the returned Storage when it is no longer needed.
func MustOpenStorage(path string, cfg *StorageConfig) *Storage {
	flushInterval := max(cfg.FlushInterval, time.Second)

	offload := max(cfg.Offload, minOffload)

	retention := max(cfg.Retention, minRetention, offload)

	futureRetention := max(cfg.FutureRetention, minRetention)

	var minFreeDiskSpaceBytes uint64
	if cfg.MinFreeDiskSpaceBytes >= 0 {
		minFreeDiskSpaceBytes = uint64(cfg.MinFreeDiskSpaceBytes)
	}

	if !fs.IsPathExist(path) {
		mustCreateStorage(path)
	}

	flockF := fs.MustCreateFlockFile(path)

	// Load caches
	streamIDCache := newCache()
	filterStreamCache := newCache()

	// Load delete tasks which may be left since the previous restart
	deleteTasksPath := filepath.Join(path, deleteTasksFilename)
	deleteTasks := mustReadDeleteTasksFromFile(deleteTasksPath)

	s := &Storage{
		path:                   path,
		offload:                offload,
		retention:              retention,
		defaultParallelReaders: cfg.DefaultParallelReaders,
		maxDiskSpaceUsageBytes: cfg.MaxDiskSpaceUsageBytes,
		maxDiskUsagePercent:    cfg.MaxDiskUsagePercent,
		flushInterval:          flushInterval,
		futureRetention:        futureRetention,
		snapshotsMaxAge:        cfg.SnapshotsMaxAge,
		minFreeDiskSpaceBytes:  minFreeDiskSpaceBytes,
		logIngestedRows:        cfg.LogIngestedRows,
		flockF:                 flockF,
		stopCh:                 make(chan struct{}),

		streamIDCache:     streamIDCache,
		filterStreamCache: filterStreamCache,

		deleteTasks: deleteTasks,
	}

	if len(cfg.ObjectStorage.Destination) > 0 {
		s.sc = objectstorage.New(cfg.ObjectStorage, s.stopCh)
	}

	s.logNewStreams.Store(cfg.LogNewStreams)

	ptws := s.loadLocalPartitions(path)

	maxBackfillAge := cfg.MaxBackfillAge.Nanoseconds()
	if maxBackfillAge <= 0 || maxBackfillAge > retention.Nanoseconds() || maxBackfillAge > offload.Nanoseconds() {
		if s.sc != nil {
			maxBackfillAge = max(retention.Nanoseconds(), offload.Nanoseconds())
		} else {
			maxBackfillAge = retention.Nanoseconds()
		}
	}

	if s.sc != nil {
		partitionExists := func(day int64) bool {
			for i := len(ptws) - 1; i >= 0; i-- {
				lptw := ptws[i]
				if lptw.day > day {
					return false
				}
				if lptw.day == day {
					return true
				}
			}
			return false
		}

		rptws := s.mustLoadRemotePartitions(partitionExists)
		now := time.Now().UnixNano()
		for _, rptw := range rptws {
			if rptw == nil {
				continue
			}
			ptMaxBackfillAge := now - (rptw.day+1)*nsecsPerDay
			if maxBackfillAge > ptMaxBackfillAge {
				maxBackfillAge = ptMaxBackfillAge
			}
			ptws = append(ptws, rptw)
		}
		sortPartitions(ptws)
	}

	s.maxBackfillAge.Store(maxBackfillAge)
	s.partitions = ptws
	s.runRetentionWatcher()
	s.runMaxDiskSpaceUsageWatcher()
	s.runDeleteTasksWatcher()
	s.runSnapshotsMaxAgeWatcher()
	s.runOffloadWatcher()
	return s
}

func (s *Storage) mustLoadRemotePartitions(exists func(d int64) bool) []*partitionWrapper {
	dirs, err := s.sc.ReadDir(partitionsDirname)
	if err != nil {
		logger.Panicf("FATAL: cannot list partitions at %q: %w", s.sc.GetRoot(), err)
	}
	ptws := make([]*partitionWrapper, len(dirs))

	// Open partitions in parallel. This should improve VictoriaLogs initialization duration
	// when it opens many partitions.
	var wg sync.WaitGroup
	concurrencyLimiterCh := make(chan struct{}, cgroup.AvailableCPUs())

	now := time.Now().UnixNano()
	maxReadOnlyDay := (now - minRetention.Nanoseconds()) / nsecsPerDay

	for idx, dir := range dirs {
		day, err := getPartitionDayFromName(dir)
		if err != nil {
			logger.Panicf("FATAL: cannot parse partition filename %q at %q: %s", dir, s.sc.GetPath(partitionsDirname), err)
		}

		if day > maxReadOnlyDay {
			logger.Infof("skipping partition %q which may block live logs ingestion", dir)
			continue
		}

		if exists(day) {
			logger.Infof("skipping partition %q which exists on a local file system", dir)
			continue
		}

		concurrencyLimiterCh <- struct{}{}
		wg.Go(func() {
			defer func() {
				<-concurrencyLimiterCh
			}()
			partitionPath := filepath.Join(partitionsDirname, dir)
			pt, ok := mustOpenRemotePartition(s, partitionPath)
			if !ok {
				logger.Infof("skipping partition %q: %s", dir, err)
				return
			}
			ptws[idx] = newPartitionWrapper(pt, day, true)
		})
	}
	wg.Wait()
	return ptws
}

func (s *Storage) loadLocalPartitions(path string) []*partitionWrapper {
	partitionsPath := filepath.Join(path, partitionsDirname)
	fs.MustMkdirIfNotExist(partitionsPath)
	fs.MustSyncPath(path)

	des := fs.MustReadDir(partitionsPath)
	var partitionNames []string
	for _, de := range des {
		fname := de.Name()
		if strings.HasPrefix(fname, ".") {
			// Ignore "hidden" entries, which can be automatically created by MacOS (such as .DS_Store).
			// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/996
			continue
		}

		partitionDir := filepath.Join(partitionsPath, fname)
		if fs.IsPartiallyRemovedDir(partitionDir) {
			// Drop partially removed partition directory. This may happen when unclean shutdown happens during partition deletion.
			fs.MustRemoveDir(partitionDir)
			continue
		}

		partitionNames = append(partitionNames, fname)
	}

	// Open partitions in parallel. This should improve VictoriaLogs initialization duration when it opens many partitions.
	ptws := make([]*partitionWrapper, len(partitionNames))
	var wg sync.WaitGroup
	concurrencyLimiterCh := make(chan struct{}, cgroup.AvailableCPUs())
	for idx, fname := range partitionNames {
		concurrencyLimiterCh <- struct{}{}
		wg.Go(func() {
			day, err := getPartitionDayFromName(fname)
			if err != nil {
				logger.Panicf("FATAL: cannot parse partition filename %q at %q: %s", fname, partitionsPath, err)
			}

			partitionPath := filepath.Join(partitionsPath, fname)
			pt := mustOpenLocalPartition(s, partitionPath)
			ptws[idx] = newPartitionWrapper(pt, day, false)

			<-concurrencyLimiterCh
		})
	}
	wg.Wait()

	sortPartitions(ptws)

	// Delete partitions from the future if needed
	now := time.Now().UnixNano()
	maxAllowedDay := s.getMaxAllowedDay(now)
	j := len(ptws) - 1
	for j >= 0 {
		ptw := ptws[j]
		if ptw.day <= maxAllowedDay {
			break
		}
		logger.Infof("the partition %s is scheduled to be deleted because it is outside the -futureRetention=%dd", ptw.pt.path, durationToDays(s.futureRetention))
		ptw.mustDrop.Store(true)
		ptw.releaseRefs()
		j--
	}
	j++
	for i := j; i < len(ptws); i++ {
		ptws[i] = nil
	}
	ptws = ptws[:j]
	return ptws
}

func sortPartitions(ptws []*partitionWrapper) {
	sort.Slice(ptws, func(i, j int) bool {
		return ptws[i].day < ptws[j].day
	})
}

func (s *Storage) runRetentionWatcher() {
	s.wg.Go(s.watchRetention)
}

func (s *Storage) runOffloadWatcher() {
	if s.sc == nil || s.retention-s.offload <= 24*time.Hour {
		return
	}
	s.wg.Go(s.watchOffload)
}

func (s *Storage) runMaxDiskSpaceUsageWatcher() {
	if s.maxDiskSpaceUsageBytes <= 0 && s.maxDiskUsagePercent <= 0 {
		return // nothing to watch
	}
	s.wg.Go(s.watchMaxDiskSpaceUsage)
}

func (s *Storage) runDeleteTasksWatcher() {
	s.wg.Go(s.watchDeleteTasks)
}

func (s *Storage) runSnapshotsMaxAgeWatcher() {
	s.wg.Go(s.watchSnapshotsMaxAge)
}

func (s *Storage) offloadPartition(lptw *partitionWrapper) error {
	lptw.incReadRef()
	defer lptw.decReadRef()

	s.partitionsLock.Lock()
	lptw.setReadOnly()
	s.partitionsLock.Unlock()

	pt := lptw.pt
	partitionPath := filepath.Join(partitionsDirname, pt.name)
	partitionReadyFile := filepath.Join(partitionPath, partitionReadyFilename)
	if exists, err := s.sc.IsFileExist(partitionReadyFile); err != nil {
		return fmt.Errorf("cannot check readiness marker for partition %q: %w", pt.name, err)
	} else if exists {
		logger.Panicf("remote partition %s already exists, please either remove local partition or remove readiness marker %s from remote", pt.name, s.sc.GetPath(partitionReadyFile))
	}
	<-lptw.writeDoneCh
	if err := pt.deleteAllSnapshots(); err != nil {
		return fmt.Errorf("failed to delete all snapshots for partition %q before offloading: %w", pt.name, err)
	}
	pt.ddb.drainBackgroundWorkers()
	pt.mustForceMerge(true)
	remotePath := s.sc.GetPath(partitionPath)
	if err := s.sc.Sync(pt.path, partitionPath); err != nil {
		return fmt.Errorf("cannot sync partition %q to %s, skipping: %w", pt.name, remotePath, err)
	}
	if err := s.sc.CreateFile(partitionReadyFile, nil); err != nil {
		return fmt.Errorf("failed to create readiness marker for partition %q: %w", pt.name, err)
	}
	rpt, _ := mustOpenRemotePartition(s, partitionPath)

	day := lptw.day
	rptw := newPartitionWrapper(rpt, day, true)

	s.partitionsLock.Lock()
	defer s.partitionsLock.Unlock()

	ptws := s.partitions
	for i, ptw := range ptws {
		if ptw.day == day {
			if ptw.mustDrop.Load() {
				rptw.mustDrop.Store(true)
				rptw.releaseRefs()
				break
			}
			ptws[i] = rptw
			break
		}
	}
	return nil
}

func (s *Storage) watchOffload() {
	d := timeutil.AddJitterToDuration(time.Hour)
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		now := time.Now().UnixNano()
		offloadDay := s.getOffloadDay(now)

		s.partitionsLock.Lock()

		// Offload partitions.
		// s.partitions are sorted by day, so find the first non-expired partition.
		n := sort.Search(len(s.partitions), func(i int) bool {
			return s.partitions[i].day >= offloadDay
		})
		ptwsToOffload := s.partitions[:n]

		// Remove reference to offloaded partitions from s.ptwHot
		if slices.Contains(ptwsToOffload, s.ptwHot) {
			s.ptwHot = nil
		}

		s.partitionsLock.Unlock()

		maxBackfillAge := s.maxBackfillAge.Load()
		for _, ptw := range ptwsToOffload {
			if ptw.pt.isRemote {
				continue
			}
			now := time.Now().UnixNano()
			ptMaxBackfillAge := now - (ptw.day+1)*nsecsPerDay
			if maxBackfillAge > ptMaxBackfillAge {
				maxBackfillAge = ptMaxBackfillAge
				s.maxBackfillAge.Store(maxBackfillAge)
			}
			logger.Infof("the partition %s is scheduled to be offloaded because it is outside the -offloadPeriod=%dd", ptw.pt.path, durationToDays(s.offload))
			if err := s.offloadPartition(ptw); err != nil {
				if needStop(s.stopCh) {
					return
				}
				logger.Errorf("failed to offload partition %q: %s", ptw.pt.name, err)
				continue
			}
			logger.Infof("successfully offloaded partition %q", ptw.pt.name)
			ptw.mustDrop.Store(true)
			ptw.releaseRefs()
		}

		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (s *Storage) watchRetention() {
	d := timeutil.AddJitterToDuration(time.Hour)
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		s.dropStalePartitions()

		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (s *Storage) dropStalePartitions() {
	now := time.Now().UnixNano()
	minAllowedDay := s.getMinAllowedDay(now)

	s.partitionsLock.Lock()

	// Delete outdated partitions.
	// s.partitions are sorted by day, so find the first non-expired partition.
	n := sort.Search(len(s.partitions), func(i int) bool {
		return s.partitions[i].day >= minAllowedDay
	})
	ptwsToDelete := s.partitions[:n]
	s.partitions = s.partitions[n:]
	s.updateDeletedPartitionsLocked(ptwsToDelete)

	// Remove reference to deleted partitions from s.ptwHot
	if slices.Contains(ptwsToDelete, s.ptwHot) {
		s.ptwHot = nil
	}

	s.partitionsLock.Unlock()

	for i, ptw := range ptwsToDelete {
		logger.Infof("the partition %s is scheduled to be deleted because it is outside the -retentionPeriod=%dd", ptw.pt.path, durationToDays(s.retention))
		ptw.mustDrop.Store(true)
		ptw.releaseRefs()
		ptwsToDelete[i] = nil
	}
}

func (s *Storage) watchMaxDiskSpaceUsage() {
	d := timeutil.AddJitterToDuration(10 * time.Second)
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		// Determine dynamic limit in bytes
		var limitBytes uint64
		if s.maxDiskSpaceUsageBytes > 0 {
			limitBytes = uint64(s.maxDiskSpaceUsageBytes)
		} else if s.maxDiskUsagePercent > 0 {
			total := fs.MustGetTotalSpace(s.path)
			if total > 0 {
				limitBytes = (total * uint64(s.maxDiskUsagePercent)) / 100
			}
		}
		if limitBytes == 0 {
			// Nothing to enforce
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				continue
			}
		}

		s.partitionsLock.Lock()
		var n uint64
		ptws := s.partitions
		var ptwsToDelete []*partitionWrapper
		for i := len(ptws) - 1; i >= 0; i-- {
			ptw := ptws[i]
			var ps PartitionStats
			ptw.pt.updateStats(&ps)
			n += ps.IndexdbSizeBytes + ps.CompressedSmallPartSize + ps.CompressedBigPartSize
			if n <= limitBytes {
				continue
			}
			if i >= len(ptws)-2 {
				// Keep the last two per-day partitions, so logs could be queried for one day time range.
				continue
			}

			// ptws are sorted by time, so just drop all the partitions until i, including i.
			i++
			ptwsToDelete = ptws[:i]
			s.partitions = ptws[i:]
			s.updateDeletedPartitionsLocked(ptwsToDelete)

			// Remove reference to deleted partitions from s.ptwHot
			if slices.Contains(ptwsToDelete, s.ptwHot) {
				s.ptwHot = nil
			}

			break
		}

		s.partitionsLock.Unlock()

		var reason string
		if s.maxDiskSpaceUsageBytes > 0 {
			reason = fmt.Sprintf("-retention.maxDiskSpaceUsageBytes=%d", s.maxDiskSpaceUsageBytes)
		} else {
			reason = fmt.Sprintf("-retention.maxDiskUsagePercent=%d%%", s.maxDiskUsagePercent)
		}

		for i, ptw := range ptwsToDelete {
			logger.Infof("the partition %s is scheduled to be deleted because the total size of partitions exceeds %s", ptw.pt.path, reason)
			ptw.mustDrop.Store(true)
			ptw.releaseRefs()
			ptwsToDelete[i] = nil
		}

		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (s *Storage) watchSnapshotsMaxAge() {
	if s.snapshotsMaxAge <= 0 {
		return
	}

	d := timeutil.AddJitterToDuration(time.Minute)
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}

		s.MustDeleteStalePartitionSnapshots(s.snapshotsMaxAge)
	}
}

func (s *Storage) watchDeleteTasks() {
	d := timeutil.AddJitterToDuration(time.Second)
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}

		var dt *DeleteTask

		s.deleteTasksLock.Lock()
		if len(s.deleteTasks) > 0 {
			dt = s.deleteTasks[0]

			// initialize dt.ctx and dt.cancel under the lock in order to avoid races
			// with canceling the task at Storage.DeleteStopTask()
			dt.ctx, dt.cancel = contextutil.NewStopChanContext(s.stopCh)
			dt.doneCh = make(chan struct{})
		}
		s.deleteTasksLock.Unlock()

		if dt == nil {
			// There are no delete tasks.
			continue
		}

		// Process delete tasks sequentially in order to limit resource usage needed for the logs' deletion.

		ok := s.processDeleteTask(dt.ctx, dt)
		close(dt.doneCh)
		dt.cancel()

		s.deleteTasksLock.Lock()

		// Set dt.ctx and dt.cancel to nil under the lock in order to avoid races
		// with canceling the task at Storage.DeleteStopTask().
		dt.ctx = nil
		dt.cancel = nil
		dt.doneCh = nil

		s.deleteTasks = s.deleteTasks[1:]
		if !ok {
			// The delete task couldn't be completed now. Try it later.
			s.deleteTasks = append(s.deleteTasks, dt)
		}
		s.mustSaveDeleteTasksLocked()

		s.deleteTasksLock.Unlock()
	}
}

// processDeleteTask processes dt.
//
// true is returned on successfully processed dt or on explicitly canceled dt.
// false is returned if dt couldn't be processed at the moment, so it must be processed later.
func (s *Storage) processDeleteTask(ctx context.Context, dt *DeleteTask) bool {
	logger.Infof("started processing delete task %s", dt)
	startTime := time.Now()
	now := dt.StartTime.UnixNano()

	f, err := ParseFilterAtTimestamp(dt.Filter, now)
	if err != nil {
		logger.Panicf("BUG: cannot parse filter from delete task: [%s]", dt.Filter)
	}

	q := &Query{
		f:         f.f,
		timestamp: dt.StartTime.UnixNano(),
	}

	// Add time filter ending at now in order to avoid deleting logs from the future.
	q.AddTimeFilter(math.MinInt64, now)

	var qs QueryStats
	qctx := NewQueryContext(ctx, &qs, dt.TenantIDs, q, false, nil)

	// Initialize subqueries
	qNew, err := initSubqueries(qctx, s.runQuery, false)
	if err != nil {
		logger.Errorf("cannot process delete task with task_id=%q while initializing subqueries: %s; retrying later", dt.TaskID, err)
		return false
	}
	q = qNew

	sso := s.getSearchOptions(dt.TenantIDs, q, qctx.HiddenFieldsFilters)

	// reset fieldsFilter in order to avoid loading all the log fields
	// during search for parts which contain rows to delete, since these fields aren't needed.
	sso.fieldsFilter.Reset()

	// delete rows matching q.f
	stopCh := ctx.Done()
	if !s.deleteRows(sso, stopCh) {
		if needStop(s.stopCh) {
			logger.Infof("the storage is stopped while executing the delete task with task_id=%q; postponing the task for later execution", dt.TaskID)
			return false
		}

		if needStop(stopCh) {
			// The task has been canceled explicitly. Return true, so it isn't re-scheduled for later execution.
			logger.Infof("the delete task with task_id=%q is explicitly canceled after %.3f seconds", dt.TaskID, time.Since(startTime).Seconds())
			return true
		}

		// The task couldn't be processed at the moment
		logger.Warnf("cannot proceed with the delete task with task_id=%q in %.3f seconds; retrying it later", dt.TaskID, time.Since(startTime).Seconds())
		return false
	}

	logger.Infof("finished processing delete task %s in %.3f seconds", dt, time.Since(startTime).Seconds())
	return true
}

func (s *Storage) deleteRows(sso *storageSearchOptions, stopCh <-chan struct{}) bool {
	ptws, _, ptwsDecRef := s.getPartitionsForTimeRange(sso.minTimestamp, sso.maxTimestamp, true)
	defer ptwsDecRef()

	// Delete rows sequentially in every partition in order to limit resource usage needed for the logs' deletion.
	ok := true
	for _, ptw := range ptws {
		if !ptw.pt.deleteRows(sso, stopCh) {
			// Return false if at least a single deletion was unsuccessful.
			// Continue deletion of rows at other partitions, since they may be successful.
			ok = false
		}
	}

	return ok
}

func (s *Storage) updateDeletedPartitionsLocked(ptwsToDelete []*partitionWrapper) {
	for _, ptw := range ptwsToDelete {
		if ptw == nil {
			continue
		}
		if !slices.Contains(s.deletedPartitions, ptw.day) {
			s.deletedPartitions = append(s.deletedPartitions, ptw.day)
		}
	}
}

func (s *Storage) getOffloadDay(now int64) int64 {
	return (now - s.offload.Nanoseconds()) / nsecsPerDay
}

func (s *Storage) getMinAllowedDay(now int64) int64 {
	return (now - s.retention.Nanoseconds()) / nsecsPerDay
}

func (s *Storage) getMaxAllowedDay(now int64) int64 {
	return (now + s.futureRetention.Nanoseconds()) / nsecsPerDay
}

// MustClose closes s.
//
// It is expected that nobody uses the storage at the close time.
func (s *Storage) MustClose() {
	// Stop background workers
	close(s.stopCh)
	s.wg.Wait()

	// Close partitions
	for _, pw := range s.partitions {
		pw.releaseRefs()
		if n := pw.writeRefCount.Load(); n != 0 {
			logger.Panicf("BUG: there are %d write users of partition", n)
		}
		if n := pw.totalRefCount.Load(); n != 0 {
			logger.Panicf("BUG: there are %d users of partition", n)
		}
	}
	s.partitions = nil
	s.ptwHot = nil

	// Stop caches

	// Do not persist caches, since they may become out of sync with partitions
	// if partitions are deleted, restored from backups or copied from other sources
	// between VictoriaLogs restarts. This may result in various issues
	// during data ingestion and querying.

	s.streamIDCache.MustStop()
	s.streamIDCache = nil

	s.filterStreamCache.MustStop()
	s.filterStreamCache = nil

	// release lock file
	fs.MustClose(s.flockF)
	s.flockF = nil

	if s.sc != nil {
		s.sc.Close()
	}

	s.path = ""
	s.sc = nil
}

// GetReadOnlyPartitions returns list of read only partition names that are starting with the given partitionPrefix.
func (s *Storage) GetReadOnlyPartitions(partitionPrefix string) []string {
	ptws := s.getReadPartitions()
	defer s.putReadPartitions(ptws)
	s.wg.Add(1)
	defer s.wg.Done()

	var readOnlyPartitions []string
	for _, ptw := range ptws {
		pt := ptw.pt
		if ptw.isReadOnly.Load() && strings.HasPrefix(pt.name, partitionPrefix) {
			readOnlyPartitions = append(readOnlyPartitions, pt.name)
		}
	}
	return readOnlyPartitions
}

// MustForceMerge force-merges parts in s partitions with names starting from the given partitionPrefix.
//
// Partitions are merged sequentially in order to reduce load on the system.
func (s *Storage) MustForceMerge(partitionPrefix string) {
	ptws := s.getWritePartitions()
	defer s.putWritePartitions(ptws)

	s.wg.Add(1)
	defer s.wg.Done()

	for _, ptw := range ptws {
		if !strings.HasPrefix(ptw.pt.name, partitionPrefix) {
			continue
		}

		logger.Infof("started force merge for partition %s", ptw.pt.name)
		startTime := time.Now()
		ptw.pt.mustForceMerge(false)
		logger.Infof("finished force merge for partition %s in %.3fs", ptw.pt.name, time.Since(startTime).Seconds())
	}
}

// MustAddRows adds lr to s.
//
// It is recommended checking whether the s is in read-only mode by calling IsReadOnly()
// before calling MustAddRows.
//
// The added rows become visible for search after small duration of time.
// Call DebugFlush if the added rows must be queried immediately (for example, in tests).
func (s *Storage) MustAddRows(lr *LogRows) {
	// Fast path - try adding all the rows to the hot partition
	s.partitionsLock.Lock()
	ptwHot := s.ptwHot
	if ptwHot != nil {
		ptwHot.incWriteRef()
	}
	s.partitionsLock.Unlock()

	if ptwHot != nil {
		if ptwHot.canAddAllRows(lr) {
			ptwHot.pt.mustAddRows(lr)
			ptwHot.decWriteRef()
			return
		}
		ptwHot.decWriteRef()
	}

	// Slow path - rows cannot be added to the hot partition, so split rows among available partitions
	now := time.Now().UnixNano()
	minAllowedDay := s.getMinAllowedDay(now)
	maxAllowedDay := s.getMaxAllowedDay(now)
	minAllowedTimestamp := now - s.maxBackfillAge.Load()

	m := make(map[int64]*LogRows)
	for i, ts := range lr.timestamps {
		day := ts / nsecsPerDay
		if day < minAllowedDay {
			line := MarshalFieldsToJSON(nil, lr.rows[i])
			tsf := TimeFormatter(ts)
			minAllowedTsf := TimeFormatter(minAllowedDay * nsecsPerDay)
			tooSmallTimestampLogger.Warnf("skipping log entry with too small timestamp=%s; it must be bigger than %s according "+
				"to the configured -retentionPeriod=%dd. See https://docs.victoriametrics.com/victorialogs/#retention ; "+
				"log entry: %s", &tsf, &minAllowedTsf, durationToDays(s.retention), line)
			s.rowsDroppedTooSmallTimestamp.Add(1)
			continue
		}
		if day > maxAllowedDay {
			line := MarshalFieldsToJSON(nil, lr.rows[i])
			tsf := TimeFormatter(ts)
			maxAllowedTsf := TimeFormatter(maxAllowedDay * nsecsPerDay)
			tooBigTimestampLogger.Warnf("skipping log entry with too big timestamp=%s; it must be smaller than %s according "+
				"to the configured -futureRetention=%dd; see https://docs.victoriametrics.com/victorialogs/#retention ; "+
				"log entry: %s", &tsf, &maxAllowedTsf, durationToDays(s.futureRetention), line)
			s.rowsDroppedTooBigTimestamp.Add(1)
			continue
		}
		if ts < minAllowedTimestamp {
			line := MarshalFieldsToJSON(nil, lr.rows[i])
			tsf := TimeFormatter(ts)
			minAllowedTsf := TimeFormatter(minAllowedTimestamp)
			tooSmallTimestampLogger.Warnf("skipping log entry with too small timestamp=%s; it must be bigger than %s according "+
				"to the configured -maxBackfillAge=%s. See https://docs.victoriametrics.com/victorialogs/#backfilling ; "+
				"log entry: %s", &tsf, &minAllowedTsf, time.Duration(s.maxBackfillAge.Load()), line)
			s.rowsDroppedTooSmallTimestamp.Add(1)
			continue
		}

		lrPart := m[day]
		if lrPart == nil {
			lrPart = GetLogRows(nil, nil, nil, nil, "")
			m[day] = lrPart
		}
		lrPart.mustAddInternal(lr.streamIDs[i], ts, lr.rows[i], lr.streamTagsCanonicals[i])
	}
	for day, lrPart := range m {
		ptw := s.getPartitionForWriting(day)
		if ptw != nil {
			ptw.pt.mustAddRows(lrPart)
			ptw.decWriteRef()
		} else {
			// the lrPart must contain at least a single row, so log it.
			line := MarshalFieldsToJSON(nil, lrPart.rows[0])
			inactivePartitionLogger.Warnf("skipping log entry because it cannot be saved into inactive per-day partition; "+
				"see https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle; log entry %s", line)
		}
		PutLogRows(lrPart)
	}
}

var tooSmallTimestampLogger = logger.WithThrottler("too_small_timestamp", 5*time.Second)
var tooBigTimestampLogger = logger.WithThrottler("too_big_timestamp", 5*time.Second)
var inactivePartitionLogger = logger.WithThrottler("inactive_partition", 5*time.Second)

// TimeFormatter implements fmt.Stringer for timestamp in nanoseconds
type TimeFormatter int64

// String returns human-readable representation for tf.
func (tf *TimeFormatter) String() string {
	ts := int64(*tf)
	t := time.Unix(0, ts).UTC()
	return t.Format(time.RFC3339Nano)
}

// getPartitionForWriting returns the partition for the given day for writing.
//
// The partition is automatically created if it didn't exist.
//
// nil is returned in the following cases:
//
//   - When the partition is outside the configured retention.
//   - When the partition has been detached via Storage.PartitionDetach().
//   - When the partition directory has been manually added, but wasn't attached yet via Storage.PartitionAttach().
//
// The caller must log this case and drop pending logs for this partition.
func (s *Storage) getPartitionForWriting(day int64) *partitionWrapper {
	s.partitionsLock.Lock()
	defer s.partitionsLock.Unlock()

	// Search for the partition using binary search
	ptws := s.partitions
	n := sort.Search(len(ptws), func(i int) bool {
		return ptws[i].day >= day
	})
	var ptw *partitionWrapper
	if n < len(ptws) {
		ptw = ptws[n]
		if ptw.day != day {
			ptw = nil
		} else if ptw.isReadOnly.Load() {
			return nil
		}
	}
	if ptw == nil {
		// Missing partition for the given day.
		if slices.Contains(s.deletedPartitions, day) {
			// The partition has been already deleted.
			return nil
		}

		fname := getPartitionNameFromDay(day)
		partitionPath := filepath.Join(s.path, partitionsDirname, fname)
		if fs.IsPathExist(partitionPath) {
			// The partition directory exists. This can happen in the following cases:
			// - When the partition directory has been manually added, but it wasn't attached yet via Storage.PartitionAttach().
			// - When the partition has been detached via Storage.PartitionDetach().
			return nil
		}

		// Create missing partition.
		mustCreatePartition(partitionPath)
		pt := mustOpenLocalPartition(s, partitionPath)
		ptw = newPartitionWrapper(pt, day, false)
		if n == len(ptws) {
			ptws = append(ptws, ptw)
		} else {
			ptws = append(ptws[:n+1], ptws[n:]...)
			ptws[n] = ptw
		}
		s.partitions = ptws
	}

	s.ptwHot = ptw
	ptw.incWriteRef()

	return ptw
}

// UpdateStats updates ss for the given s.
func (s *Storage) UpdateStats(ss *StorageStats) {
	ss.RowsDroppedTooBigTimestamp += s.rowsDroppedTooBigTimestamp.Load()
	ss.RowsDroppedTooSmallTimestamp += s.rowsDroppedTooSmallTimestamp.Load()
	if s.maxDiskSpaceUsageBytes > 0 {
		ss.MaxDiskSpaceUsageBytes = s.maxDiskSpaceUsageBytes
	} else {
		ss.MaxDiskSpaceUsageBytes = int64(fs.MustGetTotalSpace(s.path) * uint64(s.maxDiskUsagePercent) / 100)
	}
	// Use sentinel values to indicate unbounded / no data for consistency
	ss.MinTimestamp, ss.MaxTimestamp = math.MinInt64, math.MaxInt64

	s.partitionsLock.Lock()
	ss.PartitionsCount += uint64(len(s.partitions))
	for _, ptw := range s.partitions {
		ptw.pt.updateStats(&ss.PartitionStats)
	}

	if len(s.partitions) > 0 {
		p0 := s.partitions[0]
		pLast := s.partitions[len(s.partitions)-1]

		ss.MinTimestamp, _ = p0.pt.ddb.getMinMaxTimestamps()
		_, ss.MaxTimestamp = pLast.pt.ddb.getMinMaxTimestamps()
	}
	s.partitionsLock.Unlock()

	ss.IsReadOnly = s.IsReadOnly()
}

// IsReadOnly returns true if s is in read-only mode.
func (s *Storage) IsReadOnly() bool {
	available := fs.MustGetFreeSpace(s.path)
	return available < s.minFreeDiskSpaceBytes
}

// DebugFlush flushes all the buffered rows, so they become visible for search.
//
// This function is for debugging and testing purposes only, since it is slow.
func (s *Storage) DebugFlush() {
	ptws := s.getWritePartitions()
	defer s.putWritePartitions(ptws)

	for _, ptw := range ptws {
		ptw.pt.debugFlush()
	}
}

func (s *Storage) getWritePartitions() []*partitionWrapper {
	s.partitionsLock.Lock()
	defer s.partitionsLock.Unlock()
	ptws := make([]*partitionWrapper, 0, len(s.partitions))
	for _, ptw := range s.partitions {
		if ptw.isReadOnly.Load() {
			continue
		}
		ptw.incWriteRef()
		ptws = append(ptws, ptw)
	}
	return ptws
}

func (s *Storage) putWritePartitions(ptws []*partitionWrapper) {
	for _, ptw := range ptws {
		ptw.decWriteRef()
	}
}

func (s *Storage) getReadPartitions() []*partitionWrapper {
	s.partitionsLock.Lock()
	defer s.partitionsLock.Unlock()
	ptws := make([]*partitionWrapper, 0, len(s.partitions))
	for _, ptw := range s.partitions {
		ptw.incReadRef()
		ptws = append(ptws, ptw)
	}
	return ptws
}

func (s *Storage) putReadPartitions(ptws []*partitionWrapper) {
	for _, ptw := range ptws {
		ptw.decReadRef()
	}
}

func durationToDays(d time.Duration) int64 {
	return int64(d / (time.Hour * 24))
}
