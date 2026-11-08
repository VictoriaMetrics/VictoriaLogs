package logstorage

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/snapshot/snapshotutil"
)

// PartitionStats contains stats for the partition.
type PartitionStats struct {
	DatadbStats
	IndexdbStats
}

type partition struct {
	// s is the parent storage for the partition
	s *Storage

	// path is the path to the partition directory
	path string

	// name is the partition name. It is basically the directory name obtained from path.
	// It is used for creating keys for partition caches.
	name string

	// idb is indexdb used for the given partition
	idb *indexdb

	// ddb is the datadb used for the given partition
	ddb *datadb

	// isReadOnly is set if the partition is opened in read-only mode.
	//
	// Read-only partitions can be queried, but they don't accept new logs,
	// don't run background merges and aren't modified on disk in any other way.
	// See https://docs.victoriametrics.com/victorialogs/#read-only-partitions
	isReadOnly atomic.Bool

	// The snapshotLock prevents from concurrent creation of snapshots,
	// since this may result in snapshots without recently added data,
	// which may be in the process of flushing to disk by concurrently running
	// snapshot process.
	snapshotLock sync.Mutex
}

// mustCreatePartition creates a partition at the given path.
//
// The created partition can be opened with mustOpenPartition() after is has been created.
//
// The created partition can be deleted with mustDeletePartition() when it is no longer needed.
func mustCreatePartition(path string) {
	fs.MustMkdirFailIfExist(path)

	indexdbPath := filepath.Join(path, indexdbDirname)
	mustCreateIndexdb(indexdbPath)

	datadbPath := filepath.Join(path, datadbDirname)
	mustCreateDatadb(datadbPath)

	fs.MustSyncPathAndParentDir(path)
}

// mustDeletePartition deletes partition at the given path.
//
// The partition must be closed with MustClose before deleting it.
func mustDeletePartition(path string) {
	fs.MustRemoveDir(path)
}

// mustOpenPartition opens partition at the given path for the given Storage.
//
// If isReadOnly is set, then the partition is opened in read-only mode - it doesn't accept new logs,
// doesn't run background merges and isn't modified on disk in any other way.
// The partition directory must pass checkReadOnlyPartitionDir() in this case.
//
// The returned partition must be closed when no longer needed with mustClosePartition() call.
func mustOpenPartition(s *Storage, path string, isReadOnly bool) *partition {
	name := filepath.Base(path)

	indexdbPath := filepath.Join(path, indexdbDirname)
	datadbPath := filepath.Join(path, datadbDirname)

	if isReadOnly {
		// Do not try fixing the partition directory, since it must be left intact. Verify it instead.
		if err := checkReadOnlyPartitionDir(path); err != nil {
			logger.Panicf("FATAL: cannot open the partition %s in read-only mode: %s; remove the partition name from %s in order to open it in read-write mode",
				path, err, filepath.Join(s.path, readOnlyPartitionsFilename))
		}
	} else {
		isIndexDBExist := fs.IsPathExist(indexdbPath)
		isDatadbExist := fs.IsPathExist(datadbPath)

		if !isIndexDBExist {
			if isDatadbExist {
				logger.Panicf("FATAL: indexdb directory %s is missing, but datadb directory %s exists. "+
					"This indicates corruption. Manually remove the %s partition to resolve it (partition data will be lost)",
					indexdbPath, datadbPath, path)
			}

			logger.Warnf("creating missing indexdb directory %s, this could happen if VictoriaLogs shuts down uncleanly "+
				"(via OOM crash, a panic, SIGKILL or hardware shutdown) while creating new per-day partition", indexdbPath)
			mustCreateIndexdb(indexdbPath)
		}

		if !isDatadbExist {
			logger.Warnf("creating missing datadb directory %s, this could happen if VictoriaLogs shuts down uncleanly "+
				"(via OOM crash, a panic, SIGKILL or hardware shutdown) while creating new per-day partition", datadbPath)
			mustCreateDatadb(datadbPath)
		}
	}

	// Start initializing the partition
	pt := &partition{
		s:    s,
		path: path,
		name: name,
	}
	pt.isReadOnly.Store(isReadOnly)

	pt.idb = mustOpenIndexdb(indexdbPath, name, s, &pt.isReadOnly)
	pt.ddb = mustOpenDatadb(pt, datadbPath, s.flushInterval)

	return pt
}

// checkReadOnlyPartitionDir verifies that the partition directory at the given path can be opened in read-only mode.
//
// Partitions opened in read-write mode automatically fix the missing files and directories listed below,
// while read-only partitions must be left intact, so the fix cannot be applied to them.
func checkReadOnlyPartitionDir(path string) error {
	indexdbPath := filepath.Join(path, indexdbDirname)
	datadbPath := filepath.Join(path, datadbDirname)
	for _, p := range []string{
		indexdbPath,
		filepath.Join(indexdbPath, partsFilename),
		datadbPath,
		filepath.Join(datadbPath, partsFilename),
	} {
		if !fs.IsPathExist(p) {
			return fmt.Errorf("missing %s", p)
		}
	}
	return nil
}

// mustClosePartition closes pt.
//
// The caller must ensure that pt is no longer used before the call to mustClosePartition().
//
// The partition can be deleted if needed after it is closed via mustDeletePartition() call.
func mustClosePartition(pt *partition) {
	// Close datadb before indexdb, since datadb background merges may use indexdb until mustCloseDatadb() returns.
	mustCloseDatadb(pt.ddb)
	pt.ddb = nil

	// Close indexdb
	mustCloseIndexdb(pt.idb)
	pt.idb = nil

	pt.name = ""
	pt.path = ""
	pt.s = nil
}

func (pt *partition) mustAddRows(lr *LogRows) {
	if pt.isReadOnly.Load() {
		logger.Panicf("BUG: cannot add rows to read-only partition %s", pt.path)
	}

	// Register rows in indexdb
	var pendingRows []int
	streamIDs := lr.streamIDs
	for i := range lr.timestamps {
		streamID := &streamIDs[i]
		if pt.hasStreamIDInCache(streamID) {
			continue
		}
		if len(pendingRows) == 0 || !streamIDs[pendingRows[len(pendingRows)-1]].equal(streamID) {
			pendingRows = append(pendingRows, i)
		}
	}
	if len(pendingRows) > 0 {
		logNewStreams := pt.s.logNewStreams.Load()
		streamTagsCanonicals := lr.streamTagsCanonicals
		sort.Slice(pendingRows, func(i, j int) bool {
			return streamIDs[pendingRows[i]].less(&streamIDs[pendingRows[j]])
		})
		for i, rowIdx := range pendingRows {
			streamID := &streamIDs[rowIdx]
			if i > 0 && streamIDs[pendingRows[i-1]].equal(streamID) {
				continue
			}
			if pt.hasStreamIDInCache(streamID) {
				continue
			}
			if !pt.idb.hasStreamID(streamID) {
				streamTagsCanonical := streamTagsCanonicals[rowIdx]
				pt.idb.mustRegisterStream(streamID, streamTagsCanonical)
				if logNewStreams {
					pt.logNewStream(streamTagsCanonical, lr.rows[rowIdx])
				}
			}
			pt.putStreamIDToCache(streamID)
		}
	}

	// Add rows to datadb
	pt.ddb.mustAddRows(lr)
	if pt.s.logIngestedRows {
		pt.logIngestedRows(lr)
	}
}

func (pt *partition) logNewStream(streamTagsCanonical string, fields []Field) {
	streamTags := getStreamTagsString(streamTagsCanonical)
	line := MarshalFieldsToJSON(nil, fields)
	logger.Infof("partition %s: new stream %s for log entry %s", pt.path, streamTags, line)
}

func (pt *partition) logIngestedRows(lr *LogRows) {
	for i := range lr.rows {
		s := lr.GetRowString(i)
		logger.Infof("partition %s: new log entry %s", pt.path, s)
	}
}

func (pt *partition) hasStreamIDInCache(sid *streamID) bool {
	bb := bbPool.Get()
	bb.B = pt.marshalStreamIDCacheKey(bb.B, sid)
	_, ok := pt.s.streamIDCache.Get(bb.B)
	bbPool.Put(bb)

	return ok
}

func (pt *partition) putStreamIDToCache(sid *streamID) {
	bb := bbPool.Get()
	bb.B = pt.marshalStreamIDCacheKey(bb.B, sid)
	pt.s.streamIDCache.Set(bb.B, nil)
	bbPool.Put(bb)
}

func (pt *partition) marshalStreamIDCacheKey(dst []byte, sid *streamID) []byte {
	dst = encoding.MarshalUint64(dst, pt.s.partitionCacheGeneration.Load())
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(pt.name))
	dst = sid.marshal(dst)
	return dst
}

// debugFlush makes sure that all the recently ingested data data becomes searchable
func (pt *partition) debugFlush() {
	pt.ddb.debugFlush()
	pt.idb.debugFlush()
}

// mustCreateSnapshot creates snapshot for the the given pt and returns full path to the created snapshot.
func (pt *partition) mustCreateSnapshot() string {
	logger.Infof("creating a snapshot for partition %q", pt.name)
	startTime := time.Now()

	pt.snapshotLock.Lock()
	defer pt.snapshotLock.Unlock()

	snapshotName := snapshotutil.NewName()
	dstDir := filepath.Join(pt.path, snapshotsDirname, snapshotName)
	fs.MustMkdirFailIfExist(dstDir)

	dstIndexdbDir := filepath.Join(dstDir, indexdbDirname)
	pt.idb.mustCreateSnapshotAt(dstIndexdbDir)

	dstDatadbDir := filepath.Join(dstDir, datadbDirname)
	pt.ddb.mustCreateSnapshotAt(dstDatadbDir)

	fs.MustSyncPathAndParentDir(dstDir)

	logger.Infof("created a snapshot for partition %q at %q in %.3f seconds", pt.name, dstDir, time.Since(startTime).Seconds())

	return dstDir
}

// deleteSnapshot removes the snapshot with the given snapshotName from the pt.
func (pt *partition) deleteSnapshot(snapshotName string) error {
	logger.Infof("deleting snapshot %q for partition %q", snapshotName, pt.name)

	pt.snapshotLock.Lock()
	defer pt.snapshotLock.Unlock()

	snapshotPath := filepath.Join(pt.path, snapshotsDirname, snapshotName)
	if !fs.IsPathExist(snapshotPath) {
		return fmt.Errorf("snapshot %q doesn't exist at %q", snapshotName, pt.path)
	}

	fs.MustRemoveDir(snapshotPath)

	logger.Infof("deleted snapshot %q for partition %q at %q", snapshotName, pt.name, snapshotPath)

	return nil
}

func (pt *partition) updateStats(ps *PartitionStats) {
	pt.ddb.updateStats(&ps.DatadbStats)
	pt.idb.updateStats(&ps.IndexdbStats)
}

// mustForceMerge runs forced merge for all the parts in pt.
func (pt *partition) mustForceMerge() {
	pt.ddb.mustForceMergeAllParts()
}

func (pt *partition) deleteRows(sso *storageSearchOptions, stopCh <-chan struct{}) bool {
	// make recently ingested rows visible for search, so they could be deleted.
	pt.debugFlush()

	pso := pt.getSearchOptions(sso)
	return pt.ddb.deleteRows(pso, stopCh)
}

func getPartitionDayFromName(name string) (int64, error) {
	t, err := time.Parse(partitionNameFormat, name)
	if err != nil {
		return 0, fmt.Errorf("cannot parse partition name %q; it must have the format YYYYMMDD: %w", name, err)
	}
	day := t.UTC().UnixNano() / nsecsPerDay
	return day, nil
}

func getPartitionNameFromDay(day int64) string {
	name := time.Unix(0, day*nsecsPerDay).UTC().Format(partitionNameFormat)
	return name
}

const partitionNameFormat = "20060102"
