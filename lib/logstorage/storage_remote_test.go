package logstorage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/objectstorage/testutil"
)

func TestMustOpenRemotePartition(t *testing.T) {
	const rowsCount = 50
	sc, partitionPath := buildRemotePartitionSnapshot(t, rowsCount)

	s := newTestStorageForRemote(t, sc)
	pt, _ := mustOpenRemotePartition(s, partitionPath)
	defer mustClosePartition(pt)

	var ps PartitionStats
	pt.updateStats(&ps)
	if got := ps.RowsCount(); got != uint64(rowsCount) {
		t.Fatalf("RowsCount: got %d; want %d", got, rowsCount)
	}
}

func TestLoadRemotePartitions(t *testing.T) {
	const rowsCount = 50
	sc, _ := buildRemotePartitionSnapshot(t, rowsCount)

	s := newTestStorageForRemote(t, sc)
	ptws := s.mustLoadRemotePartitions(func(int64) bool { return false })

	var found *partitionWrapper
	for _, ptw := range ptws {
		if ptw != nil {
			found = ptw
			break
		}
	}
	if found == nil {
		t.Fatal("no partitions loaded")
	}
	defer found.releaseRefs()

	var ps PartitionStats
	found.pt.updateStats(&ps)
	if got := ps.RowsCount(); got != uint64(rowsCount) {
		t.Fatalf("RowsCount: got %d; want %d", got, rowsCount)
	}
}

func newTestStorageForRemote(t testing.TB, sc *testutil.StorageClient) *Storage {
	t.Helper()
	filterStreamCache := newCache()
	streamIDCache := newCache()
	t.Cleanup(func() {
		filterStreamCache.MustStop()
		streamIDCache.MustStop()
	})
	return &Storage{
		flushInterval:     time.Second,
		filterStreamCache: filterStreamCache,
		streamIDCache:     streamIDCache,
		stopCh:            make(chan struct{}),
		sc:                sc,
	}
}

type remoteQueryStorage struct {
	s         *Storage
	tenantIDs []TenantID
	startNs   int64
	endNs     int64
}

func buildRemoteQueryStorage(t testing.TB, rowsCount int) *remoteQueryStorage {
	t.Helper()

	tenantID := TenantID{AccountID: 1, ProjectID: 1}
	yesterdayNs := (time.Now().UnixNano()/nsecsPerDay - 1) * nsecsPerDay

	localPath := t.TempDir()
	cfg := &StorageConfig{Retention: 48 * time.Hour}
	s := MustOpenStorage(localPath, cfg)

	streamTags := []string{"host"}
	lr := GetLogRows(streamTags, nil, nil, nil, "")
	for i := range rowsCount {
		lr.mustAdd(tenantID, yesterdayNs+int64(i)*int64(time.Second), []Field{
			{Name: "host", Value: "server1"},
			{Name: "_msg", Value: fmt.Sprintf("message %d", i)},
		})
	}
	s.MustAddRows(lr)
	PutLogRows(lr)
	s.DebugFlush()

	partitionName := getPartitionNameFromDay(yesterdayNs / nsecsPerDay)
	s.MustClose()

	localPartitionPath := filepath.Join(localPath, partitionsDirname, partitionName)
	remotePartitionPath := filepath.Join(partitionsDirname, partitionName)

	sc := testutil.New()
	err := filepath.WalkDir(localPartitionPath, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(localPartitionPath, p)
		sc.SetFile(filepath.Join(remotePartitionPath, rel), data)
		return nil
	})
	if err != nil {
		t.Fatalf("walkDir: %v", err)
	}
	sc.SetFile(filepath.Join(remotePartitionPath, partitionReadyFilename), []byte{})

	filterStreamCache := newCache()
	streamIDCache := newCache()
	rs := &Storage{
		flushInterval:     time.Second,
		filterStreamCache: filterStreamCache,
		streamIDCache:     streamIDCache,
		stopCh:            make(chan struct{}),
		sc:                sc,
	}

	ptws := rs.mustLoadRemotePartitions(func(int64) bool { return false })
	for _, ptw := range ptws {
		if ptw != nil {
			rs.partitions = append(rs.partitions, ptw)
		}
	}
	sortPartitions(rs.partitions)

	t.Cleanup(func() {
		for _, ptw := range rs.partitions {
			ptw.releaseRefs()
		}
		filterStreamCache.MustStop()
		streamIDCache.MustStop()
	})

	return &remoteQueryStorage{
		s:         rs,
		tenantIDs: []TenantID{tenantID},
		startNs:   yesterdayNs,
		endNs:     yesterdayNs + nsecsPerDay - 1,
	}
}

func buildRemotePartitionSnapshot(t testing.TB, rowsCount int) (*testutil.StorageClient, string) {
	t.Helper()

	localPath := t.TempDir()
	cfg := &StorageConfig{Retention: 48 * time.Hour}
	s := MustOpenStorage(localPath, cfg)

	lr := newTestLogRows(1, rowsCount, 0)
	yesterdayNs := (time.Now().UnixNano()/nsecsPerDay - 1) * nsecsPerDay
	for i := range lr.timestamps {
		lr.timestamps[i] = yesterdayNs + int64(i)*int64(time.Second)
	}
	s.MustAddRows(lr)
	s.DebugFlush()

	partitionName := getPartitionNameFromDay(yesterdayNs / nsecsPerDay)
	s.MustClose()

	localPartitionPath := filepath.Join(localPath, partitionsDirname, partitionName)
	remotePartitionPath := filepath.Join(partitionsDirname, partitionName)

	sc := testutil.New()
	err := filepath.WalkDir(localPartitionPath, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(localPartitionPath, p)
		sc.SetFile(filepath.Join(remotePartitionPath, rel), data)
		return nil
	})
	if err != nil {
		t.Fatalf("walkDir: %v", err)
	}
	sc.SetFile(filepath.Join(remotePartitionPath, partitionReadyFilename), []byte{})

	return sc, remotePartitionPath
}

// sizeSyncClient mirrors Sync of the S3 client: a file whose remote copy has the same size isn't uploaded again.
type sizeSyncClient struct {
	*testutil.StorageClient

	// failCreateFile is the path of a single CreateFile call that fails.
	failCreateFile string

	// skipUploadSuffix makes the next Sync skip uploading files with this suffix.
	skipUploadSuffix string
}

func (c *sizeSyncClient) CreateFile(path string, data []byte) error {
	if path == c.failCreateFile {
		c.failCreateFile = ""
		return errors.New("injected CreateFile failure")
	}
	return c.StorageClient.CreateFile(path, data)
}

func (c *sizeSyncClient) Sync(src, dst string) error {
	skipSuffix := c.skipUploadSuffix
	c.skipUploadSuffix = ""

	remoteFiles, err := c.ListFiles(dst)
	if err != nil {
		return err
	}
	err = filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		remoteSize, ok := remoteFiles[rel]
		delete(remoteFiles, rel)
		if ok && remoteSize == uint64(len(data)) {
			return nil
		}
		if skipSuffix != "" && strings.HasSuffix(rel, skipSuffix) {
			return nil
		}
		c.SetFile(filepath.Join(dst, rel), data)
		return nil
	})
	if err != nil {
		return err
	}
	for rel := range remoteFiles {
		if err := c.DeletePath(filepath.Join(dst, rel), false); err != nil {
			return err
		}
	}
	return nil
}

// newTestStorageForOffload returns storage with rowsCount rows in the partition for 3 days ago,
// which uses sc as object storage, together with that partition.
//
// The caller must call decReadRef on the returned partition.
func newTestStorageForOffload(t *testing.T, sc *sizeSyncClient, rowsCount int) (*Storage, *partitionWrapper) {
	t.Helper()

	s := MustOpenStorage(t.TempDir(), &StorageConfig{Retention: 7 * 24 * time.Hour})
	t.Cleanup(s.MustClose)

	lr := newTestLogRows(1, rowsCount, 0)
	day := time.Now().UnixNano()/nsecsPerDay - 3
	for i := range lr.timestamps {
		lr.timestamps[i] = day*nsecsPerDay + int64(i)*int64(time.Second)
	}
	s.MustAddRows(lr)
	s.DebugFlush()
	s.sc = sc

	ptw := getTestPartition(s, day)
	if ptw == nil {
		t.Fatalf("cannot find partition for day %d", day)
	}
	ptw.incReadRef()
	return s, ptw
}

func getTestPartition(s *Storage, day int64) *partitionWrapper {
	s.partitionsLock.Lock()
	defer s.partitionsLock.Unlock()
	for _, ptw := range s.partitions {
		if ptw.day == day {
			return ptw
		}
	}
	return nil
}

// TestOffloadPartitionRetryAfterFailure verifies that a partition offloaded on the second attempt can be opened from object storage.
//
// Every attempt force-merges the partition, so its single datadb part gets a new name, while the size of parts.json stays the same.
func TestOffloadPartitionRetryAfterFailure(t *testing.T) {
	const rowsCount = 50

	sc := &sizeSyncClient{StorageClient: testutil.New()}
	s, ptw := newTestStorageForOffload(t, sc, rowsCount)
	defer ptw.decReadRef()

	partitionPath := filepath.Join(partitionsDirname, ptw.pt.name)
	sc.failCreateFile = filepath.Join(partitionPath, partitionReadyFilename)
	if err := s.offloadPartition(ptw); err == nil {
		t.Fatalf("expecting the first offload attempt to fail")
	}
	if err := s.offloadPartition(ptw); err != nil {
		t.Fatalf("unexpected error on the second offload attempt: %s", err)
	}

	rptw := getTestPartition(s, ptw.day)
	if rptw == nil || !rptw.pt.isRemote {
		t.Fatalf("expecting the partition to be replaced with the remote one")
	}
	var ps PartitionStats
	rptw.pt.updateStats(&ps)
	if got := ps.RowsCount(); got != rowsCount {
		t.Fatalf("unexpected rows count in the offloaded partition; got %d; want %d", got, rowsCount)
	}
}

// TestOffloadPartitionMissingPart verifies that offloading fails without the readiness marker
// when a part listed in parts.json didn't reach object storage, and succeeds on the next attempt.
func TestOffloadPartitionMissingPart(t *testing.T) {
	const rowsCount = 50

	sc := &sizeSyncClient{StorageClient: testutil.New()}
	s, ptw := newTestStorageForOffload(t, sc, rowsCount)
	defer ptw.decReadRef()

	partitionPath := filepath.Join(partitionsDirname, ptw.pt.name)
	partitionReadyFile := filepath.Join(partitionPath, partitionReadyFilename)

	sc.skipUploadSuffix = metadataFilename
	if err := s.offloadPartition(ptw); err == nil {
		t.Fatalf("expecting an error when parts are missing in object storage")
	}
	if ok, _ := sc.IsFileExist(partitionReadyFile); ok {
		t.Fatalf("unexpected readiness marker for the partition with missing parts")
	}
	if rptw := getTestPartition(s, ptw.day); rptw == nil || rptw.pt.isRemote {
		t.Fatalf("expecting the local partition to stay in place")
	}

	if err := s.offloadPartition(ptw); err != nil {
		t.Fatalf("unexpected error on the second offload attempt: %s", err)
	}
	if ok, _ := sc.IsFileExist(partitionReadyFile); !ok {
		t.Fatalf("missing readiness marker after successful offload")
	}
}
