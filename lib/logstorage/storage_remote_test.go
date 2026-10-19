package logstorage

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
