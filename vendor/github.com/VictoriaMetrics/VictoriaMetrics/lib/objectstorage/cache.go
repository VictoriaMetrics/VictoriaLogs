package objectstorage

import (
	"container/list"
	"fmt"
	corefs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/objectstorage/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/metrics"
)

var (
	cacheHitsTotal      = metrics.NewCounter(`vm_objectstorage_cache_hits_total`)
	cacheMissesTotal    = metrics.NewCounter(`vm_objectstorage_cache_misses_total`)
	cacheEvictionsTotal = metrics.NewCounter(`vm_objectstorage_cache_evictions_total`)
)

type CachedStorageConfig struct {
	// MaxDiskSpaceUsageBytes is an optional maximum disk space cache can use.
	// The least used entries are automatically dropped if the total disk space usage exceeds this limit.
	MaxDiskSpaceUsageBytes int64
	// Path defines a path to cache
	Path string
}

const defaultChunkSize = 4 * 1024 * 1024

type cacheEntry struct {
	key      string
	path     string
	refCount atomic.Int32
	fd       *os.File
	size     int64
	listElem *list.Element
}

func (e *cacheEntry) incRef() {
	e.refCount.Add(1)
}

func (e *cacheEntry) decRef() {
	if n := e.refCount.Add(-1); n < 0 {
		logger.Panicf("BUG: negative refCount %d for cache entry %s", n, e.key)
	}
}

type CachedStorageClient struct {
	common.StorageClient
	path                   string
	maxDiskSpaceUsageBytes int64
	usedBytes              int64

	lru    *list.List
	chunks map[string]*cacheEntry
	mu     sync.Mutex

	group singleflight.Group

	// stopCh is closed when the Storage must be stopped.
	stopCh <-chan struct{}
}

func newCachedStorageClient(sc common.StorageClient, o CachedStorageConfig, stopCh <-chan struct{}) *CachedStorageClient {
	if sc == nil {
		logger.Fatalf("storage client is required")
	}
	if o.Path == "" {
		logger.Fatalf("cache path is not set")
	}
	if o.MaxDiskSpaceUsageBytes < 0 {
		logger.Fatalf("max disk space usage bytes must be >= 0")
	}
	fs.MustMkdirIfNotExist(o.Path)
	csc := CachedStorageClient{
		StorageClient:          sc,
		path:                   o.Path,
		maxDiskSpaceUsageBytes: o.MaxDiskSpaceUsageBytes,
		chunks:                 make(map[string]*cacheEntry),
		stopCh:                 stopCh,
		lru:                    list.New(),
	}
	csc.loadFromDisk()
	go csc.watchMaxDiskSpaceUsage()
	return &csc
}

func (sc *CachedStorageClient) loadFromDisk() {
	type diskChunk struct {
		key   string
		path  string
		size  int64
		mtime time.Time
	}

	var found []diskChunk

	err := filepath.WalkDir(sc.path, func(p string, d corefs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("encountered error while scanning cache at %s: %w", p, err)
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(p, ".tmp") {
			if err := os.Remove(p); err != nil {
				return fmt.Errorf("cannot remove stale cache tmp file %s: %w", p, err)
			}
			return nil
		}
		if !strings.HasSuffix(p, ".chunk") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get cached file %s stats: %w", p, err)
		}

		rel, err := filepath.Rel(sc.path, p)
		if err != nil {
			return fmt.Errorf("failed to get cached file %s stats: %v", p, err)
		}

		found = append(found, diskChunk{
			key:   strings.TrimSuffix(rel, ".chunk"),
			path:  p,
			size:  info.Size(),
			mtime: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		logger.Panicf("failed to restore cache %s: %v", sc.path, err)
	}

	sort.Slice(found, func(i, j int) bool {
		return found[i].mtime.Before(found[j].mtime)
	})

	sc.mu.Lock()
	defer sc.mu.Unlock()

	for _, c := range found {
		fd, err := os.Open(c.path)
		if err != nil {
			logger.Panicf("cache scan: cannot open %s: %v", c.path, err)
		}
		e := &cacheEntry{key: c.key, path: c.path, fd: fd, size: c.size}
		e.listElem = sc.lru.PushFront(e)
		sc.chunks[c.key] = e
		sc.usedBytes += c.size
	}

	if len(found) > 0 {
		logger.Infof("cache: restored %d chunks (%.1f MiB) from disk",
			len(found), float64(sc.usedBytes)/(1<<20))
	}
}

func (sc *CachedStorageClient) watchMaxDiskSpaceUsage() {
	if sc.maxDiskSpaceUsageBytes == 0 {
		return
	}

	d := timeutil.AddJitterToDuration(10 * time.Second)
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-sc.stopCh:
			return
		case <-ticker.C:
		}
		sc.evict()
	}
}

func (sc *CachedStorageClient) evict() {
	sc.mu.Lock()
	for sc.usedBytes > sc.maxDiskSpaceUsageBytes {
		if !sc.evictFromLocked() {
			break
		}
	}
	sc.mu.Unlock()
}

func (sc *CachedStorageClient) evictFromLocked() bool {
	for elem := sc.lru.Back(); elem != nil; elem = elem.Prev() {
		e := elem.Value.(*cacheEntry)
		if e.refCount.Load() > 0 {
			continue
		}
		sc.lru.Remove(elem)
		delete(sc.chunks, e.key)
		sc.usedBytes -= e.size
		e.fd.Close()
		fs.MustRemovePath(e.path)
		cacheEvictionsTotal.Inc()
		return true
	}
	return false
}

func (sc *CachedStorageClient) ReadRange(key string, buf []byte, offset int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	var totalRead int64
	want := int64(len(buf))
	for totalRead < want {
		pos := offset + totalRead
		chunkIdx := pos / defaultChunkSize
		chunkStart := chunkIdx * defaultChunkSize

		chunkOff := pos - chunkStart
		toRead := min(want-totalRead, defaultChunkSize-chunkOff)

		n, err := sc.readChunk(key, chunkIdx, chunkStart, buf[totalRead:totalRead+toRead], chunkOff)
		totalRead += int64(n)
		if err != nil {
			return int(totalRead), err
		}
	}

	return int(totalRead), nil
}

func (sc *CachedStorageClient) readChunk(path string, chunkIdx, chunkStart int64, buf []byte, offset int64) (int, error) {
	e, err := sc.getEntry(path, chunkIdx, chunkStart)
	if err != nil {
		return 0, err
	}
	n, err := e.fd.ReadAt(buf, offset)
	e.decRef()
	return n, err
}

func (sc *CachedStorageClient) acquireEntry(chunkKey string) *cacheEntry {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	e, ok := sc.chunks[chunkKey]
	if !ok {
		return nil
	}
	sc.lru.MoveToFront(e.listElem)
	e.incRef()
	return e
}

func (sc *CachedStorageClient) getEntry(path string, chunkIdx, chunkStart int64) (*cacheEntry, error) {
	chunkKey := path + "/" + strconv.FormatInt(chunkIdx, 10)

	for {
		if e := sc.acquireEntry(chunkKey); e != nil {
			cacheHitsTotal.Inc()
			return e, nil
		}

		v, err, _ := sc.group.Do(chunkKey, func() (any, error) {
			sc.mu.Lock()
			_, ok := sc.chunks[chunkKey]
			sc.mu.Unlock()
			if ok {
				return nil, nil
			}
			return sc.download(path, chunkKey, chunkIdx, chunkStart)
		})
		if err != nil {
			return nil, err
		}
		if v != nil {
			// Entry was just downloaded; acquire it.
			// If it was immediately evicted, loop back to re-download.
			if e := sc.acquireEntry(chunkKey); e != nil {
				return e, nil
			}
		}
	}
}

func (sc *CachedStorageClient) download(key, chunkKey string, chunkIdx, chunkStart int64) (*cacheEntry, error) {
	cacheMissesTotal.Inc()

	dir := filepath.Join(sc.path, key)
	fs.MustMkdirIfNotExist(dir)

	bb := common.GetWriteAtBuffer()
	defer common.PutWriteAtBuffer(bb)
	bb.Grow(int(defaultChunkSize))
	bb.B = bb.B[:defaultChunkSize]

	written, err := sc.StorageClient.ReadRange(key, bb.B, chunkStart)
	if err != nil {
		return nil, fmt.Errorf("cannot read chunk %s: %w", chunkKey, err)
	}

	p := filepath.Join(dir, fmt.Sprintf("%d.chunk", chunkIdx))
	tmpPath := p + ".tmp"

	fd, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("cannot create temp file for chunk %s: %w", chunkKey, err)
	}

	// Write only the bytes received so the last chunk is not zero-padded.
	if _, err := fd.WriteAt(bb.B[:written], 0); err != nil {
		fd.Close()
		if err := os.Remove(tmpPath); err != nil {
			logger.Warnf("cannot remove temp file %s: %v", tmpPath, err)
		}
		return nil, fmt.Errorf("cannot write chunk %s to disk: %w", chunkKey, err)
	}

	// Sync before rename so loadFromDisk can trust that any committed .chunk file is complete.
	if err := fd.Sync(); err != nil {
		fd.Close()
		if err := os.Remove(tmpPath); err != nil {
			logger.Warnf("cannot remove temp file %s: %v", tmpPath, err)
		}
		return nil, fmt.Errorf("cannot sync chunk %s: %w", chunkKey, err)
	}

	fd.Close()

	if err := os.Rename(tmpPath, p); err != nil {
		if err := os.Remove(tmpPath); err != nil {
			logger.Warnf("cannot remove temp file %s: %v", tmpPath, err)
		}
		return nil, fmt.Errorf("cannot rename chunk %s: %w", chunkKey, err)
	}

	fd, err = os.Open(p)
	if err != nil {
		if err := os.Remove(p); err != nil {
			logger.Warnf("cannot remove chunk file %s: %v", p, err)
		}
		return nil, fmt.Errorf("cannot open chunk %s: %w", chunkKey, err)
	}

	e := &cacheEntry{key: chunkKey, path: p, fd: fd, size: int64(written)}

	sc.mu.Lock()
	e.listElem = sc.lru.PushFront(e)
	sc.chunks[chunkKey] = e
	sc.usedBytes += e.size
	sc.mu.Unlock()

	return e, nil
}
