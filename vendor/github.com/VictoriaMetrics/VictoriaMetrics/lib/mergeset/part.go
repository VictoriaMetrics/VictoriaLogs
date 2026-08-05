package mergeset

import (
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/blockcache"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs/fsutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/objectstorage/common"
)

var idxbCache = blockcache.NewCache(getMaxIndexBlocksCacheSize)
var ibCache = blockcache.NewCache(getMaxInmemoryBlocksCacheSize)
var ibSparseCache = blockcache.NewCache(getMaxInmemoryBlocksSparseCacheSize)

// SetIndexBlocksCacheSize overrides the default size of indexdb/indexBlocks cache
func SetIndexBlocksCacheSize(size int) {
	maxIndexBlockCacheSize = size
}

func getMaxIndexBlocksCacheSize() int {
	maxIndexBlockCacheSizeOnce.Do(func() {
		if maxIndexBlockCacheSize <= 0 {
			maxIndexBlockCacheSize = int(0.10 * float64(memory.Allowed()))
		}
	})
	return maxIndexBlockCacheSize
}

var (
	maxIndexBlockCacheSize     int
	maxIndexBlockCacheSizeOnce sync.Once
)

// SetDataBlocksCacheSize overrides the default size of indexdb/dataBlocks cache
func SetDataBlocksCacheSize(size int) {
	maxInmemoryBlockCacheSize = size
}

func getMaxInmemoryBlocksCacheSize() int {
	maxInmemoryBlockCacheSizeOnce.Do(func() {
		if maxInmemoryBlockCacheSize <= 0 {
			maxInmemoryBlockCacheSize = int(0.25 * float64(memory.Allowed()))
		}
	})
	return maxInmemoryBlockCacheSize
}

// SetDataBlocksSparseCacheSize overrides the default size of indexdb/dataBlocksSparse cache
func SetDataBlocksSparseCacheSize(size int) {
	maxInmemorySparseMergeCacheSize = size
}

func getMaxInmemoryBlocksSparseCacheSize() int {
	maxInmemoryBlockSparseCacheSizeOnce.Do(func() {
		if maxInmemorySparseMergeCacheSize <= 0 {
			maxInmemorySparseMergeCacheSize = int(0.05 * float64(memory.Allowed()))
		}
	})
	return maxInmemorySparseMergeCacheSize
}

var (
	maxInmemoryBlockCacheSize     int
	maxInmemoryBlockCacheSizeOnce sync.Once

	maxInmemorySparseMergeCacheSize     int
	maxInmemoryBlockSparseCacheSizeOnce sync.Once
)

type part struct {
	ph partHeader

	path string

	size uint64

	mrs                []metaindexRow
	metaindexSizeBytes uint64

	indexFile fs.MustReadAtCloser
	itemsFile fs.MustReadAtCloser
	lensFile  fs.MustReadAtCloser
}

func mustOpenRemotePart(sc common.StorageClient, allPartFiles map[string]uint64, path, name string) *part {
	var size uint64

	getReader := func(p string) *common.MustReadCloser {
		lookupPath := filepath.Join(name, p)
		openPath := filepath.Join(path, name, p)
		if fileSize, ok := allPartFiles[lookupPath]; ok {
			size += fileSize
			return common.NewMustReadCloser(sc, openPath, fileSize)
		}
		logger.Panicf("FATAL: cannot locate part file %s", sc.GetPath(openPath))
		return nil
	}

	metaindexFile := getReader(metaindexFilename)
	indexFile := getReader(indexFilename)
	itemsFile := getReader(itemsFilename)
	lensFile := getReader(lensFilename)

	metadataLookupPath := filepath.Join(name, metadataFilename)
	metadataOpenPath := filepath.Join(path, name, metadataFilename)
	metadataSize, ok := allPartFiles[metadataLookupPath]
	if !ok {
		logger.Panicf("FATAL: cannot locate part header file %s", sc.GetPath(metadataOpenPath))
	}

	bb := common.GetWriteAtBuffer()
	defer common.PutWriteAtBuffer(bb)
	bb.Grow(int(metadataSize))
	bb.B = bb.B[:int(metadataSize)]
	if err := sc.ReadFile(metadataOpenPath, bb); err != nil {
		logger.Panicf("cannot get header file %s: %w", sc.GetPath(metadataOpenPath), err)
	}

	var ph partHeader
	if err := ph.readMetadata(bb.B); err != nil {
		logger.Panicf("cannot read metadata file %s: %w", sc.GetPath(metadataOpenPath), err)
	}

	return newPart(&ph, path, size, metaindexFile, indexFile, itemsFile, lensFile)
}

func mustOpenFilePart(path string) *part {
	var ph partHeader
	ph.mustReadLocalMetadata(path)

	metaindexPath := filepath.Join(path, metaindexFilename)
	metaindexFile := filestream.MustOpen(metaindexPath, true)
	metaindexSize := fs.MustFileSize(metaindexPath)

	// Open part files in parallel in order to speed up this process
	// on high-latency storage systems such as NFS or Ceph.

	var pe fsutil.ParallelExecutor

	indexPath := filepath.Join(path, indexFilename)
	itemsPath := filepath.Join(path, itemsFilename)
	lensPath := filepath.Join(path, lensFilename)

	var indexFile fs.MustReadAtCloser
	var indexSize uint64
	pe.Add(fs.NewMustReaderAtOpenerTask(indexPath, &indexFile, &indexSize))

	var itemsFile fs.MustReadAtCloser
	var itemsSize uint64
	pe.Add(fs.NewMustReaderAtOpenerTask(itemsPath, &itemsFile, &itemsSize))

	var lensFile fs.MustReadAtCloser
	var lensSize uint64
	pe.Add(fs.NewMustReaderAtOpenerTask(lensPath, &lensFile, &lensSize))

	pe.Run()

	size := metaindexSize + indexSize + itemsSize + lensSize
	return newPart(&ph, path, size, metaindexFile, indexFile, itemsFile, lensFile)
}

func newPart(ph *partHeader, path string, size uint64, metaindexReader filestream.ReadCloser, indexFile, itemsFile, lensFile fs.MustReadAtCloser) *part {
	mrs, err := unmarshalMetaindexRows(nil, metaindexReader)
	if err != nil {
		logger.Panicf("cannot unmarshal metaindexRows from %s: %w", path, err)
	}
	metaindexReader.MustClose()

	var p part
	p.path = path
	p.size = size
	p.mrs = mrs
	p.metaindexSizeBytes = metaindexSizeBytes(mrs)

	p.indexFile = indexFile
	p.itemsFile = itemsFile
	p.lensFile = lensFile

	p.ph.CopyFrom(ph)
	return &p
}

func (p *part) MustClose() {
	// Close files in parallel in order to speed up this process on storage systems with high latency
	// such as NFS or Ceph.
	var pe fsutil.ParallelExecutor
	pe.Add(fs.NewMustCloserTask(p.indexFile))
	pe.Add(fs.NewMustCloserTask(p.itemsFile))
	pe.Add(fs.NewMustCloserTask(p.lensFile))
	pe.Run()

	idxbCache.RemoveBlocksForPart(p)
	ibCache.RemoveBlocksForPart(p)
	ibSparseCache.RemoveBlocksForPart(p)
}

func metaindexSizeBytes(mrs []metaindexRow) uint64 {
	n := uint64(cap(mrs)) * uint64(unsafe.Sizeof(metaindexRow{}))
	for i := range mrs {
		n += uint64(cap(mrs[i].firstItem))
	}
	return n
}

type indexBlock struct {
	bhs []blockHeader

	// The buffer for holding the data referred by bhs
	buf []byte
}

func (idxb *indexBlock) SizeBytes() int {
	bhs := idxb.bhs[:cap(idxb.bhs)]
	n := int(unsafe.Sizeof(*idxb))
	for i := range bhs {
		n += bhs[i].SizeBytes()
	}
	return n
}
