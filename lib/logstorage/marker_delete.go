package logstorage

import (
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// deleteMarker keeps per-block Delete markers.
type deleteMarker struct {
	blockIDs []uint64  // sorted block sequence numbers that have marker data
	rows     []boolRLE // same length and order as blockIDs
}

func (dm *deleteMarker) String() string {
	s := strings.Builder{}
	for i, blockID := range dm.blockIDs {
		s.WriteString(fmt.Sprintf("| [blockID: %d, row: %v] ", blockID, dm.rows[i].String()))
	}
	return s.String()
}

// GetMarkedRows returns marked rows for the given block sequence number.
func (dm *deleteMarker) GetMarkedRows(blockSeq uint64) (boolRLE, bool) {
	idx, found := slices.BinarySearch(dm.blockIDs, blockSeq)
	if !found {
		return nil, false
	}
	return dm.rows[idx], true
}

// Marshal serializes delete marker data to the provided buffer.
// Format: [num_blocks:varuint64][block_id:uint64][rle_len:varuint64][rle_data:bytes]...
func (dm *deleteMarker) Marshal(dst []byte) []byte {
	// Number of blocks with markers
	dst = encoding.MarshalVarUint64(dst, uint64(len(dm.blockIDs)))

	// For each block, write: block_id + rle_length + rle_data
	for i, blockID := range dm.blockIDs {
		dst = encoding.MarshalUint64(dst, blockID)

		rleData := dm.rows[i]
		dst = encoding.MarshalVarUint64(dst, uint64(len(rleData)))
		dst = append(dst, rleData...)
	}

	return dst
}

// Unmarshal parses delete‑marker data from the provided bytes.
// It returns an error if the data is malformed.
func (dm *deleteMarker) Unmarshal(data []byte) error {
	*dm = deleteMarker{} // reset receiver

	if len(data) == 0 {
		return nil
	}

	pos := 0
	end := len(data)

	numBlocks, n := encoding.UnmarshalVarUint64(data)
	pos += n

	// sanity guard against corrupt data
	if numBlocks > 1<<31 {
		return fmt.Errorf("too many blocks: %d", numBlocks)
	}

	dm.blockIDs = make([]uint64, 0, numBlocks)
	dm.rows = make([]boolRLE, 0, numBlocks)
	for i := range numBlocks {
		// block‑ID (8 bytes)
		if pos+8 > end {
			return fmt.Errorf("truncated data: cannot read block_id %d", i)
		}
		blockID := encoding.UnmarshalUint64(data[pos:])
		pos += 8

		// RLE length (varuint)
		rleLen, n := encoding.UnmarshalVarUint64(data[pos:])
		pos += n

		// RLE bytes
		if pos+int(rleLen) > end {
			return fmt.Errorf("truncated data: cannot read rle_data for block %d", i)
		}
		rleSlice := data[pos : pos+int(rleLen)] // zero‑copy view
		pos += int(rleLen)

		dm.blockIDs = append(dm.blockIDs, blockID)
		dm.rows = append(dm.rows, boolRLE(rleSlice))
	}

	if pos != end {
		return fmt.Errorf("unexpected %d trailing bytes in deleteMarker", end-pos)
	}
	return nil
}

// merge combines this deleteMarker with another deleteMarker.
// It merges block deletions using RLE union operations where both markers have the same blockID.
func (dm *deleteMarker) merge(other *deleteMarker) {
	// Nothing to merge?
	if other == nil || len(other.blockIDs) == 0 {
		return
	}
	if len(dm.blockIDs) == 0 {
		// dm is empty – just copy other's data.
		dm.blockIDs = append([]uint64(nil), other.blockIDs...)
		dm.rows = append([]boolRLE(nil), other.rows...)
		return
	}

	// Two‑pointer merge because both blockID slices are already sorted.
	mergedIDs := make([]uint64, 0, len(dm.blockIDs)+len(other.blockIDs))
	mergedRows := make([]boolRLE, 0, len(dm.rows)+len(other.rows))

	i, j := 0, 0
	for i < len(dm.blockIDs) && j < len(other.blockIDs) {
		idA, idB := dm.blockIDs[i], other.blockIDs[j]

		switch {
		case idA == idB:
			// Same block exists in both markers – union their RLEs.
			mergedIDs = append(mergedIDs, idA)
			mergedRows = append(mergedRows, dm.rows[i].Union(other.rows[j]))
			i++
			j++

		case idA < idB:
			mergedIDs = append(mergedIDs, idA)
			mergedRows = append(mergedRows, dm.rows[i])
			i++

		default: // idB < idA
			mergedIDs = append(mergedIDs, idB)
			mergedRows = append(mergedRows, other.rows[j])
			j++
		}
	}

	// Append leftovers from either slice.
	for ; i < len(dm.blockIDs); i++ {
		mergedIDs = append(mergedIDs, dm.blockIDs[i])
		mergedRows = append(mergedRows, dm.rows[i])
	}
	for ; j < len(other.blockIDs); j++ {
		mergedIDs = append(mergedIDs, other.blockIDs[j])
		mergedRows = append(mergedRows, other.rows[j])
	}

	// Install merged slices.
	dm.blockIDs = mergedIDs
	dm.rows = mergedRows
}

// AddBlock adds a block with its RLE data to the deleteMarker.
// If blockID already exists, it merges the RLE data using union operation.
func (dm *deleteMarker) AddBlock(blockID uint64, rle boolRLE) {
	// Find existing block or insertion point
	idx, found := slices.BinarySearch(dm.blockIDs, blockID)

	if found {
		// Block already exists - merge RLE data using union operation
		existingRLE := dm.rows[idx]
		combined := existingRLE.Union(rle)
		dm.rows[idx] = combined
	} else {
		// Block doesn't exist - insert at correct position
		dm.blockIDs = slices.Insert(dm.blockIDs, idx, blockID)
		dm.rows = slices.Insert(dm.rows, idx, rle)
	}
}

// mustReadDeleteMarkerData reads delete marker data from the provided reader.
func mustReadDeleteMarkerData(datReader filestream.ReadCloser) deleteMarker {
	var res deleteMarker
	if datReader == nil {
		return res
	}

	datBytes, err := io.ReadAll(datReader)
	if err != nil {
		logger.Panicf("FATAL: %s: cannot read delete marker data: %s", datReader.Path(), err)
	}

	if len(datBytes) == 0 {
		return res
	}

	if err := res.Unmarshal(datBytes); err != nil {
		logger.Panicf("FATAL: %s: cannot unmarshal delete marker data: %s", datReader.Path(), err)
	}

	return res
}

// flushDeleteMarker writes delMarker to disk and updates the in-memory index
// for the given part. Writers are serialized by ddb.partsLock; readers access
// the index lock-free via atomic.Load.
func flushDeleteMarker(pw *partWrapper, dm *deleteMarker, seq uint64) {
	if dm == nil || len(dm.blockIDs) == 0 {
		return // nothing to flush
	}

	current := pw.p.deleteMarker.Load()
	var merged *deleteMarker
	if current == nil {
		// First marker – just deep-copy delMarker to avoid sharing mutable slices.
		merged = &deleteMarker{
			blockIDs: append([]uint64(nil), dm.blockIDs...),
			rows:     append([]boolRLE(nil), dm.rows...),
		}
	} else {
		// Copy-on-write: start from current snapshot and merge additions.
		merged = &deleteMarker{
			blockIDs: append([]uint64(nil), current.blockIDs...),
			rows:     append([]boolRLE(nil), current.rows...),
		}
		merged.merge(dm)
	}

	// Publish the new snapshot for readers.
	pw.p.deleteMarker.Store(merged)

	// Persist to disk.
	if pw.p.path != "" {
		datBuf := merged.Marshal(nil)
		partPath := pw.p.path
		datPath := filepath.Join(partPath, deleteMarkerFilename)
		fs.MustWriteAtomic(datPath, datBuf, true /*overwrite*/)
		fs.MustSyncPath(partPath)
	}

	pw.taskSeq.Store(seq)
}
