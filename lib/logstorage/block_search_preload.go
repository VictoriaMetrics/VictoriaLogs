package logstorage

import (
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

func collectBloomFilterColumns(f filter) map[string]struct{} {
	cols := make(map[string]struct{})
	allNeeded := false

	var walk func(f filter)
	walk = func(f filter) {
		if allNeeded {
			return
		}
		switch t := f.(type) {
		case *filterAnd:
			for _, sub := range t.filters {
				walk(sub)
			}
		case *filterOr:
			for _, sub := range t.filters {
				walk(sub)
			}
		case *filterNot:
			walk(t.f)
		case *filterGeneric:
			if t.isWildcard {
				allNeeded = true
				return
			}
			if t.hasBloomFilterTokens() {
				cols[t.fieldName] = struct{}{}
			}
		}
	}
	walk(f)

	if allNeeded {
		return nil
	}
	return cols
}

func preloadBatchBloomFilters(bsws []blockSearchWork, neededCols map[string]struct{}) {
	if len(bsws) == 0 {
		return
	}

	if neededCols != nil && len(neededCols) == 0 {
		return
	}

	hasRemote := false
	for i := range bsws {
		bsw := &bsws[i]
		if bsw.p != nil && bsw.p.pt.isRemote {
			hasRemote = true
			break
		}
	}
	if !hasRemote {
		return
	}

	type headerResult struct {
		cshIndexBlock []byte
		cshBlock      []byte
	}
	headers := make([]headerResult, len(bsws))

	var wg sync.WaitGroup
	for i := range bsws {
		bsw := &bsws[i]
		if bsw.p == nil || !bsw.p.pt.isRemote {
			continue
		}
		wg.Add(1)
		go func(idx int, bsw *blockSearchWork) {
			defer wg.Done()

			p := bsw.p
			bh := &bsw.bh

			var qs QueryStats
			headers[idx].cshIndexBlock = readColumnsHeaderIndexBlock(nil, p, bh, &qs)
			headers[idx].cshBlock = readColumnsHeaderBlock(nil, p, bh, &qs)
		}(i, bsw)
	}
	wg.Wait()

	for i := range bsws {
		bsw := &bsws[i]
		hd := &headers[i]
		if len(hd.cshIndexBlock) == 0 || len(hd.cshBlock) == 0 {
			continue
		}

		p := bsw.p

		var cshIndex columnsHeaderIndex
		if err := cshIndex.unmarshalInplace(hd.cshIndexBlock); err != nil {
			logger.Errorf("%s: cannot unmarshal columnsHeaderIndex during bloom filter preload: %s", p.path, err)
			continue
		}

		for _, cr := range cshIndex.columnHeadersRefs {
			if cr.columnNameID >= uint64(len(p.columnNames)) {
				continue
			}
			if cr.offset >= uint64(len(hd.cshBlock)) {
				continue
			}

			var ch columnHeader
			if _, err := ch.unmarshalInplace(hd.cshBlock[cr.offset:], p.ph.FormatVersion); err != nil {
				continue
			}
			if ch.bloomFilterSize == 0 {
				continue
			}

			colName := p.columnNames[cr.columnNameID]

			if neededCols != nil {
				if _, ok := neededCols[colName]; !ok {
					continue
				}
			}

			bloomFile := p.getBloomValuesFileForColumnName(colName).bloom
			offset := int64(ch.bloomFilterOffset)
			size := ch.bloomFilterSize

			wg.Add(1)
			go func() {
				defer wg.Done()
				bb := bytesutil.ByteBuffer{}
				bb.B = make([]byte, size)
				bloomFile.MustReadAt(bb.B, offset)
			}()
		}
	}
	wg.Wait()
}
