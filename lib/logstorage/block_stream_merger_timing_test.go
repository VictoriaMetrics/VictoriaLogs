package logstorage

import (
	"fmt"
	"testing"
)

func BenchmarkMergeInmemoryParts(b *testing.B) {
	for _, streams := range []string{"shared", "disjoint"} {
		for _, partsCount := range []int{2, 8, 15} {
			for _, rowsPerPart := range []int{1_000, 100_000} {
				name := fmt.Sprintf("streams-%s/parts-%d/rowsPerPart-%d", streams, partsCount, rowsPerPart)
				b.Run(name, func(b *testing.B) {
					benchmarkMergeInmemoryParts(b, partsCount, rowsPerPart, streams == "shared")
				})
			}
		}
	}
}

// benchmarkMergeInmemoryParts measures merging partsCount in-memory parts into a single in-memory part.
//
// If sharedStreams is set, then all the source parts contain the same log streams, so the merge must interleave blocks
// from all the parts. Otherwise every part contains its own set of log streams.
func benchmarkMergeInmemoryParts(b *testing.B, partsCount, rowsPerPart int, sharedStreams bool) {
	const streamsPerPart = 10

	mps := make([]*inmemoryPart, partsCount)
	for i := range mps {
		seed := int64(i)
		if sharedStreams {
			seed = 0
		}
		var lr logRows
		lr.mustAddRows(newTestLogRows(streamsPerPart, rowsPerPart/streamsPerPart, seed))
		mp := getInmemoryPart()
		mp.mustInitFromRows(&lr)
		mps[i] = mp
	}
	defer func() {
		for _, mp := range mps {
			putInmemoryPart(mp)
		}
	}()

	bsrs := make([]*blockStreamReader, partsCount)

	b.ReportAllocs()
	b.SetBytes(int64(partsCount * rowsPerPart))
	for b.Loop() {
		for i, mp := range mps {
			bsr := getBlockStreamReader()
			bsr.MustInitFromInmemoryPart(mp)
			bsrs[i] = bsr
		}

		mpDst := getInmemoryPart()
		bsw := getBlockStreamWriter()
		bsw.MustInitForInmemoryPart(mpDst)
		mustMergeBlockStreams(&mpDst.ph, nil, bsw, bsrs, nil, nil)
		putBlockStreamWriter(bsw)

		if n := mpDst.ph.RowsCount; n != uint64(partsCount*rowsPerPart) {
			panic(fmt.Errorf("BUG: unexpected number of rows in the merged part; got %d; want %d", n, partsCount*rowsPerPart))
		}
		putInmemoryPart(mpDst)

		for i, bsr := range bsrs {
			putBlockStreamReader(bsr)
			bsrs[i] = nil
		}
	}
}
