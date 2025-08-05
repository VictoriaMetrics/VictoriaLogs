package logstorage

import (
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
)

var (
	searchStatsOnce sync.Once

	bytesReadPerQueryColumnsHeaders       *metrics.Histogram
	bytesReadPerQueryColumnsHeaderIndexes *metrics.Histogram
	bytesReadPerQueryBloomFilters         *metrics.Histogram
	bytesReadPerQueryValues               *metrics.Histogram
	bytesReadPerQueryTimestamps           *metrics.Histogram
	bytesReadPerQueryBlockHeaders         *metrics.Histogram

	bytesReadPerQueryTotal *metrics.Histogram
)

func initSearchStats() {
	bytesReadPerQueryColumnsHeaders = metrics.NewHistogram(`vl_storage_per_query_columns_headers_read_bytes`)
	bytesReadPerQueryColumnsHeaderIndexes = metrics.NewHistogram(`vl_storage_per_query_columns_header_indexes_read_bytes`)
	bytesReadPerQueryBloomFilters = metrics.NewHistogram(`vl_storage_per_query_bloom_filters_read_bytes`)
	bytesReadPerQueryValues = metrics.NewHistogram(`vl_storage_per_query_values_read_bytes`)
	bytesReadPerQueryTimestamps = metrics.NewHistogram(`vl_storage_per_query_timestamps_read_bytes`)
	bytesReadPerQueryBlockHeaders = metrics.NewHistogram(`vl_storage_per_query_block_headers_read_bytes`)

	bytesReadPerQueryTotal = metrics.NewHistogram(`vl_storage_per_query_total_read_bytes`)
}

func updateSearchMetrics(ss *searchStats) {
	searchStatsOnce.Do(initSearchStats)

	bytesReadPerQueryColumnsHeaders.Update(float64(ss.bytesReadColumnsHeaders))
	bytesReadPerQueryColumnsHeaderIndexes.Update(float64(ss.bytesReadColumnsHeaderIndexes))
	bytesReadPerQueryBloomFilters.Update(float64(ss.bytesReadBloomFilters))
	bytesReadPerQueryValues.Update(float64(ss.bytesReadValues))
	bytesReadPerQueryTimestamps.Update(float64(ss.bytesReadTimestamps))
	bytesReadPerQueryBlockHeaders.Update(float64(ss.bytesReadBlockHeaders))

	bytesReadTotal := ss.bytesReadColumnsHeaders + ss.bytesReadColumnsHeaderIndexes + ss.bytesReadBloomFilters +
		ss.bytesReadValues + ss.bytesReadTimestamps + ss.bytesReadBlockHeaders
	bytesReadPerQueryTotal.Update(float64(bytesReadTotal))
}

// searchStats contains various stats related to the search.
type searchStats struct {
	// bytesReadColumnsHeaders is the total number of columns header bytes read from disk during the search.
	bytesReadColumnsHeaders uint64

	// bytesReadColumnsHeaderIndexes is the total number of columns header index bytes read from disk during the search.
	bytesReadColumnsHeaderIndexes uint64

	// bytesReadBloomFilters is the total number of bloom filter bytes read from disk during the search.
	bytesReadBloomFilters uint64

	// bytesReadValues is the total number of values bytes read from disk during the search.
	bytesReadValues uint64

	// bytesReadTimestamps is the total number of timestamps bytes read from disk during the search.
	bytesReadTimestamps uint64

	// bytesReadBlockHeaders is the total number of headers bytes read from disk during the search.
	bytesReadBlockHeaders uint64
}

func (ss *searchStats) updateAtomic(src *searchStats) {
	atomic.AddUint64(&ss.bytesReadColumnsHeaders, src.bytesReadColumnsHeaders)
	atomic.AddUint64(&ss.bytesReadColumnsHeaderIndexes, src.bytesReadColumnsHeaderIndexes)
	atomic.AddUint64(&ss.bytesReadBloomFilters, src.bytesReadBloomFilters)
	atomic.AddUint64(&ss.bytesReadValues, src.bytesReadValues)
	atomic.AddUint64(&ss.bytesReadTimestamps, src.bytesReadTimestamps)
	atomic.AddUint64(&ss.bytesReadBlockHeaders, src.bytesReadBlockHeaders)
}
