package logsql

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fastjson"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	maxQueryTimeRange = flagutil.NewExtendedDuration("search.maxQueryTimeRange", "0", "The maximum time range, which can be set in the query sent to querying APIs. "+
		"Queries with bigger time ranges are rejected. See https://docs.victoriametrics.com/victorialogs/querying/#resource-usage-limits")

	allowPartialResponseFlag = flag.Bool("search.allowPartialResponse", false, "Whether to allow returning partial responses when some of vlstorage nodes "+
		"from the -storageNode list are unavailable for querying. This flag works only for cluster setup of VictoriaLogs. "+
		"See https://docs.victoriametrics.com/victorialogs/querying/#partial-responses")
)

// ProcessQueryTimeRangeRequest handles /select/logsql/query_time_range request.
//
// This request returns JSON object with "start" and "end" fields containing
// the really selected time range by the provided query in RFC3339Nano format.
// This is needed for https://github.com/VictoriaMetrics/VictoriaLogs/issues/558#issuecomment-3527811816
//
// The format of the returned JSON:
//
//	{
//	  "start":"YYYY-MM-DDThh:mm:sss.nnnnnnnnnZ",
//	  "end":"YYYY-MM-DDThh:mm:sss.nnnnnnnnnZ",
//	  "hasTimeFilter":true|false
//	}
func ProcessQueryTimeRangeRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	minTimestamp, maxTimestamp, hasTimeFilter, err := parseQueryTimeRangeArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	startStr := timestampToRFC3339Nano(minTimestamp)
	endStr := timestampToRFC3339Nano(maxTimestamp)
	fmt.Fprintf(w, `{"start":%q,"end":%q,"hasTimeFilter":%t}`, startStr, endStr, hasTimeFilter)
}

func parseQueryTimeRangeArgs(r *http.Request) (int64, int64, bool, error) {
	qStr := r.FormValue("query")
	if qStr == "" {
		return 0, 0, false, fmt.Errorf("`query` arg cannot be empty")
	}
	currTimestamp := time.Now().UnixNano()
	q, err := logstorage.ParseQueryAtTimestamp(qStr, currTimestamp)
	if err != nil {
		return 0, 0, false, fmt.Errorf("cannot parse query [%s]: %s", qStr, err)
	}

	minTimestamp, maxTimestamp := q.GetFilterTimeRange()

	// hasTimeFilter is true if the query itself contains a _time filter
	hasTimeFilter := (minTimestamp != math.MinInt64 || maxTimestamp != math.MaxInt64)

	if minTimestamp == math.MinInt64 {
		start, ok, err := getTimeNsec(r, "start")
		if err != nil {
			return 0, 0, false, err
		}
		if ok {
			minTimestamp = start
		}
	}
	if maxTimestamp == math.MaxInt64 {
		end, ok, err := getTimeNsec(r, "end")
		if err != nil {
			return 0, 0, false, err
		}
		if ok {
			maxTimestamp = end
		}
	}

	return minTimestamp, maxTimestamp, hasTimeFilter, nil
}

func timestampToRFC3339Nano(nsec int64) string {
	return time.Unix(0, nsec).UTC().Format(time.RFC3339Nano)
}

// ProcessFacetsRequest handles /select/logsql/facets request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-facets
func ProcessFacetsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	maxValuesPerField, err := getPositiveInt(r, "max_values_per_field")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	maxValueLen, err := getPositiveInt(r, "max_value_len")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	keepConstFields := httputil.GetBool(r, "keep_const_fields")

	// Pipes must be dropped, since it is expected facets are obtained
	// from the real logs stored in the database.
	ca.q.DropAllPipes()

	ca.q.AddFacetsPipe(limit, maxValuesPerField, maxValueLen, keepConstFields)

	var mLock sync.Mutex
	m := make(map[string][]facetEntry)
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()
		if rowsCount == 0 {
			return
		}

		columns := db.Columns
		if len(columns) != 3 {
			logger.Panicf("BUG: expecting 3 columns; got %d columns", len(columns))
		}

		// Fetch columns by name to avoid relying on column ordering at VictoriaLogs cluster.
		// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/648
		cFieldName := db.GetColumnByName("field_name")
		cFieldValue := db.GetColumnByName("field_value")
		cHits := db.GetColumnByName("hits")
		if cFieldName == nil || cFieldValue == nil || cHits == nil {
			logger.Panicf("BUG: missing expected columns for facets response: field_name=%v, field_value=%v, hits=%v",
				cFieldName != nil, cFieldValue != nil, cHits != nil)
		}

		fieldNames := cFieldName.Values
		fieldValues := cFieldValue.Values
		hits := cHits.Values

		bb := blockResultPool.Get()
		for i := range fieldNames {
			fieldName := strings.Clone(fieldNames[i])
			fieldValue := strings.Clone(fieldValues[i])
			hitsStr := strings.Clone(hits[i])

			mLock.Lock()
			m[fieldName] = append(m[fieldName], facetEntry{
				value: fieldValue,
				hits:  hitsStr,
			})
			mLock.Unlock()
		}
		blockResultPool.Put(bb)
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Execute the query
	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		httpserver.Errorf(w, r, "cannot execute query [%s]: %s", ca.q, err)
		return
	}

	// Write response header
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write response
	WriteFacetsResponse(w, m)
}

type facetEntry struct {
	value string
	hits  string
}

// ProcessHitsRequest handles /select/logsql/hits request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-hits-stats
func ProcessHitsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Obtain step
	stepStr := r.FormValue("step")
	if stepStr == "" {
		stepStr = "1d"
	}
	step, err := timeutil.ParseDuration(stepStr)
	if err != nil {
		httpserver.Errorf(w, r, "cannot parse 'step' arg: %s", err)
		return
	}
	if step <= 0 {
		httpserver.Errorf(w, r, "'step' must be bigger than zero")
		return
	}

	// Obtain offset
	offsetStr := r.FormValue("offset")
	if offsetStr == "" {
		offsetStr = "0s"
	}
	offset, err := timeutil.ParseDuration(offsetStr)
	if err != nil {
		httpserver.Errorf(w, r, "cannot parse 'offset' arg: %s", err)
		return
	}

	// Obtain field entries
	fields := r.Form["field"]

	// Obtain limit on the number of top fields entries.
	fieldsLimit, err := getPositiveInt(r, "fields_limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Add a pipe, which calculates hits over time with the given step and offset for the given fields.
	ca.q.AddCountByTimePipe(int64(step), int64(offset), fields)
	start, end := ca.q.GetFilterTimeRange()

	var mLock sync.Mutex
	m := make(map[string]*hitsSeries)
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()
		if rowsCount == 0 {
			return
		}

		columns := db.Columns
		timestampValues := columns[0].Values
		hitsValues := columns[len(columns)-1].Values
		columns = columns[1 : len(columns)-1]

		bb := blockResultPool.Get()
		for i := 0; i < rowsCount; i++ {
			timestampNsec, ok := logstorage.TryParseTimestampRFC3339Nano(timestampValues[i])
			if !ok {
				logger.Panicf("BUG: cannot parse timestamp=%q", timestampValues[i])
			}
			hitsStr := strings.Clone(hitsValues[i])
			hits, err := strconv.ParseUint(hitsStr, 10, 64)
			if err != nil {
				logger.Panicf("BUG: cannot parse hitsStr=%q: %s", hitsStr, err)
			}

			bb.Reset()
			WriteFieldsForHits(bb, columns, i)

			mLock.Lock()
			hs, ok := m[string(bb.B)]
			if !ok {
				hs = &hitsSeries{}
				m[string(bb.B)] = hs
			}
			hs.timestamps = append(hs.timestamps, timestampNsec)
			hs.hits = append(hs.hits, hits)
			hs.hitsTotal += hits
			mLock.Unlock()
		}
		blockResultPool.Put(bb)
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Execute the query
	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		httpserver.Errorf(w, r, "cannot execute query [%s]: %s", ca.q, err)
		return
	}

	m = getTopHitsSeries(m, fieldsLimit)
	addMissingZeroHits(m, start, end, int64(step), int64(offset))

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write response
	WriteHitsSeries(w, m)
}

func addMissingZeroHits(m map[string]*hitsSeries, start, end, step, offset int64) {
	if start == math.MinInt64 {
		start = math.MaxInt64
		for _, hs := range m {
			start = min(start, slices.Min(hs.timestamps))
		}
	} else {
		start -= start%step - offset
	}

	if end == math.MaxInt64 {
		end = math.MinInt64
		for _, hs := range m {
			end = max(end, slices.Max(hs.timestamps))
		}
	} else {
		end -= start%step - offset
	}

	if start > end {
		// nothing to do
		return
	}

	for _, hs := range m {
		ts := start
		for ts <= end {
			if !slices.Contains(hs.timestamps, ts) {
				hs.timestamps = append(hs.timestamps, ts)
				hs.hits = append(hs.hits, 0)
			}

			if ts+step < ts {
				// stop on int64 overflow
				break
			}
			ts += step
		}
	}
}

var blockResultPool bytesutil.ByteBufferPool

func getTopHitsSeries(m map[string]*hitsSeries, fieldsLimit int) map[string]*hitsSeries {
	if fieldsLimit <= 0 || fieldsLimit >= len(m) {
		return m
	}

	type fieldsHits struct {
		fieldsStr string
		hs        *hitsSeries
	}
	a := make([]fieldsHits, 0, len(m))
	for fieldsStr, hs := range m {
		a = append(a, fieldsHits{
			fieldsStr: fieldsStr,
			hs:        hs,
		})
	}
	sort.Slice(a, func(i, j int) bool {
		return a[i].hs.hitsTotal > a[j].hs.hitsTotal
	})

	hitsOther := make(map[int64]uint64)
	for _, x := range a[fieldsLimit:] {
		for i, timestamp := range x.hs.timestamps {
			hitsOther[timestamp] += x.hs.hits[i]
		}
	}
	var hsOther hitsSeries
	for timestamp, hits := range hitsOther {
		hsOther.timestamps = append(hsOther.timestamps, timestamp)
		hsOther.hits = append(hsOther.hits, hits)
		hsOther.hitsTotal += hits
	}

	mNew := make(map[string]*hitsSeries, fieldsLimit+1)
	for _, x := range a[:fieldsLimit] {
		mNew[x.fieldsStr] = x.hs
	}
	mNew["{}"] = &hsOther

	return mNew
}

type hitsSeries struct {
	hitsTotal  uint64
	timestamps []int64
	hits       []uint64
}

func (hs *hitsSeries) sort() {
	sort.Sort(hs)
}

func (hs *hitsSeries) Len() int {
	return len(hs.timestamps)
}

func (hs *hitsSeries) Swap(i, j int) {
	hs.timestamps[i], hs.timestamps[j] = hs.timestamps[j], hs.timestamps[i]
	hs.hits[i], hs.hits[j] = hs.hits[j], hs.hits[i]
}

func (hs *hitsSeries) Less(i, j int) bool {
	return hs.timestamps[i] < hs.timestamps[j]
}

// ProcessFieldNamesRequest handles /select/logsql/field_names request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-field-names
func ProcessFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Obtain field names for the given query
	startTime := time.Now()
	fieldNames, err := vlstorage.GetFieldNames(qctx)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain field names: %s", err)
		return
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write results
	WriteValuesWithHitsJSON(w, fieldNames)
}

// ProcessFieldValuesRequest handles /select/logsql/field_values request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-field-values
func ProcessFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Parse fieldName query arg
	fieldName := r.FormValue("field")
	if fieldName == "" {
		httpserver.Errorf(w, r, "missing 'field' query arg")
		return
	}

	// Parse limit query arg
	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Obtain unique values for the given field
	startTime := time.Now()
	values, err := vlstorage.GetFieldValues(qctx, fieldName, uint64(limit))
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain values for field %q: %s", fieldName, err)
		return
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write results
	WriteValuesWithHitsJSON(w, values)
}

// ProcessStreamFieldNamesRequest processes /select/logsql/stream_field_names request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-stream-field-names
func ProcessStreamFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Obtain stream field names for the given query
	startTime := time.Now()
	names, err := vlstorage.GetStreamFieldNames(qctx)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain stream field names: %s", err)
		return
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write results
	WriteValuesWithHitsJSON(w, names)
}

// ProcessStreamFieldValuesRequest processes /select/logsql/stream_field_values request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-stream-field-values
func ProcessStreamFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Parse fieldName query arg
	fieldName := r.FormValue("field")
	if fieldName == "" {
		httpserver.Errorf(w, r, "missing 'field' query arg")
		return
	}

	// Parse limit query arg
	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Obtain stream field values for the given query and the given fieldName
	startTime := time.Now()
	values, err := vlstorage.GetStreamFieldValues(qctx, fieldName, uint64(limit))
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain stream field values: %s", err)
		return
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write results
	WriteValuesWithHitsJSON(w, values)
}

// ProcessStreamIDsRequest processes /select/logsql/stream_ids request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-stream_ids
func ProcessStreamIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Parse limit query arg
	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Obtain streamIDs for the given query
	startTime := time.Now()
	streamIDs, err := vlstorage.GetStreamIDs(qctx, uint64(limit))
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain stream_ids: %s", err)
		return
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write results
	WriteValuesWithHitsJSON(w, streamIDs)
}

// ProcessStreamsRequest processes /select/logsql/streams request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-streams
func ProcessStreamsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Parse limit query arg
	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Obtain streams for the given query
	startTime := time.Now()
	streams, err := vlstorage.GetStreams(qctx, uint64(limit))
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain streams: %s", err)
		return
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write results
	WriteValuesWithHitsJSON(w, streams)
}

// ProcessLiveTailRequest processes live tailing request to /select/logsq/tail
//
// See https://docs.victoriametrics.com/victorialogs/querying/#live-tailing
func ProcessLiveTailRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	liveTailRequests.Inc()
	defer liveTailRequests.Dec()

	ca, err := parseCommonArgsWithConfig(r, true)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	if !ca.q.CanLiveTail() {
		httpserver.Errorf(w, r, "the query [%s] cannot be used in live tailing; "+
			"see https://docs.victoriametrics.com/victorialogs/querying/#live-tailing for details", ca.q)
		return
	}

	refreshIntervalMsecs, err := httputil.GetDuration(r, "refresh_interval", 1000)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	refreshInterval := time.Millisecond * time.Duration(refreshIntervalMsecs)

	startOffsetMsecs, err := httputil.GetDuration(r, "start_offset", 5*1000)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	startOffset := startOffsetMsecs * 1e6

	offsetMsecs, err := httputil.GetDuration(r, "offset", 5000)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	offset := offsetMsecs * 1e6

	ctxWithCancel, cancel := context.WithCancel(ctx)
	tp := newTailProcessor(cancel)

	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	end := time.Now().UnixNano() - offset
	start := end - startOffset
	doneCh := ctxWithCancel.Done()
	flusher, ok := w.(http.Flusher)
	if !ok {
		logger.Panicf("BUG: it is expected that http.ResponseWriter (%T) supports http.Flusher interface", w)
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	flusher.Flush()

	qctx := ca.newQueryContext(ctxWithCancel)
	defer ca.updatePerQueryStatsMetrics()

	q := ca.q
	qOrig := q
	for {
		q = qOrig.CloneWithTimeFilter(end, start, end)
		qctxLocal := qctx.WithQuery(q)
		if err := vlstorage.RunQuery(qctxLocal, tp.writeBlock); err != nil {
			httpserver.Errorf(w, r, "cannot execute tail query [%s]: %s", q, err)
			return
		}
		resultRows, err := tp.getTailRows()
		if err != nil {
			httpserver.Errorf(w, r, "cannot get tail results for query [%q]: %s", q, err)
			return
		}
		if len(resultRows) > 0 {
			WriteJSONRows(w, resultRows)
			flusher.Flush()
		}

		select {
		case <-doneCh:
			return
		case <-ticker.C:
			start = end - tailOffsetNsecs
			end = time.Now().UnixNano() - offset
		}
	}
}

var liveTailRequests = metrics.NewCounter(`vl_live_tailing_requests`)

const tailOffsetNsecs = 5e9

type logRow struct {
	timestamp int64
	fields    []logstorage.Field
}

func sortLogRows(rows []logRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].timestamp < rows[j].timestamp
	})
}

type tailProcessor struct {
	cancel func()

	mu sync.Mutex

	perStreamRows  map[string][]logRow
	lastTimestamps map[string]int64

	err error
}

func newTailProcessor(cancel func()) *tailProcessor {
	return &tailProcessor{
		cancel: cancel,

		perStreamRows:  make(map[string][]logRow),
		lastTimestamps: make(map[string]int64),
	}
}

func (tp *tailProcessor) writeBlock(_ uint, db *logstorage.DataBlock) {
	if db.RowsCount() == 0 {
		return
	}

	tp.mu.Lock()
	defer tp.mu.Unlock()

	if tp.err != nil {
		return
	}

	// Make sure columns contain _time field, since it is needed for proper tail work.
	timestamps, ok := db.GetTimestamps(nil)
	if !ok {
		tp.err = fmt.Errorf("missing _time field")
		tp.cancel()
		return
	}

	// Copy block rows to tp.perStreamRows
	for i, timestamp := range timestamps {
		streamID := ""
		fields := make([]logstorage.Field, len(db.Columns))
		for j, c := range db.Columns {
			name := strings.Clone(c.Name)
			value := strings.Clone(c.Values[i])

			fields[j] = logstorage.Field{
				Name:  name,
				Value: value,
			}

			if name == "_stream_id" {
				streamID = value
			}
		}

		tp.perStreamRows[streamID] = append(tp.perStreamRows[streamID], logRow{
			timestamp: timestamp,
			fields:    fields,
		})
	}
}

func (tp *tailProcessor) getTailRows() ([][]logstorage.Field, error) {
	if tp.err != nil {
		return nil, tp.err
	}

	var resultRows []logRow
	for streamID, rows := range tp.perStreamRows {
		sortLogRows(rows)

		lastTimestamp, ok := tp.lastTimestamps[streamID]
		if ok {
			// Skip already written rows
			for len(rows) > 0 && rows[0].timestamp <= lastTimestamp {
				rows = rows[1:]
			}
		}
		if len(rows) > 0 {
			resultRows = append(resultRows, rows...)
			tp.lastTimestamps[streamID] = rows[len(rows)-1].timestamp
		}
	}
	clear(tp.perStreamRows)

	sortLogRows(resultRows)

	tailRows := make([][]logstorage.Field, len(resultRows))
	for i, row := range resultRows {
		tailRows[i] = row.fields
	}

	return tailRows, nil
}

// ProcessStatsQueryRangeRequest handles /select/logsql/stats_query_range request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-log-range-stats
func ProcessStatsQueryRangeRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Obtain step
	stepStr := r.FormValue("step")
	if stepStr == "" {
		stepStr = "1d"
	}
	step, err := timeutil.ParseDuration(stepStr)
	if err != nil {
		err = fmt.Errorf("cannot parse 'step' arg: %s", err)
		httpserver.SendPrometheusError(w, r, err)
		return
	}
	if step <= 0 {
		err := fmt.Errorf("'step' must be bigger than zero")
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Obtain `by(...)` fields from the last `| stats` pipe in q.
	// Add `_time:step` to the `by(...)` list.
	byFields, err := ca.q.GetStatsByFieldsAddGroupingByTime(int64(step))
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	m := make(map[string]*statsSeries)
	var mLock sync.Mutex

	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()

		columns := db.Columns
		clonedColumnNames := make([]string, len(columns))
		for i, c := range columns {
			clonedColumnNames[i] = strings.Clone(c.Name)
		}
		for i := 0; i < rowsCount; i++ {
			// Do not move q.GetTimestamp() outside writeBlock, since ts
			// must be initialized to query timestamp for every processed log row.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8312
			ts := ca.q.GetTimestamp()
			labels := make([]logstorage.Field, 0, len(byFields))
			for j, c := range columns {
				if c.Name == "_time" {
					nsec, ok := logstorage.TryParseTimestampRFC3339Nano(c.Values[i])
					if ok {
						ts = nsec
						continue
					}
				}
				if slices.Contains(byFields, c.Name) {
					labels = append(labels, logstorage.Field{
						Name:  clonedColumnNames[j],
						Value: strings.Clone(c.Values[i]),
					})
				}
			}

			var dst []byte
			for j, c := range columns {
				if !slices.Contains(byFields, c.Name) {
					name := clonedColumnNames[j]
					dst = dst[:0]
					dst = append(dst, name...)
					dst = logstorage.MarshalFieldsToJSON(dst, labels)
					key := string(dst)
					p := statsPoint{
						Timestamp: ts,
						Value:     strings.Clone(c.Values[i]),
					}

					mLock.Lock()
					ss := m[key]
					if ss == nil {
						ss = &statsSeries{
							key:    key,
							Name:   name,
							Labels: labels,
						}
						m[key] = ss
					}
					ss.Points = append(ss.Points, p)
					mLock.Unlock()
				}
			}
		}
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Execute the request.
	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		err = fmt.Errorf("cannot execute query [%s]: %s", ca.q, err)
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Sort the collected stats by _time
	rows := make([]*statsSeries, 0, len(m))
	for _, ss := range m {
		points := ss.Points
		sort.Slice(points, func(i, j int) bool {
			return points[i].Timestamp < points[j].Timestamp
		})
		rows = append(rows, ss)
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].key < rows[j].key
	})

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write response
	WriteStatsQueryRangeResponse(w, rows)
}

type statsSeries struct {
	key string

	Name   string
	Labels []logstorage.Field
	Points []statsPoint
}

type statsPoint struct {
	Timestamp int64
	Value     string
}

// ProcessStatsQueryRequest handles /select/logsql/stats_query request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-log-stats
func ProcessStatsQueryRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Obtain `by(...)` fields from the last `| stats` pipe in q.
	byFields, err := ca.q.GetStatsByFields()
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	var rows []statsRow
	var rowsLock sync.Mutex

	timestamp := ca.q.GetTimestamp()
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()
		columns := db.Columns
		clonedColumnNames := make([]string, len(columns))
		for i, c := range columns {
			clonedColumnNames[i] = strings.Clone(c.Name)
		}
		for i := 0; i < rowsCount; i++ {
			labels := make([]logstorage.Field, 0, len(byFields))
			for j, c := range columns {
				if slices.Contains(byFields, c.Name) {
					labels = append(labels, logstorage.Field{
						Name:  clonedColumnNames[j],
						Value: strings.Clone(c.Values[i]),
					})
				}
			}

			for j, c := range columns {
				if !slices.Contains(byFields, c.Name) {
					r := statsRow{
						Name:      clonedColumnNames[j],
						Labels:    labels,
						Timestamp: timestamp,
						Value:     strings.Clone(c.Values[i]),
					}

					rowsLock.Lock()
					rows = append(rows, r)
					rowsLock.Unlock()
				}
			}
		}
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Execute the query
	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		err = fmt.Errorf("cannot execute query [%s]: %s", ca.q, err)
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write response
	WriteStatsQueryResponse(w, rows)
}

type statsRow struct {
	Name      string
	Labels    []logstorage.Field
	Timestamp int64
	Value     string
}

// ProcessQueryRequest handles /select/logsql/query request.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-logs
func ProcessQueryRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Parse offset query arg
	offset, err := getPositiveInt(r, "offset")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Parse limit query arg
	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	sw := &syncWriter{
		w: w,
	}

	var bwShards atomicutil.Slice[bufferedWriter]
	bwShards.Init = func(shard *bufferedWriter) {
		shard.sw = sw
	}
	defer func() {
		shards := bwShards.All()
		for _, shard := range shards {
			shard.FlushIgnoreErrors()
		}
	}()

	if limit > 0 {
		// Add '| sort by (_time) desc | offset <offset> | limit <limit>' to the end of the query.
		// This pattern is automatically optimized during query execution - see https://github.com/VictoriaMetrics/VictoriaLogs/issues/96 .
		if ca.q.CanReturnLastNResults() {
			ca.q.AddPipeSortByTimeDesc()
		}
		ca.q.AddPipeOffsetLimit(uint64(offset), uint64(limit))
	}

	startTime := time.Now()
	writeResponseHeadersOnce := sync.OnceFunc(func() {
		// Write response headers
		h := w.Header()

		h.Set("Content-Type", "application/stream+json")
		ca.writeResponseHeaders(h, startTime)
	})

	writeBlock := func(workerID uint, db *logstorage.DataBlock) {
		writeResponseHeadersOnce()
		rowsCount := db.RowsCount()
		if rowsCount == 0 {
			return
		}
		columns := db.Columns

		bw := bwShards.Get(workerID)
		for i := 0; i < rowsCount; i++ {
			WriteJSONRow(bw, columns, i)
			if len(bw.buf) > 16*1024 {
				bw.FlushIgnoreErrors()
			}
		}
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Execute the query
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		httpserver.Errorf(w, r, "cannot execute query [%s]: %s", ca.q, err)
		return
	}

	// This call is needed for the case when the response didn't return any results.
	writeResponseHeadersOnce()
}

// ProcessTenantIDsRequest processes /select/tenant_ids request.
func ProcessTenantIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	accountID := r.Header.Get("AccountID")
	if accountID != "" {
		// Security measure - prevent from requesting tenant_ids for requests with the already specified tenant.
		// This allows enforcing the needed tenants at vmauth side, so they won't have access to /select/tenant_ids endpoint.
		// See https://docs.victoriametrics.com/victoriametrics/vmauth/#modifying-http-headers
		err := &httpserver.ErrorWithStatusCode{
			Err:        fmt.Errorf("the /select/tenant_ids enpoint cannot be requested with non-empty AccountID=%q header", accountID),
			StatusCode: http.StatusForbidden,
		}
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	start, okStart, err := getTimeNsec(r, "start")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	end, okEnd, err := getTimeNsec(r, "end")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	if !okStart {
		start = math.MinInt64
	}
	if !okEnd {
		end = math.MaxInt64
	} else {
		// Treat HTTP 'end' query arg as exclusive: [start, end)
		// Convert to inclusive bound for internal filter by subtracting 1ns.
		if end != math.MinInt64 {
			end--
		}
	}

	if start > end {
		httpserver.Errorf(w, r, "'start=%d' must be smaller than 'end=%d'", start, end)
		return
	}

	tenants, err := vlstorage.GetTenantIDs(ctx, start, end)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain tenantIDs: %s", err)
		return
	}

	data, err := json.Marshal(tenants)
	if err != nil {
		httpserver.Errorf(w, r, "cannot marshal tenantIDs to JSON: %s", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if _, err := w.Write(data); err != nil {
		httpserver.Errorf(w, r, "cannot send response to the client: %s", err)
		return
	}
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (sw *syncWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	n, err := sw.w.Write(p)
	sw.mu.Unlock()
	return n, err
}

type bufferedWriter struct {
	buf []byte
	sw  *syncWriter
}

func (bw *bufferedWriter) Write(p []byte) (int, error) {
	bw.buf = append(bw.buf, p...)

	// Do not send bw.buf to bw.sw here, since the data at bw.buf may be incomplete (it must end with '\n')

	return len(p), nil
}

func (bw *bufferedWriter) FlushIgnoreErrors() {
	_, _ = bw.sw.Write(bw.buf)
	bw.buf = bw.buf[:0]
}

type commonArgs struct {
	// The parsed query. It includes optional extra_filters, extra_stream_filters and (start, end) time range filter.
	q *logstorage.Query

	// tenantIDs is the list of tenantIDs to query.
	tenantIDs []logstorage.TenantID

	// Whether to allow partial response when some of vlstorage nodes are unavailable for querying.
	// This option makes sense only for cluster setup when vlselect queries vlstorage nodes.
	allowPartialResponse bool

	// Optional fields and field prefixes to hide during query execution.
	hiddenFieldsFilters []string

	// qs contains query execution statistics.
	qs logstorage.QueryStats
}

func (ca *commonArgs) newQueryContext(ctx context.Context) *logstorage.QueryContext {
	return logstorage.NewQueryContext(ctx, &ca.qs, ca.tenantIDs, ca.q, ca.allowPartialResponse, ca.hiddenFieldsFilters)
}

func (ca *commonArgs) updatePerQueryStatsMetrics() {
	vlstorage.UpdatePerQueryStatsMetrics(&ca.qs)
}

func parseCommonArgs(r *http.Request) (*commonArgs, error) {
	return parseCommonArgsWithConfig(r, false)
}

func parseCommonArgsWithConfig(r *http.Request, skipMaxRangeCheck bool) (*commonArgs, error) {
	// Extract tenantID
	tenantID, err := logstorage.GetTenantIDFromRequest(r)
	if err != nil {
		return nil, fmt.Errorf("cannot obtain tenantID: %w", err)
	}
	tenantIDs := []logstorage.TenantID{tenantID}

	// Parse optional start and end args
	start, startOK, err := getTimeNsec(r, "start")
	if err != nil {
		return nil, err
	}
	end, endOK, err := getTimeNsec(r, "end")
	if err != nil {
		return nil, err
	}
	// Treat HTTP 'end' query arg as exclusive: [start, end)
	// Convert to inclusive bound for internal filter by subtracting 1ns.
	if endOK {
		if end != math.MinInt64 {
			end--
		}
	}

	// Parse optional time arg
	timestamp, timeOK, err := getTimeNsec(r, "time")
	if err != nil {
		return nil, err
	}
	// decrease timestamp by one nanosecond in order to avoid capturing logs belonging
	// to the first nanosecond at the next period of time (month, week, day, hour, etc.)
	timestamp--

	currTimestamp := time.Now().UnixNano()
	if !timeOK {
		// If time arg is missing, then evaluate query either at the end timestamp (if it is set)
		// or at the current timestamp (if end query arg isn't set)
		if endOK {
			timestamp = end
		} else {
			timestamp = currTimestamp
		}
	}

	// Parse query
	qStr := r.FormValue("query")
	q, err := logstorage.ParseQueryAtTimestamp(qStr, timestamp)
	if err != nil {
		return nil, fmt.Errorf("cannot parse query [%s]: %s", qStr, err)
	}

	if startOK || endOK {
		// Add _time:[start, end] filter if start or end args were set.
		if !startOK {
			start = math.MinInt64
		}
		if !endOK {
			end = math.MaxInt64
		}
		q.AddTimeFilter(start, end)
	}

	// Parse optional extra_filters
	for _, extraFiltersStr := range r.Form["extra_filters"] {
		extraFilters, err := parseExtraFilters(extraFiltersStr)
		if err != nil {
			return nil, err
		}
		q.AddExtraFilters(extraFilters)
	}

	// Parse optional extra_stream_filters
	for _, extraStreamFiltersStr := range r.Form["extra_stream_filters"] {
		extraStreamFilters, err := parseExtraStreamFilters(extraStreamFiltersStr)
		if err != nil {
			return nil, err
		}
		q.AddExtraFilters(extraStreamFilters)
	}

	if maxRange := maxQueryTimeRange.Duration(); maxRange > 0 && !skipMaxRangeCheck {
		start, end := q.GetFilterTimeRange()
		if end > start {
			queryTimeRange := end - start
			if queryTimeRange < 0 || queryTimeRange > maxRange.Nanoseconds() {
				return nil, fmt.Errorf("too big time range selected: [%s, %s]; it cannot exceed -search.maxQueryTimeRange=%s; "+
					"see https://docs.victoriametrics.com/victorialogs/querying/#resource-usage-limits",
					timestampToString(start), timestampToString(end), maxRange)
			}
		}
	}

	allowPartialResponse := *allowPartialResponseFlag
	if err := getBoolFromRequest(&allowPartialResponse, r, "allow_partial_response"); err != nil {
		return nil, err
	}

	hiddenFieldsFilters, err := getStringSliceFromRequest(r, "hidden_fields_filters")
	if err != nil {
		return nil, err
	}

	ca := &commonArgs{
		q:         q,
		tenantIDs: tenantIDs,

		allowPartialResponse: allowPartialResponse,
		hiddenFieldsFilters:  hiddenFieldsFilters,
	}
	return ca, nil
}

func timestampToString(nsecs int64) string {
	t := time.Unix(nsecs/1e9, nsecs%1e9).UTC()
	return t.Format(time.RFC3339Nano)
}

func getTimeNsec(r *http.Request, argName string) (int64, bool, error) {
	s := r.FormValue(argName)
	if s == "" {
		return 0, false, nil
	}
	currentTimestamp := time.Now().UnixNano()
	nsecs, err := timeutil.ParseTimeAt(s, currentTimestamp)
	if err != nil {
		return 0, false, fmt.Errorf("cannot parse %s=%s: %w", argName, s, err)
	}
	return nsecs, true, nil
}

func parseExtraFilters(s string) (*logstorage.Filter, error) {
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, `{"`) {
		return logstorage.ParseFilter(s)
	}

	// Extra filters in the form {"field":"value",...}.
	kvs, err := parseExtraFiltersJSON(s)
	if err != nil {
		return nil, err
	}

	filters := make([]string, len(kvs))
	for i, kv := range kvs {
		if len(kv.values) == 1 {
			filters[i] = fmt.Sprintf("%q:=%q", kv.key, kv.values[0])
		} else {
			orValues := make([]string, len(kv.values))
			for j, v := range kv.values {
				orValues[j] = fmt.Sprintf("%q", v)
			}
			filters[i] = fmt.Sprintf("%q:in(%s)", kv.key, strings.Join(orValues, ","))
		}
	}
	s = strings.Join(filters, " ")
	return logstorage.ParseFilter(s)
}

func parseExtraStreamFilters(s string) (*logstorage.Filter, error) {
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, `{"`) {
		return logstorage.ParseFilter(s)
	}

	// Extra stream filters in the form {"field":"value",...}.
	kvs, err := parseExtraFiltersJSON(s)
	if err != nil {
		return nil, err
	}

	filters := make([]string, len(kvs))
	for i, kv := range kvs {
		if len(kv.values) == 1 {
			filters[i] = fmt.Sprintf("%q=%q", kv.key, kv.values[0])
		} else {
			orValues := make([]string, len(kv.values))
			for j, v := range kv.values {
				orValues[j] = regexp.QuoteMeta(v)
			}
			filters[i] = fmt.Sprintf("%q=~%q", kv.key, strings.Join(orValues, "|"))
		}
	}
	s = "{" + strings.Join(filters, ",") + "}"
	return logstorage.ParseFilter(s)
}

type extraFilter struct {
	key    string
	values []string
}

func parseExtraFiltersJSON(s string) ([]extraFilter, error) {
	v, err := fastjson.Parse(s)
	if err != nil {
		return nil, err
	}
	o := v.GetObject()

	var errOuter error
	var filters []extraFilter
	o.Visit(func(k []byte, v *fastjson.Value) {
		if errOuter != nil {
			return
		}
		switch v.Type() {
		case fastjson.TypeString:
			filters = append(filters, extraFilter{
				key:    string(k),
				values: []string{string(v.GetStringBytes())},
			})
		case fastjson.TypeArray:
			a := v.GetArray()
			if len(a) == 0 {
				return
			}
			orValues := make([]string, len(a))
			for i, av := range a {
				ov, err := av.StringBytes()
				if err != nil {
					errOuter = fmt.Errorf("cannot obtain string item at the array for key %q; item: %s", k, av)
					return
				}
				orValues[i] = string(ov)
			}
			filters = append(filters, extraFilter{
				key:    string(k),
				values: orValues,
			})
		default:
			errOuter = fmt.Errorf("unexpected type of value for key %q: %s; value: %s", k, v.Type(), v)
		}
	})
	if errOuter != nil {
		return nil, errOuter
	}
	return filters, nil
}

func getPositiveInt(r *http.Request, argName string) (int, error) {
	n, err := httputil.GetInt(r, argName)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("%q cannot be smaller than 0; got %d", argName, n)
	}
	return n, nil
}

func getBoolFromRequest(dst *bool, r *http.Request, argName string) error {
	s := r.FormValue(argName)
	if s == "" {
		return nil
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return fmt.Errorf("cannot parse %s=%q as bool: %w", argName, s, err)
	}
	*dst = b
	return nil
}

func getStringSliceFromRequest(r *http.Request, argName string) ([]string, error) {
	s := r.FormValue(argName)
	if s == "" {
		return nil, nil
	}

	if strings.HasPrefix(s, "[") {
		// Parse as a JSON array of strings.
		var a []string
		if err := json.Unmarshal([]byte(s), &a); err != nil {
			return nil, fmt.Errorf("cannot unmarshal JSON array from %s=%q: %w", argName, s, err)
		}
		return a, nil
	}

	// Parse as a comma-separated list of strings
	a := strings.Split(s, ",")
	return a, nil
}

func (ca *commonArgs) writeResponseHeaders(h http.Header, startTime time.Time) {
	// Write request duration
	accessControlExposeHeaders := []string{"VL-Request-Duration-Seconds"}
	h.Set("VL-Request-Duration-Seconds", fmt.Sprintf("%.3f", time.Since(startTime).Seconds()))

	if len(ca.tenantIDs) == 1 {
		// Write the used AccountID and ProjectID, so the client could show them properly.
		accessControlExposeHeaders = append(accessControlExposeHeaders, "AccountID", "ProjectID")
		tenantID := ca.tenantIDs[0]
		h.Set("AccountID", fmt.Sprintf("%d", tenantID.AccountID))
		h.Set("ProjectID", fmt.Sprintf("%d", tenantID.ProjectID))
	}

	for i, v := range accessControlExposeHeaders {
		accessControlExposeHeaders[i] = http.CanonicalHeaderKey(v)
	}
	h.Set("Access-Control-Expose-Headers", strings.Join(accessControlExposeHeaders, ", "))
}
