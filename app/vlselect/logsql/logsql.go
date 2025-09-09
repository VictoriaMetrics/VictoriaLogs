package logsql

import (
	"context"
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
)

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

		// Fetch columns by name to avoid relying on column reordering, which may change after sort/partition pipes
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
	writeRequestDuration(h, startTime)

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
			timestampStr := strings.Clone(timestampValues[i])
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
				k := string(bb.B)
				hs = &hitsSeries{}
				m[k] = hs
			}
			hs.timestamps = append(hs.timestamps, timestampStr)
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

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	writeRequestDuration(h, startTime)

	// The VL-Selected-Time-Range contains the time range specified in the query, not counting (start, end) and extra_filters
	// It is used by the built-in web UI in order to adjust the selected time range.
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/558#issuecomment-3180070712
	h.Set("VL-Selected-Time-Range", ca.getSelectedTimeRange())

	// Write response
	WriteHitsSeries(w, m)
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

	hitsOther := make(map[string]uint64)
	for _, x := range a[fieldsLimit:] {
		for i, timestampStr := range x.hs.timestamps {
			hitsOther[timestampStr] += x.hs.hits[i]
		}
	}
	var hsOther hitsSeries
	for timestampStr, hits := range hitsOther {
		hsOther.timestamps = append(hsOther.timestamps, timestampStr)
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
	timestamps []string
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
	writeRequestDuration(h, startTime)

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
	writeRequestDuration(h, startTime)

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
	writeRequestDuration(h, startTime)

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
	writeRequestDuration(h, startTime)

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
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	writeRequestDuration(h, startTime)

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
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	writeRequestDuration(h, startTime)

	// Write results
	WriteValuesWithHitsJSON(w, streams)
}

// ProcessLiveTailRequest processes live tailing request to /select/logsq/tail
//
// See https://docs.victoriametrics.com/victorialogs/querying/#live-tailing
func ProcessLiveTailRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	liveTailRequests.Inc()
	defer liveTailRequests.Dec()

	ca, err := parseCommonArgs(r)
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
	writeRequestDuration(h, startTime)

	// The VL-Selected-Time-Range contains the time range specified in the query, not counting (start, end) and extra_filters
	// It is used by the built-in web UI in order to adjust the selected time range.
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/558#issuecomment-3180070712
	h.Set("VL-Selected-Time-Range", ca.getSelectedTimeRange())

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
	writeRequestDuration(h, startTime)

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
		writeRequestDuration(h, startTime)
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

	// minTimestamp and maxTimestamp is the time range specified in the original query,
	// without taking into account extra_filters and (start, end) query args.
	minTimestamp int64
	maxTimestamp int64

	// qs contains query execution statistics.
	qs logstorage.QueryStats
}

func (ca *commonArgs) newQueryContext(ctx context.Context) *logstorage.QueryContext {
	return logstorage.NewQueryContext(ctx, &ca.qs, ca.tenantIDs, ca.q)
}

func (ca *commonArgs) updatePerQueryStatsMetrics() {
	vlstorage.UpdatePerQueryStatsMetrics(&ca.qs)
}

func (ca *commonArgs) getSelectedTimeRange() string {
	return fmt.Sprintf("[%d,%d]", ca.minTimestamp, ca.maxTimestamp)
}

func parseCommonArgs(r *http.Request) (*commonArgs, error) {
	// Extract tenantID
	tenantID, err := logstorage.GetTenantIDFromRequest(r)
	if err != nil {
		return nil, fmt.Errorf("cannot obtain tenantID: %w", err)
	}
	tenantIDs := []logstorage.TenantID{tenantID}

	// Parse optional start and end args
	start, startStr, err := getTimeNsec(r, "start")
	if err != nil {
		return nil, err
	}
	end, endStr, err := getTimeNsec(r, "end")
	if err != nil {
		return nil, err
	}
	// Adjust end time according to its string representation
	end = logstorage.AdjustEndTimestamp(end, endStr)

	// Parse optional time arg
	timestamp, timeStr, err := getTimeNsec(r, "time")
	if err != nil {
		return nil, err
	}
	// decrease timestamp by one nanosecond in order to avoid capturing logs belonging
	// to the first nanosecond at the next period of time (month, week, day, hour, etc.)
	timestamp--

	if timeStr == "" {
		// If time arg is missing, then evaluate query either at the end timestamp (if it is set)
		// or at the current timestamp (if end query arg isn't set)
		if endStr != "" {
			timestamp = end
		} else {
			timestamp = time.Now().UnixNano()
		}
	}

	// Parse query
	qStr := r.FormValue("query")
	q, err := logstorage.ParseQueryAtTimestamp(qStr, timestamp)
	if err != nil {
		return nil, fmt.Errorf("cannot parse query [%s]: %s", qStr, err)
	}

	minTimestamp, maxTimestamp := q.GetFilterTimeRange()

	if startStr != "" || endStr != "" {
		// Add _time:[start, end] filter if start or end args were set.
		if startStr == "" {
			start = math.MinInt64
		}
		if endStr == "" {
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

	if minTimestamp == math.MinInt64 || maxTimestamp == math.MaxInt64 {
		// The original time range is open-bounded.
		// Override it with the (start, end) time range in this case.
		minTimestamp, maxTimestamp = q.GetFilterTimeRange()
		if maxTimestamp == math.MaxInt64 {
			maxTimestamp = timestamp
		}
	}

	if maxRange := maxQueryTimeRange.Duration(); maxRange > 0 {
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

	ca := &commonArgs{
		q:         q,
		tenantIDs: tenantIDs,

		minTimestamp: minTimestamp,
		maxTimestamp: maxTimestamp,
	}
	return ca, nil
}

func timestampToString(nsecs int64) string {
	t := time.Unix(nsecs/1e9, nsecs%1e9).UTC()
	return t.Format(time.RFC3339Nano)
}

func getTimeNsec(r *http.Request, argName string) (int64, string, error) {
	s := r.FormValue(argName)
	if s == "" {
		return 0, "", nil
	}
	currentTimestamp := time.Now().UnixNano()
	nsecs, err := timeutil.ParseTimeAt(s, currentTimestamp)
	if err != nil {
		return 0, "", fmt.Errorf("cannot parse %s=%s: %w", argName, s, err)
	}
	return nsecs, s, nil
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

func writeRequestDuration(h http.Header, startTime time.Time) {
	h.Set("Access-Control-Expose-Headers", "VL-Request-Duration-Seconds")
	h.Set("VL-Request-Duration-Seconds", fmt.Sprintf("%.3f", time.Since(startTime).Seconds()))
}
