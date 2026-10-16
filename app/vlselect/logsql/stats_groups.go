package logsql

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// The `groups` response format for /select/logsql/stats_query and /select/logsql/stats_query_range.
//
// It is enabled by passing `format=groups` query arg.
//
// Instead of returning a separate point per result column per group (plus a separate point per histogram
// bucket in case of the histogram() stats function), it returns a single entry per `by (...)` group with
// all its result columns inside:
//
//	{"status":"success","data":{"resultType":"groups","result":[
//	  {"metric":{"destination.ip":"10.0.0.1"},"timestamp":1791006420.195,
//	   "values":{"histogram(\"fgt.duration\")":[{"vmrange":"1.0e+00...1.1e+00","hits":17}],"sum(fgt.duration)":"123"}}}
//	]}}
//
// For stats_query_range the values contain `[[timestamp,value],...]` points per column:
//
//	{"metric":{...},"values":{"histogram(\"fgt.duration\")":[[1791000000,[{"vmrange":"...","hits":1}]],...],"sum(f)":[[1791000000,"5"],...]}}
//
// This format avoids duplicating the full label set for every histogram bucket of every group,
// which shrinks both the response size and the CPU needed for building it.

// processStatsQueryGroupsRequest executes the stats query from ca and writes the results to w in the `groups` format.
func processStatsQueryGroupsRequest(ctx context.Context, ca *commonArgs, w http.ResponseWriter, r *http.Request, labelFields []string, resultColumns []logstorage.StatsResultColumn) {
	labelSet := newStringSet(labelFields)
	colTypes := newColumnTypes(resultColumns)

	var rowsLock sync.Mutex
	var rows [][]byte

	timestamp := ca.q.GetTimestamp()
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()
		columns := db.GetColumns(false)

		fragments := make([][]byte, 0, rowsCount)
		for i := range rowsCount {
			fragments = append(fragments, appendStatsGroupsRow(nil, columns, i, timestamp, labelSet, colTypes))
		}

		rowsLock.Lock()
		rows = append(rows, fragments...)
		rowsLock.Unlock()
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		err = fmt.Errorf("cannot execute query [%s]: %w", ca.q, err)
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	bw := newChunksWriter(w)
	defer bw.close()
	bw.writeString(`{"status":"success","data":{"resultType":"groups","result":[`)
	for i, row := range rows {
		if i > 0 {
			bw.writeString(",")
		}
		bw.write(row)
	}
	bw.writeString(`]}}`)
}

// appendStatsGroupsRow appends a single group entry in the `groups` format to dst.
func appendStatsGroupsRow(dst []byte, columns []logstorage.BlockColumn, rowIdx int, timestamp int64, labelSet map[string]struct{}, colTypes map[string]bool) []byte {
	dst = append(dst, `{"metric":{`...)
	first := true
	for j := range columns {
		c := &columns[j]
		if _, ok := labelSet[c.Name]; !ok {
			continue
		}
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = appendJSONString(dst, c.Name)
		dst = append(dst, ':')
		dst = appendJSONString(dst, c.Values[rowIdx])
	}
	dst = append(dst, '}')

	dst = append(dst, `,"timestamp":`...)
	dst = strconv.AppendFloat(dst, float64(timestamp)/1e9, 'f', -1, 64)

	dst = append(dst, `,"values":{`...)
	first = true
	for j := range columns {
		c := &columns[j]
		if _, ok := labelSet[c.Name]; ok {
			continue
		}
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = appendJSONString(dst, c.Name)
		dst = append(dst, ':')

		v := c.Values[rowIdx]
		if colTypes[c.Name] && len(v) > 0 && v[0] == '[' {
			// The column contains the result of the histogram() stats function -
			// a JSON array of `{"vmrange":"...","hits":N}` objects. Embed it as is.
			dst = append(dst, v...)
		} else {
			dst = appendJSONString(dst, v)
		}
	}
	dst = append(dst, '}', '}')
	return dst
}

// statsGroupsRangePoint is a single point of a single result column of a single group for stats_query_range.
type statsGroupsRangePoint struct {
	ts int64

	// value contains the raw column value: a JSON bucket array for histogram() columns, an unescaped string otherwise.
	value string
}

// statsGroupsRangeColumn holds all the points of a single result column of a single group.
type statsGroupsRangeColumn struct {
	name string
	hist bool

	points []statsGroupsRangePoint
}

// statsGroupsRangeSeries holds all the result columns for a single `by (...)` group.
type statsGroupsRangeSeries struct {
	labels []logstorage.Field
	key    string
	cols   []*statsGroupsRangeColumn
}

// processStatsQueryRangeGroupsRequest executes the range stats query from ca and writes the results to w in the `groups` format.
func processStatsQueryRangeGroupsRequest(ctx context.Context, ca *commonArgs, w http.ResponseWriter, r *http.Request, step, offset int64, labelFields []string, resultColumns []logstorage.StatsResultColumn) {
	labelSet := newStringSet(labelFields)
	colTypes := newColumnTypes(resultColumns)

	var mLock sync.Mutex
	m := make(map[string]*statsGroupsRangeSeries)

	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()
		columns := db.GetColumns(false)

		for i := range rowsCount {
			// Do not move q.GetTimestamp() outside writeBlock, since ts
			// must be initialized to query timestamp for every processed log row.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8312
			ts := ca.q.GetTimestamp()

			var labels []logstorage.Field
			var metricCols []*logstorage.BlockColumn
			for j := range columns {
				c := &columns[j]
				if c.Name == "_time" {
					if nsec, ok := logstorage.TryParseTimestampRFC3339Nano(c.Values[i]); ok {
						ts = nsec
						continue
					}
				}
				if _, ok := labelSet[c.Name]; ok {
					labels = append(labels, logstorage.Field{
						Name:  c.Name,
						Value: strings.Clone(c.Values[i]),
					})
					continue
				}
				metricCols = append(metricCols, c)
			}

			key := string(logstorage.MarshalFieldsToJSON(nil, labels))

			mLock.Lock()
			ss := m[key]
			if ss == nil {
				ss = &statsGroupsRangeSeries{
					labels: labels,
					key:    key,
				}
				m[key] = ss
			}
			for _, c := range metricCols {
				col := findStatsGroupsRangeColumn(ss, c.Name, colTypes[c.Name])
				col.points = append(col.points, statsGroupsRangePoint{
					ts:    ts,
					value: strings.Clone(c.Values[i]),
				})
			}
			mLock.Unlock()
		}
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		err = fmt.Errorf("cannot execute query [%s]: %w", ca.q, err)
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Sort the collected stats by _time
	seriesList := make([]*statsGroupsRangeSeries, 0, len(m))
	for _, ss := range m {
		for _, col := range ss.cols {
			points := col.points
			sort.Slice(points, func(i, j int) bool {
				return points[i].ts < points[j].ts
			})
		}
		seriesList = append(seriesList, ss)
	}
	sort.Slice(seriesList, func(i, j int) bool {
		return seriesList[i].key < seriesList[j].key
	})

	// Write response headers
	h := w.Header()
	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	bw := newChunksWriter(w)
	defer bw.close()
	bw.writeString(`{"status":"success","data":{"resultType":"groups","result":[`)
	for i, ss := range seriesList {
		if i > 0 {
			bw.writeString(",")
		}
		bw.write(appendStatsGroupsRangeSeries(nil, ss))
	}
	bw.writeString(`]}}`)
}

// findStatsGroupsRangeColumn returns the column with the given name from ss, creating it if needed.
func findStatsGroupsRangeColumn(ss *statsGroupsRangeSeries, name string, hist bool) *statsGroupsRangeColumn {
	for _, col := range ss.cols {
		if col.name == name {
			return col
		}
	}
	col := &statsGroupsRangeColumn{
		name: name,
		hist: hist,
	}
	ss.cols = append(ss.cols, col)
	return col
}

// appendStatsGroupsRangeSeries appends a single group entry in the `groups` format to dst.
func appendStatsGroupsRangeSeries(dst []byte, ss *statsGroupsRangeSeries) []byte {
	dst = append(dst, `{"metric":{`...)
	for i, label := range ss.labels {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, label.Name)
		dst = append(dst, ':')
		dst = appendJSONString(dst, label.Value)
	}
	dst = append(dst, `},"values":{`...)
	for i, col := range ss.cols {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, col.name)
		dst = append(dst, ':', '[')
		for j, p := range col.points {
			if j > 0 {
				dst = append(dst, ',')
			}
			dst = append(dst, '[')
			dst = strconv.AppendFloat(dst, float64(p.ts)/1e9, 'f', -1, 64)
			dst = append(dst, ',')
			if col.hist && len(p.value) > 0 && p.value[0] == '[' {
				// The point contains the result of the histogram() stats function -
				// a JSON array of `{"vmrange":"...","hits":N}` objects. Embed it as is.
				dst = append(dst, p.value...)
			} else {
				dst = appendJSONString(dst, p.value)
			}
			dst = append(dst, ']')
		}
		dst = append(dst, ']')
	}
	dst = append(dst, '}', '}')
	return dst
}

// chunksWriter accumulates written chunks and flushes them to the underlying http.ResponseWriter in 64KB pieces.
type chunksWriter struct {
	w http.ResponseWriter
	b []byte
}

func newChunksWriter(w http.ResponseWriter) *chunksWriter {
	return &chunksWriter{
		w: w,
		b: make([]byte, 0, 64*1024),
	}
}

func (cw *chunksWriter) write(chunk []byte) {
	if len(cw.b)+len(chunk) > 64*1024 {
		cw.flush()
	}
	cw.b = append(cw.b, chunk...)
}

func (cw *chunksWriter) writeString(s string) {
	cw.write([]byte(s))
}

func (cw *chunksWriter) flush() {
	if len(cw.b) == 0 {
		return
	}
	_, _ = cw.w.Write(cw.b)
	cw.b = cw.b[:0]
}

func (cw *chunksWriter) close() {
	cw.flush()
}

func newStringSet(fields []string) map[string]struct{} {
	set := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		set[f] = struct{}{}
	}
	return set
}

func newColumnTypes(resultColumns []logstorage.StatsResultColumn) map[string]bool {
	colTypes := make(map[string]bool, len(resultColumns))
	for _, rc := range resultColumns {
		colTypes[rc.Name] = rc.IsHistogram
	}
	return colTypes
}

const hexDigits = "0123456789abcdef"

// appendJSONString appends the JSON representation of s to dst.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"', c == '\\':
			dst = append(dst, '\\', c)
		case c == '\n':
			dst = append(dst, '\\', 'n')
		case c == '\r':
			dst = append(dst, '\\', 'r')
		case c == '\t':
			dst = append(dst, '\\', 't')
		case c < 0x20:
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		default:
			dst = append(dst, c)
		}
	}
	return append(dst, '"')
}
