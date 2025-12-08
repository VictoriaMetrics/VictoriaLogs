package internalselect

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netselect"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var maxConcurrentRequests = flag.Int("internalselect.maxConcurrentRequests", 100, "The limit on the number of concurrent requests to /internal/select/* endpoints; "+
	"other requests are put into the wait queue; see https://docs.victoriametrics.com/victorialogs/cluster/")

// RequestHandler processes requests to /internal/select/*
func RequestHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	select {
	case concurrencyLimitCh <- struct{}{}:
		if d := time.Since(startTime); d > 100*time.Millisecond {
			// Measure the wait duration for requests, which hit the concurrency limit and waited for more than 100 milliseconds to be executed.
			concurrentRequestsWaitDuration.Update(d.Seconds())
		}
		requestHandler(ctx, w, r, startTime)
		<-concurrencyLimitCh
	case <-ctx.Done():
		// Unconditionally measure the wait time until the the request is canceled by the client.
		concurrentRequestsWaitDuration.UpdateDuration(startTime)
	}
}

// Init initializes internalselect package.
func Init() {
	concurrencyLimitCh = make(chan struct{}, *maxConcurrentRequests)
}

// Stop stops vlselect
func Stop() {
	concurrencyLimitCh = nil
}

var concurrencyLimitCh chan struct{}

var concurrentRequestsWaitDuration = metrics.NewSummary(`vl_concurrent_internalselect_requests_wait_duration`)

func requestHandler(ctx context.Context, w http.ResponseWriter, r *http.Request, startTime time.Time) {
	path := r.URL.Path
	rh := requestHandlers[path]
	if rh == nil {
		httpserver.Errorf(w, r, "unsupported endpoint requested: %s", path)
		return
	}

	metrics.GetOrCreateCounter(fmt.Sprintf(`vl_http_requests_total{path=%q}`, path)).Inc()
	if err := rh(ctx, w, r); err != nil && !netutil.IsTrivialNetworkError(err) {
		metrics.GetOrCreateCounter(fmt.Sprintf(`vl_http_request_errors_total{path=%q}`, path)).Inc()
		httpserver.Errorf(w, r, "%s", err)
		// The return is skipped intentionally in order to track the duration of failed queries.
	}
	metrics.GetOrCreateSummary(fmt.Sprintf(`vl_http_request_duration_seconds{path=%q}`, path)).UpdateDuration(startTime)
}

var requestHandlers = map[string]func(ctx context.Context, w http.ResponseWriter, r *http.Request) error{
	"/internal/select/query":               processQueryRequest,
	"/internal/select/field_names":         processFieldNamesRequest,
	"/internal/select/field_values":        processFieldValuesRequest,
	"/internal/select/stream_field_names":  processStreamFieldNamesRequest,
	"/internal/select/stream_field_values": processStreamFieldValuesRequest,
	"/internal/select/streams":             processStreamsRequest,
	"/internal/select/stream_ids":          processStreamIDsRequest,
	"/internal/select/tenant_ids":          processTenantIDsRequest,

	"/internal/delete/run_task":     processDeleteRunTask,
	"/internal/delete/stop_task":    processDeleteStopTask,
	"/internal/delete/active_tasks": processDeleteActiveTasks,
}

func processQueryRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.QueryProtocolVersion)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	var wLock sync.Mutex
	var dataLenBuf []byte

	sendBuf := func(bb *bytesutil.ByteBuffer) error {
		if len(bb.B) == 0 {
			return nil
		}

		data := bb.B
		if !cp.DisableCompression {
			bufLen := len(bb.B)
			bb.B = zstd.CompressLevel(bb.B, bb.B, 1)
			data = bb.B[bufLen:]
		}

		wLock.Lock()
		dataLenBuf = encoding.MarshalUint64(dataLenBuf[:0], uint64(len(data)))
		_, err := w.Write(dataLenBuf)
		if err == nil {
			_, err = w.Write(data)
		}
		wLock.Unlock()

		// Reset the sent buf
		bb.Reset()

		return err
	}

	var bufs atomicutil.Slice[bytesutil.ByteBuffer]

	var errGlobal atomic.Pointer[error]

	writeBlock := func(workerID uint, db *logstorage.DataBlock) {
		if errGlobal.Load() != nil {
			return
		}

		bb := bufs.Get(workerID)

		// Write the marker of a regular data block.
		bb.B = append(bb.B, 0)

		// Marshal the data block.
		bb.B = db.Marshal(bb.B)

		if len(bb.B) < 1024*1024 {
			// Fast path - the bb is too small to be sent to the client yet.
			return
		}

		// Slow path - the bb must be sent to the client.
		if err := sendBuf(bb); err != nil {
			errGlobal.CompareAndSwap(nil, &err)
		}
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		return err
	}
	if errP := errGlobal.Load(); errP != nil {
		return *errP
	}

	// Send the remaining data
	for _, bb := range bufs.All() {
		if err := sendBuf(bb); err != nil {
			return err
		}
	}

	// Send the query stats block.
	bb := bufs.Get(0)
	// Write the marker of query stats block.
	bb.B = append(bb.B, 1)
	// Marshal the block itself
	bb.B = marshalQueryStatsBlock(bb.B, qctx)
	return sendBuf(bb)
}

func processFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.FieldNamesProtocolVersion)
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	fieldNames, err := vlstorage.GetFieldNames(qctx)
	if err != nil {
		return fmt.Errorf("cannot obtain field names: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldNames, cp.DisableCompression)
}

func processFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.FieldValuesProtocolVersion)
	if err != nil {
		return err
	}

	fieldName := r.FormValue("field")

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	fieldValues, err := vlstorage.GetFieldValues(qctx, fieldName, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain field values: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldValues, cp.DisableCompression)
}

func processStreamFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamFieldNamesProtocolVersion)
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	fieldNames, err := vlstorage.GetStreamFieldNames(qctx)
	if err != nil {
		return fmt.Errorf("cannot obtain stream field names: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldNames, cp.DisableCompression)
}

func processStreamFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamFieldValuesProtocolVersion)
	if err != nil {
		return err
	}

	fieldName := r.FormValue("field")

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	fieldValues, err := vlstorage.GetStreamFieldValues(qctx, fieldName, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain stream field values: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldValues, cp.DisableCompression)
}

func processStreamsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamsProtocolVersion)
	if err != nil {
		return err
	}

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	streams, err := vlstorage.GetStreams(qctx, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain streams: %w", err)
	}

	return writeValuesWithHits(w, qctx, streams, cp.DisableCompression)
}

func processStreamIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamIDsProtocolVersion)
	if err != nil {
		return err
	}

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	streamIDs, err := vlstorage.GetStreamIDs(qctx, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain streams: %w", err)
	}

	return writeValuesWithHits(w, qctx, streamIDs, cp.DisableCompression)
}

func processDeleteRunTask(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	if err := checkProtocolVersion(r, netselect.DeleteRunTaskProtocolVersion); err != nil {
		return err
	}

	// Parse query args
	taskID := r.FormValue("task_id")
	if taskID == "" {
		return fmt.Errorf("missing task_id arg")
	}

	timestamp, err := getInt64FromRequest(r, "timestamp")
	if err != nil {
		return err
	}

	tenantIDsStr := r.FormValue("tenant_ids")
	tenantIDs, err := logstorage.UnmarshalTenantIDsFromJSON([]byte(tenantIDsStr))
	if err != nil {
		return fmt.Errorf("cannot unmarshal tenant_ids=%q: %w", tenantIDsStr, err)
	}

	fStr := r.FormValue("filter")
	f, err := logstorage.ParseFilter(fStr)
	if err != nil {
		return fmt.Errorf("cannot unmarshal filter=%q: %w", fStr, err)
	}

	// Execute the delete task
	return vlstorage.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, f)
}

func processDeleteStopTask(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	if err := checkProtocolVersion(r, netselect.DeleteStopTaskProtocolVersion); err != nil {
		return err
	}

	taskID := r.FormValue("task_id")
	if taskID == "" {
		return fmt.Errorf("missing task_id arg")
	}

	return vlstorage.DeleteStopTask(ctx, taskID)
}

func processDeleteActiveTasks(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	if err := checkProtocolVersion(r, netselect.DeleteActiveTasksProtocolVersion); err != nil {
		return err
	}

	tasks, err := vlstorage.DeleteActiveTasks(ctx)
	if err != nil {
		return err
	}

	data := logstorage.MarshalDeleteTasksToJSON(tasks)

	w.Header().Set("Content-Type", "application/json")

	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("cannot send response to the client: %w", err)
	}

	return nil
}

func processTenantIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	start, err := getInt64FromRequest(r, "start")
	if err != nil {
		return err
	}
	end, err := getInt64FromRequest(r, "end")
	if err != nil {
		return err
	}

	tenantIDs, err := vlstorage.GetTenantIDs(ctx, start, end)
	if err != nil {
		return fmt.Errorf("cannot obtain tenant IDs: %w", err)
	}

	// Marshal tenantIDs at first
	data, err := json.Marshal(tenantIDs)
	if err != nil {
		return fmt.Errorf("cannot marshal tenantIDs: %w", err)
	}

	// Send the marshaled tenantIDs to the client
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("cannot send response to the client: %w", err)
	}
	return nil
}

type commonParams struct {
	TenantIDs []logstorage.TenantID
	Query     *logstorage.Query

	// Whether to disable compression of the response sent to the vlselect.
	DisableCompression bool

	// Whether to allow partial response when some of vlstorage nodes are unavailable.
	AllowPartialResponse bool

	// Optional list of log fields or log field prefixes ending with *, which must be hidden during query execution.
	HiddenFieldsFilters []string

	// qs contains execution statistics for the Query.
	qs logstorage.QueryStats
}

func (cp *commonParams) NewQueryContext(ctx context.Context) *logstorage.QueryContext {
	return logstorage.NewQueryContext(ctx, &cp.qs, cp.TenantIDs, cp.Query, cp.AllowPartialResponse, cp.HiddenFieldsFilters)
}

func (cp *commonParams) UpdatePerQueryStatsMetrics() {
	vlstorage.UpdatePerQueryStatsMetrics(&cp.qs)
}

func getCommonParams(r *http.Request, expectedProtocolVersion string) (*commonParams, error) {
	if err := checkProtocolVersion(r, expectedProtocolVersion); err != nil {
		return nil, err
	}

	tenantIDsStr := r.FormValue("tenant_ids")
	tenantIDs, err := logstorage.UnmarshalTenantIDsFromJSON([]byte(tenantIDsStr))
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tenant_ids=%q: %w", tenantIDsStr, err)
	}

	timestamp, err := getInt64FromRequest(r, "timestamp")
	if err != nil {
		return nil, err
	}

	qStr := r.FormValue("query")
	q, err := logstorage.ParseQueryAtTimestamp(qStr, timestamp)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal query=%q: %w", qStr, err)
	}

	disableCompression, err := getBoolFromRequest(r, "disable_compression")
	if err != nil {
		return nil, err
	}

	allowPartialResponse, err := getBoolFromRequest(r, "allow_partial_response")
	if err != nil {
		return nil, err
	}

	hiddenFieldsFilters, err := getStringSliceFromRequest(r, "hidden_fields_filters")
	if err != nil {
		return nil, err
	}

	cp := &commonParams{
		TenantIDs: tenantIDs,
		Query:     q,

		DisableCompression: disableCompression,

		AllowPartialResponse: allowPartialResponse,
		HiddenFieldsFilters:  hiddenFieldsFilters,
	}
	return cp, nil
}

func checkProtocolVersion(r *http.Request, expectedProtocolVersion string) error {
	version := r.FormValue("version")
	if version != expectedProtocolVersion {
		return fmt.Errorf("unexpected protocol version=%q; want %q; the most likely cause of this error is different versions of VictoriaLogs cluster components; "+
			"make sure VictoriaLogs compoments have the same release version", version, expectedProtocolVersion)
	}
	return nil
}

func writeValuesWithHits(w http.ResponseWriter, qctx *logstorage.QueryContext, vhs []logstorage.ValueWithHits, disableCompression bool) error {
	var b []byte

	// Marshal vhs at first
	b = encoding.MarshalUint64(b, uint64(len(vhs)))
	for i := range vhs {
		b = vhs[i].Marshal(b)
	}

	// Marshal query stats block after that
	b = marshalQueryStatsBlock(b, qctx)

	if !disableCompression {
		b = zstd.CompressLevel(nil, b, 1)
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("cannot send response to the client: %w", err)
	}

	return nil
}

func marshalQueryStatsBlock(dst []byte, qctx *logstorage.QueryContext) []byte {
	queryDurationNsecs := qctx.QueryDurationNsecs()
	db := qctx.QueryStats.CreateDataBlock(queryDurationNsecs)
	dst = db.Marshal(dst)
	return dst
}

func getInt64FromRequest(r *http.Request, argName string) (int64, error) {
	s := r.FormValue(argName)
	if s == "" {
		return 0, fmt.Errorf("missing the required arg %s", argName)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %s=%q: %w", argName, s, err)
	}
	return n, nil
}

func getBoolFromRequest(r *http.Request, argName string) (bool, error) {
	s := r.FormValue(argName)
	if s == "" {
		return false, fmt.Errorf("missing the required arg %s", argName)
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, fmt.Errorf("cannot parse %s=%q as bool: %w", argName, s, err)
	}

	return b, nil
}

func getStringSliceFromRequest(r *http.Request, argName string) ([]string, error) {
	s := r.FormValue(argName)
	if s == "" {
		return nil, fmt.Errorf("missing the required arg %s", argName)
	}

	var a []string
	if err := json.Unmarshal([]byte(s), &a); err != nil {
		return nil, fmt.Errorf("cannot unmarshal JSON array from %s=%q: %w", argName, s, err)
	}

	return a, nil
}
