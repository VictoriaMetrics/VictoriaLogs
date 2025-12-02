package vlselect

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/internalselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/logsql"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	maxConcurrentRequests = flag.Int("search.maxConcurrentRequests", getDefaultMaxConcurrentRequests(), "The maximum number of concurrent search requests. "+
		"It shouldn't be high, since a single request can saturate all the CPU cores, while many concurrently executed requests may require high amounts of memory. "+
		"See also -search.maxQueueDuration")
	maxQueueDuration = flag.Duration("search.maxQueueDuration", 10*time.Second, "The maximum time the search request waits for execution when -search.maxConcurrentRequests "+
		"limit is reached; see also -search.maxQueryDuration")
	maxQueryDuration = flag.Duration("search.maxQueryDuration", time.Second*30, "The maximum duration for query execution. It can be overridden to a smaller value on a per-query basis via 'timeout' query arg")

	disableSelect         = flag.Bool("select.disable", false, "Whether to disable /select/* HTTP endpoints")
	disableInternalSelect = flag.Bool("internalselect.disable", false, "Whether to disable /internal/select/* HTTP endpoints")

	enableDelete         = flag.Bool("delete.enable", false, "Whether to enable /delete/* HTTP endpoints; see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs")
	enableInternalDelete = flag.Bool("internaldelete.enable", false, "Whether to enable /internal/delete/* HTTP endpoints, which are used by vlselect for deleting logs "+
		"via delete API at vlstorage nodes; see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs")
	logSlowQueryDuration = flag.Duration("search.logSlowQueryDuration", 5*time.Second,
		"Log queries with execution time exceeding this value. Zero disables slow query logging")
)

func getDefaultMaxConcurrentRequests() int {
	n := cgroup.AvailableCPUs()
	if n <= 4 {
		n *= 2
	}
	if n > 16 {
		// A single request can saturate all the CPU cores, so there is no sense
		// in allowing higher number of concurrent requests - they will just contend
		// for unavailable CPU time.
		n = 16
	}
	return n
}

// Init initializes vlselect
func Init() {
	concurrencyLimitCh = make(chan struct{}, *maxConcurrentRequests)

	internalselect.Init()
}

// Stop stops vlselect
func Stop() {
	internalselect.Stop()

	concurrencyLimitCh = nil
}

var concurrencyLimitCh chan struct{}

var (
	concurrencyLimitReached = metrics.NewCounter(`vl_concurrent_select_limit_reached_total`)
	concurrencyLimitTimeout = metrics.NewCounter(`vl_concurrent_select_limit_timeout_total`)

	_ = metrics.NewGauge(`vl_concurrent_select_capacity`, func() float64 {
		return float64(cap(concurrencyLimitCh))
	})
	_ = metrics.NewGauge(`vl_concurrent_select_current`, func() float64 {
		return float64(len(concurrencyLimitCh))
	})
)

//go:embed vmui
var vmuiFiles embed.FS

var vmuiFileServer = http.FileServer(http.FS(vmuiFiles))

// RequestHandler handles select requests for VictoriaLogs
func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
	path := strings.ReplaceAll(r.URL.Path, "//", "/")

	if strings.HasPrefix(path, "/delete/") {
		if !*enableDelete {
			httpserver.Errorf(w, r, "requests to /delete/* are disabled; pass -delete.enable command-line flag for enabling them; "+
				"see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs")
			return true
		}
		deleteHandler(w, r, path)
		return true
	}

	if strings.HasPrefix(path, "/select/") {
		if *disableSelect {
			httpserver.Errorf(w, r, "requests to /select/* are disabled with -select.disable command-line flag")
			return true
		}

		return selectHandler(w, r, path)
	}

	if strings.HasPrefix(path, "/internal/delete/") {
		if !*enableInternalDelete {
			httpserver.Errorf(w, r, "requests to /internal/delete/*` are disabled; pass -internaldelete.enable command-line flag for enabling them; "+
				"see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs")
			return true
		}
		internalselect.RequestHandler(r.Context(), w, r)
		return true
	}

	if strings.HasPrefix(path, "/internal/select/") {
		if *disableInternalSelect {
			httpserver.Errorf(w, r, "requests to /internal/select/* are disabled with -internalselect.disable command-line flag")
			return true
		}
		if *disableSelect {
			httpserver.Errorf(w, r, "requests to /internal/select/* are disabled with -select.disable command-line flag")
			return true
		}
		internalselect.RequestHandler(r.Context(), w, r)
		return true
	}

	return false
}

func selectHandler(w http.ResponseWriter, r *http.Request, path string) bool {
	ctx := r.Context()

	if path == "/select/buildinfo" {
		httpserver.EnableCORS(w, r)

		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Fprintf(w, `{"status":"error","msg":"method %q isn't allowed"}`, r.Method)
			return true
		}

		v := buildinfo.ShortVersion()
		if v == "" {
			// buildinfo.ShortVersion() may return empty result for builds without tags
			v = buildinfo.Version
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"success","data":{"version":%q}}`, v)
		return true
	}

	if path == "/select/vmui" {
		// VMUI access via incomplete url without `/` in the end. Redirect to complete url.
		// Use relative redirect, since the hostname and path prefix may be incorrect if VictoriaMetrics
		// is hidden behind vmauth or similar proxy.
		_ = r.ParseForm()
		newURL := "vmui/?" + r.Form.Encode()
		httpserver.Redirect(w, newURL)
		return true
	}
	if strings.HasPrefix(path, "/select/vmui/") {
		if strings.HasPrefix(path, "/select/vmui/static/") {
			// Allow clients caching static contents for long period of time, since it shouldn't change over time.
			// Path to static contents (such as js and css) must be changed whenever its contents is changed.
			// See https://developer.chrome.com/docs/lighthouse/performance/uses-long-cache-ttl/
			w.Header().Set("Cache-Control", "max-age=31536000")
		}
		r.URL.Path = strings.TrimPrefix(path, "/select")
		vmuiFileServer.ServeHTTP(w, r)
		return true
	}

	if path == "/select/logsql/tail" {
		logsqlTailRequests.Inc()
		// Process live tailing request without timeout, since it is OK to run live tailing requests for very long time.
		// Also do not apply concurrency limit to tail requests, since these limits are intended for non-tail requests.
		logsql.ProcessLiveTailRequest(ctx, w, r)
		return true
	}

	// Limit the number of concurrent queries, which can consume big amounts of CPU time.
	startTime := time.Now()
	d := getMaxQueryDuration(r)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	if !incRequestConcurrency(ctxWithTimeout, w, r) {
		return true
	}
	defer decRequestConcurrency()

	ok := processSelectRequest(ctxWithTimeout, w, r, path)
	if !ok {
		return false
	}

	// Log slow queries
	if *logSlowQueryDuration > 0 {
		d := time.Since(startTime)
		if d >= *logSlowQueryDuration {
			remoteAddr := httpserver.GetQuotedRemoteAddr(r)
			requestURI := httpserver.GetRequestURI(r)
			logger.Warnf("slow query according to -search.logSlowQueryDuration=%s: remoteAddr=%s, duration=%.3f seconds; requestURI: %q",
				*logSlowQueryDuration, remoteAddr, d.Seconds(), requestURI)
			slowQueries.Inc()
		}
	}

	logRequestErrorIfNeeded(ctxWithTimeout, w, r, startTime)
	return true
}

func logRequestErrorIfNeeded(ctx context.Context, w http.ResponseWriter, r *http.Request, startTime time.Time) {
	err := ctx.Err()
	switch err {
	case nil:
		// nothing to do
	case context.Canceled:
		// do not log canceled requests, since they are expected and legal.
	case context.DeadlineExceeded:
		err = &httpserver.ErrorWithStatusCode{
			Err: fmt.Errorf("the request couldn't be executed in %.3f seconds; possible solutions: "+
				"to increase -search.maxQueryDuration=%s; to pass bigger value to 'timeout' query arg", time.Since(startTime).Seconds(), maxQueryDuration),
			StatusCode: http.StatusServiceUnavailable,
		}
		httpserver.Errorf(w, r, "%s", err)
	default:
		httpserver.Errorf(w, r, "unexpected error: %s", err)
	}
}

func incRequestConcurrency(ctx context.Context, w http.ResponseWriter, r *http.Request) bool {
	startTime := time.Now()
	stopCh := ctx.Done()
	select {
	case concurrencyLimitCh <- struct{}{}:
		return true
	default:
		// Sleep for a while until giving up. This should resolve short bursts in requests.
		concurrencyLimitReached.Inc()
		select {
		case concurrencyLimitCh <- struct{}{}:
			return true
		case <-stopCh:
			switch ctx.Err() {
			case context.Canceled:
				remoteAddr := httpserver.GetQuotedRemoteAddr(r)
				requestURI := httpserver.GetRequestURI(r)
				logger.Infof("client has canceled the pending request after %.3f seconds: remoteAddr=%s, requestURI: %q",
					time.Since(startTime).Seconds(), remoteAddr, requestURI)
			case context.DeadlineExceeded:
				concurrencyLimitTimeout.Inc()
				err := &httpserver.ErrorWithStatusCode{
					Err: fmt.Errorf("couldn't start executing the request in %.3f seconds, since -search.maxConcurrentRequests=%d concurrent requests "+
						"are executed. Possible solutions: to reduce query load; to add more compute resources to the server; "+
						"to increase -search.maxQueueDuration=%s; to increase -search.maxQueryDuration=%s; to increase -search.maxConcurrentRequests; "+
						"to pass bigger value to 'timeout' query arg",
						time.Since(startTime).Seconds(), *maxConcurrentRequests, maxQueueDuration, maxQueryDuration),
					StatusCode: http.StatusServiceUnavailable,
				}
				httpserver.Errorf(w, r, "%s", err)
			}
			return false
		}
	}
}

func decRequestConcurrency() {
	<-concurrencyLimitCh
}

func processSelectRequest(ctx context.Context, w http.ResponseWriter, r *http.Request, path string) bool {
	httpserver.EnableCORS(w, r)
	startTime := time.Now()
	switch path {
	case "/select/logsql/query_time_range":
		logsqlQueryTimeRangeRequests.Inc()
		logsql.ProcessQueryTimeRangeRequest(ctx, w, r)
		return true
	case "/select/logsql/facets":
		logsqlFacetsRequests.Inc()
		logsql.ProcessFacetsRequest(ctx, w, r)
		logsqlFacetsDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/field_names":
		logsqlFieldNamesRequests.Inc()
		logsql.ProcessFieldNamesRequest(ctx, w, r)
		logsqlFieldNamesDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/field_values":
		logsqlFieldValuesRequests.Inc()
		logsql.ProcessFieldValuesRequest(ctx, w, r)
		logsqlFieldValuesDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/hits":
		logsqlHitsRequests.Inc()
		logsql.ProcessHitsRequest(ctx, w, r)
		logsqlHitsDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/query":
		logsqlQueryRequests.Inc()
		logsql.ProcessQueryRequest(ctx, w, r)
		logsqlQueryDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stats_query":
		logsqlStatsQueryRequests.Inc()
		logsql.ProcessStatsQueryRequest(ctx, w, r)
		logsqlStatsQueryDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stats_query_range":
		logsqlStatsQueryRangeRequests.Inc()
		logsql.ProcessStatsQueryRangeRequest(ctx, w, r)
		logsqlStatsQueryRangeDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stream_field_names":
		logsqlStreamFieldNamesRequests.Inc()
		logsql.ProcessStreamFieldNamesRequest(ctx, w, r)
		logsqlStreamFieldNamesDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stream_field_values":
		logsqlStreamFieldValuesRequests.Inc()
		logsql.ProcessStreamFieldValuesRequest(ctx, w, r)
		logsqlStreamFieldValuesDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stream_ids":
		logsqlStreamIDsRequests.Inc()
		logsql.ProcessStreamIDsRequest(ctx, w, r)
		logsqlStreamIDsDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/streams":
		logsqlStreamsRequests.Inc()
		logsql.ProcessStreamsRequest(ctx, w, r)
		logsqlStreamsDuration.UpdateDuration(startTime)
		return true
	case "/select/tenant_ids":
		tenantIDsRequests.Inc()
		logsql.ProcessTenantIDsRequest(ctx, w, r)
		tenantIDsDuration.UpdateDuration(startTime)
		return true
	default:
		return false
	}
}

func deleteHandler(w http.ResponseWriter, r *http.Request, path string) {
	ctx := r.Context()

	switch path {
	case "/delete/run_task":
		deleteRunTaskRequests.Inc()
		processDeleteRunTaskRequest(ctx, w, r)
	case "/delete/stop_task":
		deleteStopTaskRequests.Inc()
		processDeleteStopTaskRequest(ctx, w, r)
	case "/delete/active_tasks":
		deleteActiveTasksRequests.Inc()
		processDeleteActiveTasksRequest(ctx, w, r)
	default:
		httpserver.Errorf(w, r, "unsupported path requested: %q", path)
	}
}

func processDeleteRunTaskRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	tenantID, err := logstorage.GetTenantIDFromRequest(r)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain tenantID: %s", err)
		return
	}

	fStr := r.FormValue("filter")
	f, err := logstorage.ParseFilter(fStr)
	if err != nil {
		httpserver.Errorf(w, r, "cannot parse filter [%s]: %s", fStr, err)
		return
	}

	// Generate taskID from the current timestamp in nanoseconds
	timestamp := time.Now().UnixNano()
	taskID := fmt.Sprintf("%d", timestamp)

	tenantIDs := []logstorage.TenantID{tenantID}
	if err := vlstorage.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, f); err != nil {
		httpserver.Errorf(w, r, "cannot run delete task: %s", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"task_id":%q}`, taskID)
}

func processDeleteStopTaskRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	taskID := r.FormValue("task_id")
	if taskID == "" {
		httpserver.Errorf(w, r, "missing task_id arg")
		return
	}

	if err := vlstorage.DeleteStopTask(ctx, taskID); err != nil {
		httpserver.Errorf(w, r, "cannot stop task with task_id=%q: %s", taskID, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok"}`)
}

func processDeleteActiveTasksRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	tasks, err := vlstorage.DeleteActiveTasks(ctx)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain active delete tasks: %s", err)
		return
	}

	data := logstorage.MarshalDeleteTasksToJSON(tasks)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", data)
}

// getMaxQueryDuration returns the maximum duration for query from r.
func getMaxQueryDuration(r *http.Request) time.Duration {
	dms, err := httputil.GetDuration(r, "timeout", 0)
	if err != nil {
		dms = 0
	}
	d := time.Duration(dms) * time.Millisecond
	if d <= 0 || d > *maxQueryDuration {
		d = *maxQueryDuration
	}
	return d
}

var (
	logsqlFacetsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/facets"}`)
	logsqlFacetsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/facets"}`)

	logsqlFieldNamesRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/field_names"}`)
	logsqlFieldNamesDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/field_names"}`)

	logsqlFieldValuesRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/field_values"}`)
	logsqlFieldValuesDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/field_values"}`)

	logsqlHitsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/hits"}`)
	logsqlHitsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/hits"}`)

	logsqlQueryRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/query"}`)
	logsqlQueryDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/query"}`)

	logsqlStatsQueryRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stats_query"}`)
	logsqlStatsQueryDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stats_query"}`)

	logsqlStatsQueryRangeRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stats_query_range"}`)
	logsqlStatsQueryRangeDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stats_query_range"}`)

	logsqlStreamFieldNamesRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stream_field_names"}`)
	logsqlStreamFieldNamesDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stream_field_names"}`)

	logsqlStreamFieldValuesRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stream_field_values"}`)
	logsqlStreamFieldValuesDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stream_field_values"}`)

	logsqlStreamIDsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stream_ids"}`)
	logsqlStreamIDsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stream_ids"}`)

	logsqlStreamsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/streams"}`)
	logsqlStreamsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/streams"}`)

	tenantIDsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/tenant_ids"}`)
	tenantIDsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/tenant_ids"}`)

	// no need to track duration for tail requests, as they usually take long time
	logsqlTailRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/tail"}`)

	// no need to track the duration for query_time_range requests, since they are instant
	logsqlQueryTimeRangeRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/query_time_range"}`)

	// no need to track duration for /delete/* requests, because they are asynchornous
	deleteRunTaskRequests     = metrics.NewCounter(`vl_http_requests_total{path="/delete/run_task"}`)
	deleteStopTaskRequests    = metrics.NewCounter(`vl_http_requests_total{path="/delete/stop_task"}`)
	deleteActiveTasksRequests = metrics.NewCounter(`vl_http_requests_total{path="/delete/active_tasks"}`)

	slowQueries = metrics.NewCounter(`vl_slow_queries_total`)
)
