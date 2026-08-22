package vlinsert

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/datadog"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/elasticsearch"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/internalinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/journald"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/jsonline"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/loki"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/nativeinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/nativeinsert/nativemultitenant"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/opentelemetry"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/splunk"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/syslog"
)

var (
	disableInsert         = flag.Bool("insert.disable", false, "Whether to disable both /insert/* and /internal/insert HTTP endpoints. Useful for dedicated vlselect nodes. See also -internalinsert.disable. See https://docs.victoriametrics.com/victorialogs/cluster/#security")
	disableInternalInsert = flag.Bool("internalinsert.disable", false, "Whether to disable /internal/insert HTTP endpoint. See also -insert.disable. See https://docs.victoriametrics.com/victorialogs/cluster/#security")
)

// Init initializes vlinsert
func Init() {
	insertutil.InitRateLimiters()
	syslog.MustInit()
	journald.MustInit()
	splunk.MustInit()
}

// Stop stops vlinsert
func Stop() {
	syslog.MustStop()
}

// RequestHandler handles insert requests for VictoriaLogs
func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
	path := strings.ReplaceAll(r.URL.Path, "//", "/")

	if strings.HasPrefix(path, "/insert/") {
		if *disableInsert {
			httpserver.Errorf(w, r, "requests to /insert/* are disabled with -insert.disable command-line flag")
			return true
		}
		if path != "/insert/ready" && rejectOnIngestRateLimit(w, r) {
			return true
		}

		return insertHandler(w, r, path)
	}

	if path == "/internal/insert" {
		if *disableInternalInsert || *disableInsert {
			httpserver.Errorf(w, r, "requests to /internal/insert are disabled with -internalinsert.disable or -insert.disable command-line flag")
			return true
		}
		if rejectOnIngestRateLimit(w, r) {
			return true
		}
		internalinsert.RequestHandler(w, r)
		return true
	}

	switch {
	case strings.HasPrefix(path, "/api/v2/logs") || strings.HasPrefix(path, "/api/v1/validate"):
		if *disableInsert {
			httpserver.Errorf(w, r, "requests to /api/v2/logs and /api/v1/validate are disabled with -insert.disable command-line flag")
			return true
		}
		if rejectOnIngestRateLimit(w, r) {
			return true
		}
		return datadog.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/services/collector/"):
		if *disableInsert {
			httpserver.Errorf(w, r, "requests to /services/collector/* are disabled with -insert.disable command-line flag")
			return true
		}
		if rejectOnIngestRateLimit(w, r) {
			return true
		}
		return splunk.RequestHandler(path, w, r)
	}

	return false
}

// rejectOnIngestRateLimit responds with HTTP 429 and returns true
// if the limits set via -insert.maxLogsPerSecond or -insert.maxBytesPerSecond are exceeded.
//
// Data ingestion protocols, which do not run on top of HTTP, are throttled instead of being rejected.
// See insertutil.RegisterIngestedData.
func rejectOnIngestRateLimit(w http.ResponseWriter, r *http.Request) bool {
	if !insertutil.IsIngestRateLimitExceeded() {
		return false
	}

	// Retry-After must be set before writing the response status code.
	w.Header().Set("Retry-After", "1")
	err := &httpserver.ErrorWithStatusCode{
		Err: errors.New("cannot ingest data, since the ingestion rate limit set via -insert.maxLogsPerSecond and/or -insert.maxBytesPerSecond is exceeded; " +
			"retry the request later; see https://docs.victoriametrics.com/victorialogs/data-ingestion/#rate-limiting"),
		StatusCode: http.StatusTooManyRequests,
	}
	httpserver.Errorf(w, r, "%s", err)
	return true
}

func insertHandler(w http.ResponseWriter, r *http.Request, path string) bool {
	switch path {
	case "/insert/jsonline":
		jsonline.RequestHandler(w, r)
		return true
	case "/insert/native":
		nativeinsert.RequestHandler(w, r)
		return true
	case "/insert/multitenant/native":
		nativemultitenant.RequestHandler(w, r)
		return true
	case "/insert/ready":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		fmt.Fprintf(w, `{"status":"ok"}`)
		return true
	}
	switch {
	// some clients may omit trailing slash at elasticsearch protocol.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8353
	case strings.HasPrefix(path, "/insert/elasticsearch"):
		return elasticsearch.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/insert/splunk/"):
		return splunk.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/insert/loki/"):
		return loki.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/insert/opentelemetry/"):
		return opentelemetry.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/insert/journald/"):
		return journald.RequestHandler(path, w, r)
	case strings.HasPrefix(path, "/insert/datadog/"):
		return datadog.RequestHandler(path, w, r)
	}

	return false
}
