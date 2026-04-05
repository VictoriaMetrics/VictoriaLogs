package nativemultitenant

import (
	"fmt"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/nativeinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// RequestHandler processes /insert/multitenant/native requests.
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	version := r.FormValue("version")
	if version != netinsert.ProtocolVersion {
		httpserver.Errorf(w, r, "unsupported protocol version=%q; want %q", version, netinsert.ProtocolVersion)
		return
	}

	requestsTotal.Inc()

	cp, err := insertutil.GetCommonParams(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	if err := insertutil.CanWriteData(); err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	if cp.TenantID.AccountID != 0 || cp.TenantID.ProjectID != 0 {
		unsupportedOptionsLogger.Warnf("/insert/multitenant/native endpoint doesn't support setting tenantID via AccountID and ProjectID request headers; "+
			"ignoring it; tenantID=%q; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy", cp.TenantID)
		cp.TenantID = logstorage.TenantID{}
	}

	if cp.IsTimeFieldSet {
		unsupportedOptionsLogger.Warnf("/insert/multitenant/native endpoint doesn't support setting time fields via _time_field query arg and via VL-Time-Field request header; "+
			"ignoring them; timeFields=%q; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy", cp.TimeFields)
	}
	// Unconditionally reset cp.TimeFields, since the code below shouldn't depend on this field.
	cp.TimeFields = nil

	if len(cp.MsgFields) > 0 {
		unsupportedOptionsLogger.Warnf("/insert/multitenant/native endpoint doesn't support setting msg fields via _msg_field query arg and via VL-Msg-Field request header; "+
			"ignoring them; msgFields=%q; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy", cp.MsgFields)
		cp.MsgFields = nil
	}
	if len(cp.StreamFields) > 0 {
		unsupportedOptionsLogger.Warnf("/insert/multitenant/native endpoint doesn't support setting stream fields via _stream_fields query arg and via VL-Stream-Fields request header; "+
			"ignoring them; streamFields=%q; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy", cp.StreamFields)
		cp.StreamFields = nil
	}
	if len(cp.DecolorizeFields) > 0 {
		unsupportedOptionsLogger.Warnf("/insert/multitenant/native endpoint doesn't support setting decolorize_fields query arg and VL-Decolorize-Fields request header; "+
			"ignoring them; decolorizeFields=%q; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy", cp.DecolorizeFields)
		cp.DecolorizeFields = nil
	}

	encoding := r.Header.Get("Content-Encoding")
	err = protoparserutil.ReadUncompressedData(r.Body, encoding, nativeinsert.MaxRequestSize, func(data []byte) error {
		lmp := cp.NewLogMessageProcessor("nativemultitenant", false)
		irp := lmp.(insertutil.InsertRowProcessor)
		err := parseData(irp, data)
		lmp.MustClose()
		return err
	})
	if err != nil {
		errorsTotal.Inc()
		httpserver.Errorf(w, r, "cannot parse request to /insert/multitenant/native: %s", err)
		return
	}

	requestDuration.UpdateDuration(startTime)
}

var unsupportedOptionsLogger = logger.WithThrottler("unsuppoted_options", 5*time.Second)

func parseData(irp insertutil.InsertRowProcessor, data []byte) error {
	r := logstorage.GetInsertRow()
	defer logstorage.PutInsertRow(r)

	src := data
	i := 0
	for len(src) > 0 {
		tail, err := r.UnmarshalInplace(src)
		if err != nil {
			return fmt.Errorf("cannot parse row #%d: %s", i, err)
		}
		src = tail
		i++

		irp.AddInsertRow(r)
	}

	return nil
}

var (
	requestsTotal = metrics.NewCounter(`vl_http_requests_total{path="/insert/multitenant/native"}`)
	errorsTotal   = metrics.NewCounter(`vl_http_errors_total{path="/insert/multitenant/native"}`)

	requestDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/insert/multitenant/native"}`)
)
