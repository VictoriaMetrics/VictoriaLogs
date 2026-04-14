package opentelemetry

import (
	"fmt"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var maxRequestSize = flagutil.NewBytes("opentelemetry.maxRequestSize", 64*1024*1024, "The maximum size in bytes of a single OpenTelemetry request")

// RequestHandler processes Opentelemetry insert requests
func RequestHandler(path string, w http.ResponseWriter, r *http.Request) bool {
	switch path {
	// use the same path as opentelemetry collector
	// https://opentelemetry.io/docs/specs/otlp/#otlphttp-request
	case "/insert/opentelemetry/v1/logs":
		ct := r.Header.Get("Content-Type")
		if insertutil.IsJSONContentType(ct) {
			httpserver.Errorf(w, r, "json encoding isn't supported for opentelemetry format. Use protobuf encoding")
			return true
		}
		handleProtobuf(r, w)
		return true
	default:
		return false
	}
}

func handleProtobuf(r *http.Request, w http.ResponseWriter) {
	startTime := time.Now()
	requestsProtobufTotal.Inc()

	cp, err := insertutil.GetCommonParams(r)
	if err != nil {
		httpserver.Errorf(w, r, "cannot parse common params from request: %s", err)
		return
	}
	if err := insertutil.CanWriteData(); err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	encoding := r.Header.Get("Content-Encoding")
	err = protoparserutil.ReadUncompressedData(r.Body, encoding, maxRequestSize, func(data []byte) error {
		lmp := cp.NewLogMessageProcessor("opentelemetry_protobuf", false)
		useDefaultStreamFields := len(cp.StreamFields) == 0
		err := pushProtobufRequest(data, lmp, cp.MsgFields, useDefaultStreamFields)
		lmp.MustClose()
		return err
	})
	if err != nil {
		httpserver.Errorf(w, r, "cannot read OpenTelemetry protocol data: %s", err)
		return
	}

	// update requestProtobufDuration only for successfully parsed requests
	// There is no need in updating requestProtobufDuration for request errors,
	// since their timings are usually much smaller than the timing for successful request parsing.
	requestProtobufDuration.UpdateDuration(startTime)
}

var (
	requestsProtobufTotal = metrics.NewCounter(`vl_http_requests_total{path="/insert/opentelemetry/v1/logs",format="protobuf"}`)
	errorsTotal           = metrics.NewCounter(`vl_http_errors_total{path="/insert/opentelemetry/v1/logs",format="protobuf"}`)

	requestProtobufDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/insert/opentelemetry/v1/logs",format="protobuf"}`)
)

func pushProtobufRequest(data []byte, lmp insertutil.LogMessageProcessor, msgFields []string, useDefaultStreamFields bool) error {
	pushLogs := func(timestamp int64, fields []logstorage.Field, streamFieldsLen int) {
		logstorage.RenameField(fields[streamFieldsLen:], msgFields, "_msg")

		if !useDefaultStreamFields {
			streamFieldsLen = -1
		}

		lmp.AddRow(timestamp, fields, streamFieldsLen)
	}

	if err := decodeLogsData(data, pushLogs); err != nil {
		errorsTotal.Inc()
		return fmt.Errorf("cannot decode LogsData request from %d bytes: %w", len(data), err)
	}
	return nil
}
