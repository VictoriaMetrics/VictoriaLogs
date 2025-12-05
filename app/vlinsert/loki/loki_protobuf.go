package loki

import (
	"fmt"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func handleProtobuf(r *http.Request, w http.ResponseWriter) {
	startTime := time.Now()
	requestsProtobufTotal.Inc()

	cp, err := getCommonParams(r)
	if err != nil {
		httpserver.Errorf(w, r, "cannot parse common params from request: %s", err)
		return
	}
	if err := insertutil.CanWriteData(); err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	encoding := r.Header.Get("Content-Encoding")
	if encoding == "" {
		// Loki protocol uses snappy compression by default.
		// See https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
		encoding = "snappy"
	}
	err = protoparserutil.ReadUncompressedData(r.Body, encoding, maxRequestSize, func(data []byte) error {
		lmp := cp.cp.NewLogMessageProcessor("loki_protobuf", false)
		useDefaultStreamFields := len(cp.cp.StreamFields) == 0
		err := parseProtobufRequest(data, lmp, cp.cp.MsgFields, useDefaultStreamFields, cp.parseMessage)
		lmp.MustClose()
		return err
	})
	if err != nil {
		httpserver.Errorf(w, r, "cannot read Loki protobuf data: %s", err)
		return
	}

	// update requestProtobufDuration only for successfully parsed requests
	// There is no need in updating requestProtobufDuration for request errors,
	// since their timings are usually much smaller than the timing for successful request parsing.
	requestProtobufDuration.UpdateDuration(startTime)

	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8505
	w.WriteHeader(http.StatusNoContent)
}

var (
	requestsProtobufTotal   = metrics.NewCounter(`vl_http_requests_total{path="/insert/loki/api/v1/push",format="protobuf"}`)
	requestProtobufDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/insert/loki/api/v1/push",format="protobuf"}`)
)

func parseProtobufRequest(data []byte, lmp insertutil.LogMessageProcessor, msgFields []string, useDefaultStreamFields, parseMessage bool) error {
	var msgParser *logstorage.JSONParser
	if parseMessage {
		msgParser = logstorage.GetJSONParser()
		defer logstorage.PutJSONParser(msgParser)
	}

	pushLogs := func(timestamp int64, line string, fs *logstorage.Fields, streamFieldsLen int) {
		if timestamp == 0 {
			timestamp = time.Now().UnixNano()
		}

		allowMsgRenaming := addMsgField(fs, msgParser, line)

		var streamFields []logstorage.Field
		if useDefaultStreamFields {
			streamFields = fs.Fields[:streamFieldsLen]
		}
		if allowMsgRenaming {
			logstorage.RenameField(fs.Fields[streamFieldsLen:], msgFields, "_msg")
		}

		lmp.AddRow(timestamp, fs.Fields, streamFields)
	}

	if err := decodePushRequest(data, pushLogs); err != nil {
		return fmt.Errorf("cannot decode PushRequest: %w", err)
	}

	return nil
}
