package opentelemetry

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/opentelemetry/pb"
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
		if r.Header.Get("Content-Type") == "application/json" {
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
		lmp := cp.NewLogMessageProcessor("opentelelemtry_protobuf", false)
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

var (
	exportLogsReqPool = sync.Pool{
		New: func() any {
			return &pb.ExportLogsServiceRequest{}
		},
	}
	fieldsPool = sync.Pool{
		New: func() any {
			s := make([]logstorage.Field, 0)
			return &s
		},
	}
)

func pushProtobufRequest(data []byte, lmp insertutil.LogMessageProcessor, msgFields []string, useDefaultStreamFields bool) error {
	req := getExportLogsReq()
	defer putExportLogsReq(req)

	if err := req.UnmarshalProtobuf(data); err != nil {
		errorsTotal.Inc()
		return fmt.Errorf("cannot unmarshal request from %d bytes: %w", len(data), err)
	}

	commonFields := getFields()
	defer putFields(commonFields)

	for _, rl := range req.ResourceLogs {
		*commonFields = (*commonFields)[:0]
		*commonFields = appendKeyValues(*commonFields, rl.Resource.Attributes, "")
		commonFieldsLen := len(*commonFields)
		for _, sc := range rl.ScopeLogs {
			*commonFields = pushFieldsFromScopeLogs(&sc, (*commonFields)[:commonFieldsLen], lmp, msgFields, useDefaultStreamFields)
		}
	}

	return nil
}

func pushFieldsFromScopeLogs(sc *pb.ScopeLogs, commonFields []logstorage.Field, lmp insertutil.LogMessageProcessor, msgFields []string, useDefaultStreamFields bool) []logstorage.Field {
	fields := commonFields
	for _, lr := range sc.LogRecords {
		fields = fields[:len(commonFields)]
		if lr.Body.KeyValueList != nil {
			fields = appendKeyValues(fields, lr.Body.KeyValueList.Values, "")
			logstorage.RenameField(fields[len(commonFields):], msgFields, "_msg")
		} else {
			fields = append(fields, logstorage.Field{
				Name:  "_msg",
				Value: lr.Body.FormatString(true),
			})
		}
		fields = appendKeyValues(fields, lr.Attributes, "")
		if len(lr.TraceID) > 0 {
			fields = append(fields, logstorage.Field{
				Name:  "trace_id",
				Value: lr.TraceID,
			})
		}
		if len(lr.SpanID) > 0 {
			fields = append(fields, logstorage.Field{
				Name:  "span_id",
				Value: lr.SpanID,
			})
		}
		fields = append(fields, logstorage.Field{
			Name:  "severity",
			Value: lr.FormatSeverity(),
		})

		var streamFields []logstorage.Field
		if useDefaultStreamFields {
			streamFields = commonFields
		}
		lmp.AddRow(lr.ExtractTimestampNano(), fields, streamFields)
	}
	return fields
}

func appendKeyValues(fields []logstorage.Field, kvs []*pb.KeyValue, parentField string) []logstorage.Field {
	for _, attr := range kvs {
		fieldName := attr.Key
		if parentField != "" {
			fieldName = parentField + "." + fieldName
		}

		if attr.Value.KeyValueList != nil {
			fields = appendKeyValues(fields, attr.Value.KeyValueList.Values, fieldName)
		} else {
			fields = append(fields, logstorage.Field{
				Name:  fieldName,
				Value: attr.Value.FormatString(true),
			})
		}
	}
	return fields
}

func getExportLogsReq() *pb.ExportLogsServiceRequest {
	req := exportLogsReqPool.Get().(*pb.ExportLogsServiceRequest)
	if req == nil {
		return &pb.ExportLogsServiceRequest{}
	}
	return req
}

func putExportLogsReq(req *pb.ExportLogsServiceRequest) {
	req.ResourceLogs = req.ResourceLogs[:0]
	exportLogsReqPool.Put(req)
}

func getFields() *[]logstorage.Field {
	value := fieldsPool.Get()
	if value == nil {
		return &[]logstorage.Field{}
	}
	return value.(*[]logstorage.Field)
}

func putFields(fields *[]logstorage.Field) {
	*fields = (*fields)[:0]
	fieldsPool.Put(fields)
}
