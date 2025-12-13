package loki

import (
	"fmt"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fastjson"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var maxRequestSize = flagutil.NewBytes("loki.maxRequestSize", 64*1024*1024, "The maximum size in bytes of a single Loki request")

var parserPool fastjson.ParserPool

func handleJSON(r *http.Request, w http.ResponseWriter) {
	startTime := time.Now()
	requestsJSONTotal.Inc()

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
	err = protoparserutil.ReadUncompressedData(r.Body, encoding, maxRequestSize, func(data []byte) error {
		lmp := cp.cp.NewLogMessageProcessor("loki_json", false)
		useDefaultStreamFields := len(cp.cp.StreamFields) == 0
		err := parseJSONRequest(data, lmp, cp.cp.MsgFields, useDefaultStreamFields, cp.parseMessage)
		lmp.MustClose()
		return err
	})
	if err != nil {
		httpserver.Errorf(w, r, "cannot read Loki json data: %s", err)
		return
	}

	// update requestJSONDuration only for successfully parsed requests
	// There is no need in updating requestJSONDuration for request errors,
	// since their timings are usually much smaller than the timing for successful request parsing.
	requestJSONDuration.UpdateDuration(startTime)

	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8505
	w.WriteHeader(http.StatusNoContent)
}

var (
	requestsJSONTotal   = metrics.NewCounter(`vl_http_requests_total{path="/insert/loki/api/v1/push",format="json"}`)
	requestJSONDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/insert/loki/api/v1/push",format="json"}`)
)

func parseJSONRequest(data []byte, lmp insertutil.LogMessageProcessor, msgFields []string, useDefaultStreamFields, parseMessage bool) error {
	p := parserPool.Get()
	defer parserPool.Put(p)

	v, err := p.ParseBytes(data)
	if err != nil {
		return fmt.Errorf("cannot parse JSON request body: %w", err)
	}

	streamsV := v.Get("streams")
	if streamsV == nil {
		return fmt.Errorf("missing `streams` item in the parsed JSON")
	}
	streams, err := streamsV.Array()
	if err != nil {
		return fmt.Errorf("`streams` item in the parsed JSON must contain an array; got %q", streamsV)
	}

	fieldsTmp := logstorage.GetFields()
	defer func() {
		// Explicitly clear fieldsTmp up to its' capacity in order to free up
		// all the references to the original byte slice, so it could be freed by Go GC.
		fieldsTmp.ClearUpToCapacity()
		logstorage.PutFields(fieldsTmp)
	}()

	var msgParser *logstorage.JSONParser
	if parseMessage {
		msgParser = logstorage.GetJSONParser()
		defer logstorage.PutJSONParser(msgParser)
	}

	for _, stream := range streams {
		// populate common labels from `stream` dict
		labelsV := stream.Get("stream")
		var labels *fastjson.Object
		if labelsV != nil {
			o, err := labelsV.Object()
			if err != nil {
				return fmt.Errorf("`stream` item in the parsed JSON must contain an object; got %q", labelsV)
			}
			labels = o
		}
		fieldsTmp.Reset()
		labels.Visit(func(k []byte, v *fastjson.Value) {
			vStr := getMarshaledJSONValue(v)
			fieldsTmp.Add(bytesutil.ToUnsafeString(k), bytesutil.ToUnsafeString(vStr))
		})

		// populate messages from `values` array
		linesV := stream.Get("values")
		if linesV == nil {
			return fmt.Errorf("missing `values` item in the parsed `stream` object %q", stream)
		}
		lines, err := linesV.Array()
		if err != nil {
			return fmt.Errorf("`values` item in the parsed JSON must contain an array; got %q", linesV)
		}

		commonFieldsLen := len(fieldsTmp.Fields)
		for _, line := range lines {
			fieldsTmp.Fields = fieldsTmp.Fields[:commonFieldsLen]

			lineA, err := line.Array()
			if err != nil {
				return fmt.Errorf("unexpected contents of `values` item; want array; got %q", line)
			}
			if len(lineA) < 2 || len(lineA) > 3 {
				return fmt.Errorf("unexpected number of values in `values` item array %q; got %d want 2 or 3", line, len(lineA))
			}

			// parse timestamp
			timestamp, err := lineA[0].StringBytes()
			if err != nil {
				return fmt.Errorf("unexpected log timestamp type for %q; want string", lineA[0])
			}
			ts, err := parseLokiTimestamp(bytesutil.ToUnsafeString(timestamp))
			if err != nil {
				return fmt.Errorf("cannot parse log timestamp %q: %w", timestamp, err)
			}
			if ts == 0 {
				ts = time.Now().UnixNano()
			}

			// parse structured metadata - see https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
			if len(lineA) > 2 {
				structuredMetadata, err := lineA[2].Object()
				if err != nil {
					return fmt.Errorf("unexpected structured metadata type for %q; want JSON object", lineA[2])
				}
				structuredMetadata.Visit(func(k []byte, v *fastjson.Value) {
					vStr := getMarshaledJSONValue(v)
					fieldsTmp.Add(bytesutil.ToUnsafeString(k), bytesutil.ToUnsafeString(vStr))
				})
			}

			// parse log message
			msg, err := lineA[1].StringBytes()
			if err != nil {
				return fmt.Errorf("unexpected log message type for %q; want string", lineA[1])
			}
			allowMsgRenaming := addMsgField(fieldsTmp, msgParser, bytesutil.ToUnsafeString(msg))
			if allowMsgRenaming {
				logstorage.RenameField(fieldsTmp.Fields[commonFieldsLen:], msgFields, "_msg")
			}

			streamFieldsLen := -1
			if useDefaultStreamFields {
				streamFieldsLen = commonFieldsLen
			}

			lmp.AddRow(ts, fieldsTmp.Fields, streamFieldsLen)
		}
	}

	return nil
}

func addMsgField(fs *logstorage.Fields, msgParser *logstorage.JSONParser, msg string) bool {
	if msgParser == nil || len(msg) < 2 || msg[0] != '{' || msg[len(msg)-1] != '}' {
		fs.Add("_msg", msg)
		return false
	}
	if msgParser != nil && len(msg) >= 2 && msg[0] == '{' && msg[len(msg)-1] == '}' {
		if err := msgParser.ParseLogMessage(bytesutil.ToUnsafeBytes(msg)); err == nil {
			fs.Fields = append(fs.Fields, msgParser.Fields...)
			return true
		}
	}
	fs.Add("_msg", msg)
	return false
}

func parseLokiTimestamp(s string) (int64, error) {
	if s == "" {
		// Special case - an empty timestamp must be substituted with the current time by the caller.
		return 0, nil
	}
	nsecs, ok := timeutil.TryParseUnixTimestamp(s)
	if !ok {
		return 0, fmt.Errorf("cannot parse unix timestamp %q", s)
	}
	return nsecs, nil
}

func getMarshaledJSONValue(v *fastjson.Value) []byte {
	vt := v.Type()

	if vt == fastjson.TypeString {
		sb, err := v.StringBytes()
		if err != nil {
			logger.Panicf("BUG: unexpected error from v.StringBytes(): %s", err)
		}
		return sb
	}

	// Convert any non-string value into string representation like Loki does.
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/547
	sb := v.MarshalTo(nil)
	return sb
}
