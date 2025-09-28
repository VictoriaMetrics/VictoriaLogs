package opentelemetry

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/easyproto"
	"github.com/valyala/fastjson"
)

type handler func(timestamp int64, resource, attributes []logstorage.Field)

// decodeRequest parses a LogsData protobuf message from src
// and calls the provided handler for each decoded log record.
//
// See the definition of LogsData here:
// https://github.com/open-telemetry/opentelemetry-proto/blob/34d29fe5ad4689b5db0259d3750de2bfa195bc85/opentelemetry/proto/logs/v1/logs.proto#L38
func decodeRequest(src []byte, handle handler) (err error) {
	// message LogsData {
	//   repeated ResourceLogs resource_logs = 1;
	// }

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in ExportLogsServiceRequest: %s", err)
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read ResourceLogs data")
			}

			if err := decodeResourceLogs(data, handle); err != nil {
				return fmt.Errorf("cannot decode ResourceLogs: %s", err)
			}
		}
	}
	return nil
}

func decodeResourceLogs(src []byte, handle handler) (err error) {
	// message ResourceLogs {
	//   Resource resource = 1;
	//   repeated ScopeLogs scope_logs = 2;
	// }

	fb := getFmtBuffer()
	defer putFmtBuffer(fb)
	fs := getFieldsSlice()
	defer putFieldsSlice(fs)

	// Decode resource
	data, ok, err := findMessageData(src, 1)
	if err != nil {
		return fmt.Errorf("cannot find Resource in ResourceLogs: %s", err)
	}
	if ok {
		fs.s, err = decodeResource(fs.s, data, fb)
		if err != nil {
			return fmt.Errorf("cannot decode Resource: %s", err)
		}
	}
	resource := fs.s

	// Decode scope_logs
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next ScopeLogs in ResourceLogs: %s", err)
		}
		switch fc.FieldNum {
		case 2:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read ScopeLogs data")
			}

			if err := decodeScopeLogs(data, resource, handle); err != nil {
				return fmt.Errorf("cannot decode ScopeLogs: %s", err)
			}
		}
	}

	return nil
}

func decodeResource(dst []logstorage.Field, src []byte, fb *fmtBuffer) (_ []logstorage.Field, err error) {
	// message Resource {
	//   repeated KeyValue attributes = 1;
	// }

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return dst, fmt.Errorf("cannot read next field in Resource")
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return dst, fmt.Errorf("cannot read Attributes data")
			}
			dst, err = decodeKeyValue(fb, dst, "", data)
			if err != nil {
				return dst, fmt.Errorf("cannot decode Attributes: %s", err)
			}
		}
	}
	return dst, nil
}

func decodeScopeLogs(src []byte, resource []logstorage.Field, handle handler) (err error) {
	// message ScopeLogs {
	//   repeated LogRecord log_records = 2;
	// }

	fb := getFmtBuffer()
	defer putFmtBuffer(fb)
	fs := getFieldsSlice()
	defer putFieldsSlice(fs)

	// Resource fields must be present in every log entry.
	fs.s = append(fs.s, resource...)

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in ScopeLogs: %w", err)
		}
		switch fc.FieldNum {
		case 2:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read LogRecord data")
			}

			fb.reset()
			fs.s = fs.s[:len(resource)]

			var timestamp int64
			fs.s, timestamp, err = decodeLogRecord(fs.s, data, fb)
			if err != nil {
				return fmt.Errorf("cannot decode LogRecord: %w", err)
			}

			attrs := fs.s
			handle(timestamp, resource, attrs)
		}
	}
	return nil
}

func decodeLogRecord(dst []logstorage.Field, src []byte, fb *fmtBuffer) ([]logstorage.Field, int64, error) {
	// message LogRecord {
	//   fixed64 time_unix_nano = 1;
	//   fixed64 observed_time_unix_nano = 11;
	//   SeverityNumber severity_number = 2;
	//   string severity_text = 3;
	//   AnyValue body = 5;
	//   repeated KeyValue attributes = 6;
	//   bytes trace_id = 9;
	//   bytes span_id = 10;
	// }

	var (
		timeUnixNano         uint64
		observedTimeUnixNano uint64
		severityText         string
		severityNumber       int32
	)

	var fc easyproto.FieldContext
	for len(src) > 0 {
		var err error
		src, err = fc.NextField(src)
		if err != nil {
			return nil, 0, fmt.Errorf("cannot read next field in LogRecord: %w", err)
		}
		var ok bool
		switch fc.FieldNum {
		case 1:
			timeUnixNano, ok = fc.Fixed64()
			if !ok {
				return nil, 0, fmt.Errorf("cannot read log record timestamp")
			}
		case 11:
			observedTimeUnixNano, ok = fc.Fixed64()
			if !ok {
				return nil, 0, fmt.Errorf("cannot read log record observed timestamp")
			}
		case 2:
			severityNumber, ok = fc.Int32()
			if !ok {
				return nil, 0, fmt.Errorf("cannot read severity number")
			}
		case 3:
			severityText, ok = fc.String()
			if !ok {
				return nil, 0, fmt.Errorf("cannot read severity string")
			}
		case 5:
			body, ok := fc.MessageData()
			if !ok {
				return nil, 0, fmt.Errorf("cannot read Body")
			}
			dst, err = decodeAnyValue(dst, body, "", fb)
			if err != nil {
				return nil, 0, fmt.Errorf("cannot decode Body: %w", err)
			}
		case 6:
			data, ok := fc.MessageData()
			if !ok {
				return nil, 0, fmt.Errorf("cannot read attributes data")
			}
			dst, err = decodeKeyValue(fb, dst, "", data)
			if err != nil {
				return nil, 0, fmt.Errorf("cannot decode attributes: %w", err)
			}
		case 9:
			traceID, ok := fc.Bytes()
			if !ok {
				return nil, 0, fmt.Errorf("cannot read trace id")
			}
			dst = append(dst, logstorage.Field{
				Name:  "trace_id",
				Value: fb.formatHex(traceID),
			})
		case 10:
			spanID, ok := fc.Bytes()
			if !ok {
				return nil, 0, fmt.Errorf("cannot read span id")
			}
			dst = append(dst, logstorage.Field{
				Name:  "span_id",
				Value: fb.formatHex(spanID),
			})
		}
	}

	if severityText == "" {
		severityText = formatSeverity(severityNumber)
	}
	dst = append(dst, logstorage.Field{
		Name:  "severity",
		Value: severityText,
	})

	var timestamp int64
	switch {
	case timeUnixNano > 0:
		timestamp = int64(timeUnixNano)
	case observedTimeUnixNano > 0:
		timestamp = int64(observedTimeUnixNano)
	default:
		timestamp = time.Now().UnixNano()
	}

	return dst, timestamp, nil
}

// https://github.com/open-telemetry/opentelemetry-collector/blob/cd1f7623fe67240e32e74735488c3db111fad47b/pdata/plog/severity_number.go#L41
var logSeverities = []string{
	"Unspecified",
	"Trace",
	"Trace2",
	"Trace3",
	"Trace4",
	"Debug",
	"Debug2",
	"Debug3",
	"Debug4",
	"Info",
	"Info2",
	"Info3",
	"Info4",
	"Warn",
	"Warn2",
	"Warn3",
	"Warn4",
	"Error",
	"Error2",
	"Error3",
	"Error4",
	"Fatal",
	"Fatal2",
	"Fatal3",
	"Fatal4",
}

func formatSeverity(severity int32) string {
	if severity < 0 || severity >= int32(len(logSeverities)) {
		return logSeverities[0]
	}
	return logSeverities[severity]
}

func decodeKeyValue(fb *fmtBuffer, dst []logstorage.Field, fieldName string, src []byte) (_ []logstorage.Field, err error) {
	// message KeyValue {
	//   string key = 1;
	//   AnyValue value = 2;
	// }

	// Decode key
	data, ok, err := findMessageData(src, 1)
	if err != nil {
		return dst, fmt.Errorf("cannot find Key in KeyValue: %s", err)
	}
	if !ok {
		return dst, fmt.Errorf("key is missing in KeyValue")
	}
	fieldName = fb.formatSubFieldName(fieldName, data)

	// Decode value
	data, ok, err = findMessageData(src, 2)
	if err != nil {
		return dst, fmt.Errorf("cannot find Value in KeyValue: %s", err)
	}
	if !ok {
		// Value is null, skip it.
		return dst, nil
	}

	dst, err = decodeAnyValue(dst, data, fieldName, fb)
	if err != nil {
		return dst, fmt.Errorf("cannot decode AnyValue: %s", err)
	}
	return dst, nil
}

func decodeAnyValue(dst []logstorage.Field, src []byte, fieldName string, fb *fmtBuffer) (_ []logstorage.Field, err error) {
	// message AnyValue {
	//   oneof value {
	//     string string_value = 1;
	//     bool bool_value = 2;
	//     int64 int_value = 3;
	//     double double_value = 4;
	//     ArrayValue array_value = 5;
	//     KeyValueList kvlist_value = 6;
	//     bytes bytes_value = 7;
	//   }
	// }

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return dst, fmt.Errorf("cannot read next field in AnyValue")
		}
		switch fc.FieldNum {
		case 1:
			stringValue, ok := fc.String()
			if !ok {
				return dst, fmt.Errorf("cannot read StringValue")
			}
			dst = append(dst, logstorage.Field{
				Name:  fieldName,
				Value: stringValue,
			})
		case 2:
			boolValue, ok := fc.Bool()
			if !ok {
				return dst, fmt.Errorf("cannot read BoolValue")
			}
			dst = append(dst, logstorage.Field{
				Name:  fieldName,
				Value: strconv.FormatBool(boolValue),
			})
		case 3:
			intValue, ok := fc.Int64()
			if !ok {
				return dst, fmt.Errorf("cannot read IntValue")
			}
			dst = append(dst, logstorage.Field{
				Name:  fieldName,
				Value: fb.formatInt(intValue),
			})
		case 4:
			doubleValue, ok := fc.Double()
			if !ok {
				return dst, fmt.Errorf("cannot read DoubleValue")
			}
			dst = append(dst, logstorage.Field{
				Name:  fieldName,
				Value: fb.formatFloat(doubleValue),
			})
		case 5:
			data, ok := fc.MessageData()
			if !ok {
				return dst, fmt.Errorf("cannot read ArrayValue")
			}

			arena := jsonArenaPool.Get()
			// Encode arrays as JSON to match the behavior of /insert/jsonline
			arr, err := decodeArrayValueToJSON(arena, data)
			if err != nil {
				return dst, fmt.Errorf("cannot decode ArrayValue: %s", err)
			}
			encodedArr := fb.encodeJSONValue(arr)
			jsonArenaPool.Put(arena)

			dst = append(dst, logstorage.Field{
				Name:  fieldName,
				Value: encodedArr,
			})
		case 6:
			data, ok := fc.MessageData()
			if !ok {
				return dst, fmt.Errorf("cannot read KeyValueList")
			}
			dst, err = decodeKeyValueList(fb, dst, fieldName, data)
			if err != nil {
				return dst, fmt.Errorf("cannot decode KeyValueList: %s", err)
			}
		case 7:
			bytesValue, ok := fc.Bytes()
			if !ok {
				return dst, fmt.Errorf("cannot read BytesValue")
			}
			v := fb.formatBase64(bytesValue)
			dst = append(dst, logstorage.Field{
				Name:  fieldName,
				Value: v,
			})
		default:
			unsupportedTypeLogger.Warnf("unsupported AnyValue type %d, please create an issue: https://github.com/VictoriaMetrics/VictoriaLogs/issues", fc.FieldNum)
		}
	}
	return dst, nil
}

var unsupportedTypeLogger = logger.WithThrottler("otel_unsupported_type", 10*time.Second)

func decodeKeyValueList(fb *fmtBuffer, fields []logstorage.Field, fieldName string, src []byte) (_ []logstorage.Field, err error) {
	// message KeyValueList {
	//   repeated KeyValue values = 1;
	// }

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fields, fmt.Errorf("cannot read next field in KeyValueList")
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return fields, fmt.Errorf("cannot read Value data")
			}
			fields, err = decodeKeyValue(fb, fields, fieldName, data)
			if err != nil {
				return fields, fmt.Errorf("cannot decode KeyValue: %s", err)
			}
		}
	}
	return fields, nil
}

func findMessageData(src []byte, fieldNum uint32) (data []byte, ok bool, err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return nil, false, fmt.Errorf("cannot read next field: %s", err)
		}

		if fc.FieldNum != fieldNum {
			continue
		}

		data, ok = fc.MessageData()
		if !ok {
			return nil, false, fmt.Errorf("cannot read field data")
		}
		return data, true, nil
	}
	return nil, false, nil
}

type fieldSlice struct {
	s []logstorage.Field
}

var fieldsSlicePool = sync.Pool{
	New: func() any {
		return &fieldSlice{}
	},
}

func getFieldsSlice() *fieldSlice {
	c := fieldsSlicePool.Get().(*fieldSlice)
	return c
}

func putFieldsSlice(c *fieldSlice) {
	c.reset()
	fieldsSlicePool.Put(c)
}

func (c *fieldSlice) reset() {
	clear(c.s)
	c.s = c.s[:0]
}

type fmtBuffer struct {
	buf []byte
}

var fmtBufferPool = sync.Pool{
	New: func() any {
		return &fmtBuffer{}
	},
}

func getFmtBuffer() *fmtBuffer {
	fb := fmtBufferPool.Get().(*fmtBuffer)
	return fb
}

func putFmtBuffer(fb *fmtBuffer) {
	fb.reset()
	fmtBufferPool.Put(fb)
}

func (fb *fmtBuffer) reset() {
	fb.buf = fb.buf[:0]
}

func (fb *fmtBuffer) formatInt(v int64) string {
	n := len(fb.buf)
	fb.buf = strconv.AppendInt(fb.buf, v, 10)
	return bytesutil.ToUnsafeString(fb.buf[n:])
}

func (fb *fmtBuffer) formatFloat(v float64) string {
	n := len(fb.buf)
	fb.buf = strconv.AppendFloat(fb.buf, v, 'f', -1, 64)
	return bytesutil.ToUnsafeString(fb.buf[n:])
}

func (fb *fmtBuffer) formatSubFieldName(prefix string, suffix []byte) string {
	if prefix == "" {
		// There is no prefix, so just return the suffix as is.
		return bytesutil.ToUnsafeString(suffix)
	}

	n := len(fb.buf)
	fb.buf = append(fb.buf, prefix...)
	fb.buf = append(fb.buf, '.')
	fb.buf = append(fb.buf, suffix...)

	fieldName := bytesutil.ToUnsafeString(fb.buf[n:])
	return fieldName
}

func (fb *fmtBuffer) formatHex(src []byte) string {
	n := len(fb.buf)
	size := hex.EncodedLen(len(src))

	fb.buf = bytesutil.ResizeNoCopyMayOverallocate(fb.buf, n+size)
	hex.Encode(fb.buf[n:], src)

	v := bytesutil.ToUnsafeString(fb.buf[n:])
	return v
}

func (fb *fmtBuffer) formatBase64(src []byte) string {
	n := len(fb.buf)
	size := base64.StdEncoding.EncodedLen(len(src))

	fb.buf = bytesutil.ResizeNoCopyMayOverallocate(fb.buf, n+size)
	base64.StdEncoding.Encode(fb.buf[n:], src)

	v := bytesutil.ToUnsafeString(fb.buf[n:])
	return v
}

func (fb *fmtBuffer) encodeJSONValue(value *fastjson.Value) string {
	n := len(fb.buf)
	fb.buf = value.MarshalTo(fb.buf)
	v := bytesutil.ToUnsafeString(fb.buf[n:])
	return v
}
