package opentelemetry

import (
	"fmt"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/easyproto"
)

// the pushLogsHandler must store log entry with the given args.
//
// The handler must copy resource and attributes before returning,
// since the caller can change them, so they become invalid if not copied.
type pushLogsHandler func(timestamp int64, fields []logstorage.Field, streamFieldsLen int)

// decodeLogsData parses a LogsData protobuf message from src and calls the provided pushLogs for each decoded log record.
//
// See https://github.com/open-telemetry/opentelemetry-proto/blob/a5f0eac5b802f7ae51dfe41e5116fe5548955e64/opentelemetry/proto/logs/v1/logs.proto#L38
func decodeLogsData(src []byte, pushLogs pushLogsHandler) (err error) {
	// message LogsData {
	//   repeated ResourceLogs resource_logs = 1;
	// }

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read ResourceLogs data")
			}

			if err := decodeResourceLogs(data, pushLogs); err != nil {
				return fmt.Errorf("cannot decode ResourceLogs: %w", err)
			}
		}
	}
	return nil
}

func decodeResourceLogs(src []byte, pushLogs pushLogsHandler) (err error) {
	// message ResourceLogs {
	//   Resource resource = 1;
	//   repeated ScopeLogs scope_logs = 2;
	// }

	fb := getFmtBuffer()
	defer putFmtBuffer(fb)

	fs := logstorage.GetFields()
	defer func() {
		// Explicitly clear fs up to its' capacity in order to free up
		// all the references to the original byte slice, so it could be freed by Go GC.
		fs.ClearUpToCapacity()
		logstorage.PutFields(fs)
	}()

	// Decode resource
	resourceData, ok, err := easyproto.GetMessageData(src, 1)
	if err != nil {
		return fmt.Errorf("cannot find Resource: %w", err)
	}
	if ok {
		if err = decodeResource(resourceData, fs, fb); err != nil {
			return fmt.Errorf("cannot decode Resource: %w", err)
		}
	}

	streamFieldsLen := len(fs.Fields)
	fbLen := len(fb.buf)

	// Decode scope_logs
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 2:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read ScopeLogs data")
			}

			if err := decodeScopeLogs(data, fs, fb, pushLogs); err != nil {
				return fmt.Errorf("cannot decode ScopeLogs: %w", err)
			}

			fs.Fields = fs.Fields[:streamFieldsLen]
			fb.buf = fb.buf[:fbLen]
		}
	}

	return nil
}

func decodeResource(src []byte, fs *logstorage.Fields, fb *fmtBuffer) (err error) {
	// message Resource {
	//   repeated KeyValue attributes = 1;
	// }

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read Attributes data")
			}

			if err := decodeKeyValue(data, fs, fb, ""); err != nil {
				return fmt.Errorf("cannot decode Attributes: %w", err)
			}
		}
	}
	return nil
}

func decodeScopeLogs(src []byte, fs *logstorage.Fields, fb *fmtBuffer, pushLogs pushLogsHandler) (err error) {
	// message ScopeLogs {
	//   InstrumentationScope scope = 1;
	//   repeated LogRecord log_records = 2;
	// }

	streamFieldsLen := len(fs.Fields)

	scopeData, ok, err := easyproto.GetMessageData(src, 1)
	if err != nil {
		return fmt.Errorf("cannot read InstrumentationScope: %w", err)
	}
	if ok {
		if err := decodeInstrumentationScope(scopeData, fs, fb); err != nil {
			return fmt.Errorf("cannot decode InstrumentationScope: %w", err)
		}
	}

	commonFieldsLen := len(fs.Fields)
	fbLen := len(fb.buf)

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 2:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read LogRecord data")
			}

			eventName, timestamp, err := decodeLogRecord(data, fs, fb)
			if err != nil {
				return fmt.Errorf("cannot decode LogRecord: %w", err)
			}
			if eventName != "" {
				// Insert eventName into stream fields
				fs.Add("dummy", "value")
				for i := len(fs.Fields) - 1; i > streamFieldsLen; i-- {
					fs.Fields[i] = fs.Fields[i-1]
				}
				f := &fs.Fields[streamFieldsLen]
				f.Name = "event_name"
				f.Value = eventName

				pushLogs(timestamp, fs.Fields, streamFieldsLen+1)

				// Return back common fields to their places before the next iteration
				fs.Fields = append(fs.Fields[:streamFieldsLen], fs.Fields[streamFieldsLen+1:commonFieldsLen+1]...)
			} else {
				pushLogs(timestamp, fs.Fields, streamFieldsLen)

				fs.Fields = fs.Fields[:commonFieldsLen]
			}

			fb.buf = fb.buf[:fbLen]
		}
	}
	return nil
}

func decodeInstrumentationScope(src []byte, fs *logstorage.Fields, fb *fmtBuffer) error {
	// See https://github.com/open-telemetry/opentelemetry-proto/blob/a5f0eac5b802f7ae51dfe41e5116fe5548955e64/opentelemetry/proto/common/v1/common.proto#L76
	//
	// message InstrumentationScope {
	//   string name = 1;
	//   string version = 2;
	//   repeated KeyValue attributes = 3;
	// }

	nameStr, ok, err := easyproto.GetString(src, 1)
	if err != nil {
		return fmt.Errorf("cannot read name: %w", err)
	}
	name := "unknown"
	if ok {
		name = nameStr
	}
	fs.Add("scope.name", name)

	versionStr, ok, err := easyproto.GetString(src, 2)
	if err != nil {
		return fmt.Errorf("cannot read version: %w", err)
	}
	version := "unknown"
	if ok {
		version = versionStr
	}
	fs.Add("scope.version", version)

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 3:
			attributesData, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read Attributes data")
			}
			if err := decodeKeyValue(attributesData, fs, fb, "scope.attributes"); err != nil {
				return fmt.Errorf("cannot decode Attributes: %w", err)
			}
		}
	}

	return nil
}

func decodeLogRecord(src []byte, fs *logstorage.Fields, fb *fmtBuffer) (string, int64, error) {
	// See https://github.com/open-telemetry/opentelemetry-proto/blob/a5f0eac5b802f7ae51dfe41e5116fe5548955e64/opentelemetry/proto/logs/v1/logs.proto#L136
	//
	// message LogRecord {
	//   fixed64 time_unix_nano = 1;
	//   fixed64 observed_time_unix_nano = 11;
	//   SeverityNumber severity_number = 2;
	//   string severity_text = 3;
	//   AnyValue body = 5;
	//   repeated KeyValue attributes = 6;
	//   bytes trace_id = 9;
	//   bytes span_id = 10;
	//   string event_name = 12;
	// }

	var (
		timeUnixNano         uint64
		observedTimeUnixNano uint64
		severityText         string
		severityNumber       int32
		eventName            string
	)

	var fc easyproto.FieldContext
	for len(src) > 0 {
		var err error
		src, err = fc.NextField(src)
		if err != nil {
			return "", 0, fmt.Errorf("cannot read the next field: %w", err)
		}
		var ok bool
		switch fc.FieldNum {
		case 1:
			timeUnixNano, ok = fc.Fixed64()
			if !ok {
				return "", 0, fmt.Errorf("cannot read log record timestamp")
			}
		case 11:
			observedTimeUnixNano, ok = fc.Fixed64()
			if !ok {
				return "", 0, fmt.Errorf("cannot read log record observed timestamp")
			}
		case 2:
			severityNumber, ok = fc.Int32()
			if !ok {
				return "", 0, fmt.Errorf("cannot read severity number")
			}
		case 3:
			severityText, ok = fc.String()
			if !ok {
				return "", 0, fmt.Errorf("cannot read severity string")
			}
		case 5:
			body, ok := fc.MessageData()
			if !ok {
				return "", 0, fmt.Errorf("cannot read Body")
			}
			if err := decodeAnyValue(body, fs, fb, ""); err != nil {
				return "", 0, fmt.Errorf("cannot decode Body: %w", err)
			}
		case 6:
			attributesData, ok := fc.MessageData()
			if !ok {
				return "", 0, fmt.Errorf("cannot read Attributes data")
			}
			if err := decodeKeyValue(attributesData, fs, fb, ""); err != nil {
				return "", 0, fmt.Errorf("cannot decode Attributes: %w", err)
			}
		case 9:
			traceID, ok := fc.Bytes()
			if !ok {
				return "", 0, fmt.Errorf("cannot read trace id")
			}
			traceIDHex := fb.formatHex(traceID)
			fs.Add("trace_id", traceIDHex)
		case 10:
			spanID, ok := fc.Bytes()
			if !ok {
				return "", 0, fmt.Errorf("cannot read span id")
			}
			spanIDHex := fb.formatHex(spanID)
			fs.Add("span_id", spanIDHex)
		case 12:
			eventName, ok = fc.String()
			if !ok {
				return "", 0, fmt.Errorf("cannot read event_name")
			}
		}
	}

	if severityText == "" {
		severityText = formatSeverity(severityNumber)
	}
	fs.Add("severity", severityText)

	var timestamp int64
	switch {
	case timeUnixNano > 0:
		timestamp = int64(timeUnixNano)
	case observedTimeUnixNano > 0:
		timestamp = int64(observedTimeUnixNano)
	default:
		timestamp = time.Now().UnixNano()
	}

	return eventName, timestamp, nil
}

func decodeKeyValue(src []byte, fs *logstorage.Fields, fb *fmtBuffer, fieldNamePrefix string) error {
	// message KeyValue {
	//   string key = 1;
	//   AnyValue value = 2;
	// }

	// Decode key
	key, ok, err := easyproto.GetString(src, 1)
	if err != nil {
		return fmt.Errorf("cannot find Key in KeyValue: %w", err)
	}
	if !ok {
		// Key is missing, skip it.
		// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/869#issuecomment-3631307996
		return nil
	}
	fieldName := fb.formatSubFieldName(fieldNamePrefix, key)

	// Decode value
	valueData, ok, err := easyproto.GetMessageData(src, 2)
	if err != nil {
		return fmt.Errorf("cannot find Value in KeyValue: %w", err)
	}
	if !ok {
		// Value is null, skip it.
		return nil
	}

	if err := decodeAnyValue(valueData, fs, fb, fieldName); err != nil {
		return fmt.Errorf("cannot decode AnyValue: %w", err)
	}

	return nil
}

func decodeAnyValue(src []byte, fs *logstorage.Fields, fb *fmtBuffer, fieldName string) (err error) {
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
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			stringValue, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read StringValue")
			}
			fs.Add(fieldName, stringValue)
		case 2:
			boolValue, ok := fc.Bool()
			if !ok {
				return fmt.Errorf("cannot read BoolValue")
			}
			boolValueStr := strconv.FormatBool(boolValue)
			fs.Add(fieldName, boolValueStr)
		case 3:
			intValue, ok := fc.Int64()
			if !ok {
				return fmt.Errorf("cannot read IntValue")
			}
			intValueStr := fb.formatInt(intValue)
			fs.Add(fieldName, intValueStr)
		case 4:
			doubleValue, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read DoubleValue")
			}
			doubleValueStr := fb.formatFloat(doubleValue)
			fs.Add(fieldName, doubleValueStr)
		case 5:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read ArrayValue")
			}

			a := jsonArenaPool.Get()
			// Encode arrays as JSON to match the behavior of /insert/jsonline
			arr, err := decodeArrayValueToJSON(data, a, fb)
			if err != nil {
				jsonArenaPool.Put(a)
				return fmt.Errorf("cannot decode ArrayValue: %w", err)
			}
			encodedArr := fb.encodeJSONValue(arr)
			jsonArenaPool.Put(a)

			fs.Add(fieldName, encodedArr)
		case 6:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read KeyValueList")
			}
			if err := decodeKeyValueList(data, fs, fb, fieldName); err != nil {
				return fmt.Errorf("cannot decode KeyValueList: %w", err)
			}
		case 7:
			bytesValue, ok := fc.Bytes()
			if !ok {
				return fmt.Errorf("cannot read BytesValue")
			}
			v := fb.formatBase64(bytesValue)
			fs.Add(fieldName, v)
		}
	}
	return nil
}

func decodeKeyValueList(src []byte, fs *logstorage.Fields, fb *fmtBuffer, fieldNamePrefix string) (err error) {
	// message KeyValueList {
	//   repeated KeyValue values = 1;
	// }

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read KeyValue data")
			}
			if err := decodeKeyValue(data, fs, fb, fieldNamePrefix); err != nil {
				return fmt.Errorf("cannot decode KeyValue: %w", err)
			}
		}
	}
	return nil
}

func formatSeverity(severity int32) string {
	if severity < 0 || severity >= int32(len(logSeverities)) {
		return logSeverities[0]
	}
	return logSeverities[severity]
}

// See https://github.com/open-telemetry/opentelemetry-collector/blob/a0cbea73c189551d751d09659e306f48f594fd62/pdata/plog/severity_number.go#L41
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
