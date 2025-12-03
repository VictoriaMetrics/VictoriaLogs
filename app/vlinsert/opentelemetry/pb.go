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
type pushLogsHandler func(timestamp int64, fields []logstorage.Field, resourceFieldsLen int)

// decodeLogsData parses a LogsData protobuf message from src and calls the provided pushLogs for each decoded log record.
//
// See the definition of LogsData here:
// https://github.com/open-telemetry/opentelemetry-proto/blob/34d29fe5ad4689b5db0259d3750de2bfa195bc85/opentelemetry/proto/logs/v1/logs.proto#L38
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
	defer logstorage.PutFields(fs)

	// Decode resource
	data, ok, err := findMessageData(src, 1)
	if err != nil {
		return fmt.Errorf("cannot find Resource: %w", err)
	}
	if ok {
		if err = decodeResource(data, fs, fb); err != nil {
			return fmt.Errorf("cannot decode Resource: %w", err)
		}
	}
	resourceFieldsLen := len(fs.Fields)

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

			fs.Fields = fs.Fields[:resourceFieldsLen]

			if err := decodeScopeLogs(data, fs, pushLogs); err != nil {
				return fmt.Errorf("cannot decode ScopeLogs: %w", err)
			}
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

func decodeScopeLogs(src []byte, fs *logstorage.Fields, pushLogs pushLogsHandler) (err error) {
	// message ScopeLogs {
	//   repeated LogRecord log_records = 2;
	// }

	fb := getFmtBuffer()
	defer putFmtBuffer(fb)

	resourceFieldsLen := len(fs.Fields)

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

			fb.reset()
			fs.Fields = fs.Fields[:resourceFieldsLen]

			var timestamp int64
			timestamp, err = decodeLogRecord(data, fs, fb)
			if err != nil {
				return fmt.Errorf("cannot decode LogRecord: %w", err)
			}

			pushLogs(timestamp, fs.Fields, resourceFieldsLen)
		}
	}
	return nil
}

func decodeLogRecord(src []byte, fs *logstorage.Fields, fb *fmtBuffer) (int64, error) {
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
			return 0, fmt.Errorf("cannot read the next field: %w", err)
		}
		var ok bool
		switch fc.FieldNum {
		case 1:
			timeUnixNano, ok = fc.Fixed64()
			if !ok {
				return 0, fmt.Errorf("cannot read log record timestamp")
			}
		case 11:
			observedTimeUnixNano, ok = fc.Fixed64()
			if !ok {
				return 0, fmt.Errorf("cannot read log record observed timestamp")
			}
		case 2:
			severityNumber, ok = fc.Int32()
			if !ok {
				return 0, fmt.Errorf("cannot read severity number")
			}
		case 3:
			severityText, ok = fc.String()
			if !ok {
				return 0, fmt.Errorf("cannot read severity string")
			}
		case 5:
			body, ok := fc.MessageData()
			if !ok {
				return 0, fmt.Errorf("cannot read Body")
			}
			if err := decodeAnyValue(body, fs, fb, ""); err != nil {
				return 0, fmt.Errorf("cannot decode Body: %w", err)
			}
		case 6:
			data, ok := fc.MessageData()
			if !ok {
				return 0, fmt.Errorf("cannot read Attributes data")
			}
			if err := decodeKeyValue(data, fs, fb, ""); err != nil {
				return 0, fmt.Errorf("cannot decode Attributes: %w", err)
			}
		case 9:
			traceID, ok := fc.Bytes()
			if !ok {
				return 0, fmt.Errorf("cannot read trace id")
			}
			traceIDHex := fb.formatHex(traceID)
			fs.Add("trace_id", traceIDHex)
		case 10:
			spanID, ok := fc.Bytes()
			if !ok {
				return 0, fmt.Errorf("cannot read span id")
			}
			spanIDHex := fb.formatHex(spanID)
			fs.Add("span_id", spanIDHex)
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

	return timestamp, nil
}

func decodeKeyValue(src []byte, fs *logstorage.Fields, fb *fmtBuffer, fieldNamePrefix string) error {
	// message KeyValue {
	//   string key = 1;
	//   AnyValue value = 2;
	// }

	// Decode key
	data, ok, err := findMessageData(src, 1)
	if err != nil {
		return fmt.Errorf("cannot find Key in KeyValue: %w", err)
	}
	if !ok {
		return fmt.Errorf("key is missing in KeyValue")
	}
	fieldName := fb.formatSubFieldName(fieldNamePrefix, data)

	// Decode value
	data, ok, err = findMessageData(src, 2)
	if err != nil {
		return fmt.Errorf("cannot find Value in KeyValue: %w", err)
	}
	if !ok {
		// Value is null, skip it.
		return nil
	}

	if err := decodeAnyValue(data, fs, fb, fieldName); err != nil {
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

func findMessageData(src []byte, fieldNum uint32) (data []byte, ok bool, err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return nil, false, fmt.Errorf("cannot read next field: %w", err)
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

func formatSeverity(severity int32) string {
	if severity < 0 || severity >= int32(len(logSeverities)) {
		return logSeverities[0]
	}
	return logSeverities[severity]
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
