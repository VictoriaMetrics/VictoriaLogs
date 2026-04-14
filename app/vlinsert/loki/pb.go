package loki

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/easyproto"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// the pushLogsHandler must store log entry with the given args.
//
// The handler must copy resource and attributes before returning,
// since the caller can change them, so they become invalid if not copied.
type pushLogsHandler func(timestamp int64, line string, fs *logstorage.Fields, streamFieldsLen int)

// decodePushRequest parses a PushRequest protobuf message from src and calls the provided pushLogs for each decoded log record.
//
// See https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L14
func decodePushRequest(src []byte, pushLogs pushLogsHandler) (err error) {
	// message PushRequest {
	//   repeated Stream streams = 1;
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
				return fmt.Errorf("cannot read Stream data")
			}
			if err := decodeStream(data, pushLogs); err != nil {
				return fmt.Errorf("cannot unmarshal Stream: %w", err)
			}
		}
	}
	return nil
}

func decodeStream(src []byte, pushLogs pushLogsHandler) error {
	// message Stream {
	//   string labels = 1;
	//   repeated Entry entries = 2;
	// }

	fs := logstorage.GetFields()
	defer func() {
		// Explicitly clear fs up to its' capacity in order to free up
		// all the references to the original byte slice, so it could be freed by Go GC.
		fs.ClearUpToCapacity()
		logstorage.PutFields(fs)
	}()

	labels, ok, err := easyproto.GetString(src, 1)
	if err != nil {
		return fmt.Errorf("cannot read labels: %w", err)
	}
	if !ok {
		return fmt.Errorf("missing labels")
	}
	if err := parsePromLabels(fs, labels); err != nil {
		return fmt.Errorf("cannot parse labels %q: %w", labels, err)
	}
	streamFieldsLen := len(fs.Fields)

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
				return fmt.Errorf("cannot read Entry data")
			}

			if err := decodeEntry(data, fs, pushLogs); err != nil {
				return fmt.Errorf("cannot unmarshal Entry: %w", err)
			}

			fs.Fields = fs.Fields[:streamFieldsLen]
		}
	}
	return nil
}

func decodeEntry(src []byte, fs *logstorage.Fields, pushLogs pushLogsHandler) error {
	// message Entry {
	//   Timestamp timestamp = 1;
	//   string line = 2;
	//   repeated LabelPair structuredMetadata = 3;
	// }

	var (
		timestamp int64
		line      string
	)

	streamFieldsLen := len(fs.Fields)

	var fc easyproto.FieldContext
	for len(src) > 0 {
		var err error
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		var ok bool
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read Timestamp data")
			}
			timestamp, err = decodeTimestamp(data)
			if err != nil {
				return fmt.Errorf("cannot unmarshal Timestamp: %w", err)
			}
		case 2:
			line, ok = fc.String()
			if !ok {
				return fmt.Errorf("cannot read Line")
			}
		case 3:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read StructuredMetadata")
			}
			if err := decodeLabelPair(data, fs); err != nil {
				return fmt.Errorf("cannot unmarshal StructuredMetadata: %w", err)
			}
		}
	}

	pushLogs(timestamp, line, fs, streamFieldsLen)

	return nil
}

func decodeLabelPair(src []byte, fs *logstorage.Fields) error {
	// message LabelPair {
	//   string name = 1;
	//   string value = 2;
	// }

	name, ok, err := easyproto.GetString(src, 1)
	if err != nil {
		return fmt.Errorf("cannot read name: %w", err)
	}
	if !ok {
		return fmt.Errorf("missing name")
	}

	value, ok, err := easyproto.GetString(src, 2)
	if err != nil {
		return fmt.Errorf("cannot read value: %w", err)
	}
	if !ok {
		return fmt.Errorf("missing value")
	}

	if name != "" && value != "" {
		fs.Add(name, value)
	}

	return nil
}

func decodeTimestamp(src []byte) (int64, error) {
	// message Timestamp {
	//   int64 seconds = 1;
	//   int32 nanos = 2;
	// }

	var (
		seconds int64
		nanos   int32
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
			seconds, ok = fc.Int64()
			if !ok {
				return 0, fmt.Errorf("cannot read Seconds")
			}
		case 2:
			nanos, ok = fc.Int32()
			if !ok {
				return 0, fmt.Errorf("cannot read Nanos")
			}
		}
	}

	nsecs := seconds*1e9 + int64(nanos)

	return nsecs, nil
}

// parsePromLabels parses log fields in Prometheus text exposition format from s and appends them to fs.
//
// See test data of promtail for examples: https://github.com/grafana/loki/blob/a24ef7b206e0ca63ee74ca6ecb0a09b745cd2258/pkg/push/types_test.go
func parsePromLabels(fs *logstorage.Fields, s string) error {
	// Make sure s is wrapped into `{...}`
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return fmt.Errorf("too short string to parse: %q", s)
	}
	if s[0] != '{' {
		return fmt.Errorf("missing `{` at the beginning of %q", s)
	}
	if s[len(s)-1] != '}' {
		return fmt.Errorf("missing `}` at the end of %q", s)
	}
	s = s[1 : len(s)-1]

	for len(s) > 0 {
		// Parse label name
		n := strings.IndexByte(s, '=')
		if n < 0 {
			return fmt.Errorf("cannot find `=` char for label value at %s", s)
		}
		name := s[:n]
		s = s[n+1:]

		// Parse label value
		qs, err := strconv.QuotedPrefix(s)
		if err != nil {
			return fmt.Errorf("cannot parse value for label %q at %s: %w", name, s, err)
		}
		s = s[len(qs):]
		value, err := strconv.Unquote(qs)
		if err != nil {
			return fmt.Errorf("cannot unquote value %q for label %q: %w", qs, name, err)
		}

		// Append the found field to dst.
		fs.Add(name, value)

		// Check whether there are other labels remaining
		if len(s) == 0 {
			break
		}
		if !strings.HasPrefix(s, ",") {
			return fmt.Errorf("missing `,` char at %s", s)
		}
		s = s[1:]
		s = strings.TrimPrefix(s, " ")
	}
	return nil
}
