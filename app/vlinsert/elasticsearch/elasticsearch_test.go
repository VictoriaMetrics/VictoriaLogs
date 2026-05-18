package elasticsearch

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/compress/zstd"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
)

func TestReadBulkRequest_Failure(t *testing.T) {
	f := func(data string) {
		t.Helper()

		tlp := &insertutil.TestLogMessageProcessor{}
		r := bytes.NewBufferString(data)
		results, err := readBulkRequest("test", r, "", []string{"_time"}, []string{"_msg"}, nil, tlp)
		if err == nil {
			t.Fatalf("expecting non-empty error")
		}
		if len(results) != 0 {
			t.Fatalf("unexpected non-zero results=%d", len(results))
		}
	}
	f("foobar")
	f(`{}`)
	f(`{"create":{}}`)
	f(`{"creat":{}}
{}`)
	f(`{"create":{}}
foobar`)
}

func TestReadBulkRequest_TooLongLine(t *testing.T) {
	f := func(data string, resultsExpected bulkItemResult, timestampsExpected []int64, rowsExpected, responseExpected string) {
		t.Helper()

		origMaxLineSizeBytes := insertutil.MaxLineSizeBytes.String()
		if err := insertutil.MaxLineSizeBytes.Set("128"); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		defer func() {
			if err := insertutil.MaxLineSizeBytes.Set(origMaxLineSizeBytes); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		}()

		tlp := &insertutil.TestLogMessageProcessor{}
		r := bytes.NewBufferString(data)
		results, err := readBulkRequest("test", r, "", []string{"@timestamp"}, []string{"message"}, nil, tlp)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if len(results) != len(resultsExpected) {
			t.Fatalf("unexpected results read; got %d; want %d", len(results), len(resultsExpected))
		}
		for i, resultExpected := range resultsExpected {
			if !errors.Is(results[i], resultExpected) {
				t.Fatalf("unexpected error for result[%d]; got %s; want %s", i, results[i], resultExpected)
			}
		}
		if err := tlp.Verify(timestampsExpected, rowsExpected); err != nil {
			t.Fatal(err)
		}

		result := BulkResponse(results, 123)
		if result != responseExpected {
			t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", result, responseExpected)
		}
	}

	// a too long log message in the middle doesn't prevent parsing the following log messages.
	data := `{"create":{"_index":"filebeat-8.8.0"}}
{"@timestamp":"2023-06-06T04:48:11.735Z","message":"foo"}
{"create":{"_index":"filebeat-8.8.0"}}
{"message":"` + strings.Repeat("x", 200) + `"}
{"create":{"_index":"filebeat-8.8.0"}}
{"@timestamp":"2023-06-06T04:48:12.735Z","message":"bar"}
`
	timestampsExpected := []int64{1686026891735000000, 1686026892735000000}
	rowsExpected := `{"_msg":"foo"}
{"_msg":"bar"}`
	responseExpected := `{"took":123,"errors":true,"items":[{"create":{"status":201}},{"create":{"status":413,"error":{"reason":"log line exceeds -insert.maxLineSizeBytes"}}},{"create":{"status":201}}]}`
	f(data, bulkItemResult{nil, errTooLongLine, nil}, timestampsExpected, rowsExpected, responseExpected)

	// a too long log message at EOF doesn't result in the whole request failure.
	data = `{"create":{"_index":"filebeat-8.8.0"}}
{"@timestamp":"2023-06-06T04:48:11.735Z","message":"foo"}
{"create":{"_index":"filebeat-8.8.0"}}
{"message":"` + strings.Repeat("x", 200) + `"}`
	timestampsExpected = []int64{1686026891735000000}
	rowsExpected = `{"_msg":"foo"}`
	responseExpected = `{"took":123,"errors":true,"items":[{"create":{"status":201}},{"create":{"status":413,"error":{"reason":"log line exceeds -insert.maxLineSizeBytes"}}}]}`
	f(data, bulkItemResult{nil, errTooLongLine}, timestampsExpected, rowsExpected, responseExpected)
}

func TestReadBulkRequest_Success(t *testing.T) {
	f := func(data, encoding, timeField, msgField string, preserveKeys []string, timestampsExpected []int64, resultExpected string) {
		t.Helper()

		timeFields := []string{"non_existing_foo", timeField, "non_existing_bar"}
		msgFields := []string{"non_existing_foo", msgField, "non_exiting_bar"}
		tlp := &insertutil.TestLogMessageProcessor{}

		// Read the request without compression
		r := bytes.NewBufferString(data)
		results, err := readBulkRequest("test", r, "", timeFields, msgFields, preserveKeys, tlp)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if len(results) != len(timestampsExpected) {
			t.Fatalf("unexpected results read; got %d; want %d", len(results), len(timestampsExpected))
		}
		if err := tlp.Verify(timestampsExpected, resultExpected); err != nil {
			t.Fatal(err)
		}

		// Read the request with compression
		tlp = &insertutil.TestLogMessageProcessor{}
		if encoding != "" {
			data = compressData(data, encoding)
		}
		r = bytes.NewBufferString(data)
		results, err = readBulkRequest("test", r, encoding, timeFields, msgFields, preserveKeys, tlp)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if len(results) != len(timestampsExpected) {
			t.Fatalf("unexpected results read; got %d; want %d", len(results), len(timestampsExpected))
		}
		if err := tlp.Verify(timestampsExpected, resultExpected); err != nil {
			t.Fatalf("verification failure after compression: %s", err)
		}
	}

	// Verify an empty data
	f("", "gzip", "_time", "_msg", nil, nil, "")
	f("\n", "gzip", "_time", "_msg", nil, nil, "")
	f("\n\n", "gzip", "_time", "_msg", nil, nil, "")

	// Verify non-empty data
	data := `{"create":{"_index":"filebeat-8.8.0"}}
{"@timestamp":"2023-06-06T04:48:11.735Z","log":{"offset":71770,"file":{"path":"/var/log/auth.log"}},"message":"foobar"}
{"create":{"_index":"filebeat-8.8.0"}}
{"@timestamp":"2023-06-06 04:48:12.735+01:00","message":"baz"}
{"index":{"_index":"filebeat-8.8.0"}}
{"message":"xyz","@timestamp":"1686026893735","x":"y"}
{"create":{"_index":"filebeat-8.8.0"}}
{"message":"qwe rty","@timestamp":"1686026893"}
{"create":{"_index":"filebeat-8.8.0"}}
{"message":"qwe rty float","@timestamp":"1686026123.62"}
`
	timeField := "@timestamp"
	msgField := "message"
	timestampsExpected := []int64{1686026891735000000, 1686023292735000000, 1686026893735000000, 1686026893000000000, 1686026123620000000}
	resultExpected := `{"log.offset":"71770","log.file.path":"/var/log/auth.log","_msg":"foobar"}
{"_msg":"baz"}
{"_msg":"xyz","x":"y"}
{"_msg":"qwe rty"}
{"_msg":"qwe rty float"}`
	f(data, "zstd", timeField, msgField, nil, timestampsExpected, resultExpected)

	// Verify non-empty data with preserve keys
	data = `{"create":{"_index":"filebeat-8.8.0"}}
{"@timestamp":"2023-06-06T04:48:11.735Z","log":{"offset":71770,"file":{"path":"/var/log/auth.log"}},"message":"foobar"}
`
	timeField = "@timestamp"
	msgField = "message"
	preserveKeys := []string{"log.file"}
	timestampsExpected = []int64{1686026891735000000}
	resultExpected = `{"log.offset":"71770","log.file":"{\"path\":\"/var/log/auth.log\"}","_msg":"foobar"}`
	f(data, "zstd", timeField, msgField, preserveKeys, timestampsExpected, resultExpected)
}

func compressData(s string, encoding string) string {
	var bb bytes.Buffer
	var zw io.WriteCloser
	switch encoding {
	case "gzip":
		zw = gzip.NewWriter(&bb)
	case "zstd":
		zw, _ = zstd.NewWriter(&bb)
	case "snappy":
		return string(snappy.Encode(nil, []byte(s)))
	case "deflate":
		zw = zlib.NewWriter(&bb)
	default:
		panic(fmt.Errorf("%q encoding is not supported", encoding))
	}
	if _, err := zw.Write([]byte(s)); err != nil {
		panic(fmt.Errorf("unexpected error when compressing data: %w", err))
	}
	if err := zw.Close(); err != nil {
		panic(fmt.Errorf("unexpected error when closing gzip writer: %w", err))
	}
	return bb.String()
}
