package logstorage

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
)

func TestTruncateTimestamp(t *testing.T) {
	f := func(timestampStr, bucketSizeStr, offsetStr, resultExpected string) {
		t.Helper()

		ts, ok := TryParseTimestampRFC3339Nano(timestampStr)
		if !ok {
			t.Fatalf("cannot parse timestamp %q", timestampStr)
		}

		var bucketSize int64
		if bucketSizeStr != "month" && bucketSizeStr != "year" {
			n, ok := tryParseBucketSize(bucketSizeStr)
			if !ok {
				t.Fatalf("cannot parse bucket %q", bucketSizeStr)
			}
			bucketSize = int64(n)
		}

		var offset int64
		if offsetStr != "" {
			offset, ok = tryParseDuration(offsetStr)
			if !ok {
				t.Fatalf("cannot parse offset %q", offsetStr)
			}
		}

		tsBucketed := truncateTimestamp(ts, bucketSize, offset, bucketSizeStr)
		result := marshalTimestampRFC3339NanoString(nil, tsBucketed)
		if string(result) != resultExpected {
			t.Fatalf("unexpected result; got %q; want %q", result, resultExpected)
		}
	}

	f("2025-01-20T10:20:30.12345Z", "10ms", "", "2025-01-20T10:20:30.12Z")
	f("2025-01-20T10:20:30.12345Z", "10m", "", "2025-01-20T10:20:00Z")
	f("2025-01-20T10:20:30.12345Z", "hour", "", "2025-01-20T10:00:00Z")
	f("2025-01-20T10:20:30.12345Z", "day", "", "2025-01-20T00:00:00Z")
	f("2025-01-19T23:59:59.999999999Z", "week", "", "2025-01-13T00:00:00Z")
	f("2025-01-20T10:20:30.12345Z", "week", "", "2025-01-20T00:00:00Z")
	f("2025-01-21T10:20:30.12345Z", "week", "", "2025-01-20T00:00:00Z")
	f("2025-03-20T10:20:30.12345Z", "month", "", "2025-03-01T00:00:00Z")
	f("2025-01-20T10:20:30.12345Z", "year", "", "2025-01-01T00:00:00Z")

	// with offset
	f("2025-01-20T10:20:30.1234Z", "1d", "", "2025-01-20T00:00:00Z")
	f("2025-01-20T10:20:30.1234Z", "1d", "2h", "2025-01-19T22:00:00Z")
	f("2025-01-20T10:20:30.1234Z", "1d", "-2h", "2025-01-20T02:00:00Z")
	f("2025-01-20T22:20:30.1234-05:00", "1d", "", "2025-01-21T00:00:00Z")
	f("2025-01-20T22:20:30.1234-05:00", "1d", "5h", "2025-01-20T19:00:00Z")
	f("2025-01-20T22:20:30.1234-05:00", "1d", "-5h", "2025-01-20T05:00:00Z")
	f("2025-01-19T23:59:59.999999999Z", "week", "3h", "2025-01-19T21:00:00Z")
	f("2025-01-19T23:59:59.999999999Z", "week", "-3h", "2025-01-13T03:00:00Z")
	f("2025-01-31T23:20:30-04:00", "month", "", "2025-02-01T00:00:00Z")
	f("2025-01-31T23:20:30+04:00", "month", "", "2025-01-01T00:00:00Z")
	f("2025-01-31T23:20:30Z", "month", "4h", "2025-01-31T20:00:00Z")
	f("2025-01-31T23:20:30Z", "month", "-4h", "2025-01-01T04:00:00Z")
	f("2024-12-31T23:20:30Z", "year", "", "2024-01-01T00:00:00Z")
	f("2024-12-31T23:20:30Z", "year", "4h", "2024-12-31T20:00:00Z")
	f("2024-12-31T23:20:30Z", "year", "-4h", "2024-01-01T04:00:00Z")

	// negative timestamps
	f("1970-01-01T00:00:00Z", "week", "", "1969-12-29T00:00:00Z")
	f("1970-01-01T00:00:00Z", "week", "3d", "1969-12-26T00:00:00Z")
	f("1970-01-01T00:00:00Z", "week", "4d", "1970-01-01T00:00:00Z")
	f("1970-01-01T00:00:00Z", "week", "-3d", "1970-01-01T00:00:00Z")
	f("1970-01-01T00:00:00Z", "week", "-4d", "1969-12-26T00:00:00Z")
}

func TestTruncateFloat64(t *testing.T) {
	f := func(n, bucketSize, offset, resultExpected float64) {
		t.Helper()

		_, e := decimal.FromFloat(bucketSize)
		p10 := math.Pow10(int(-e))
		bucketSizeP10 := int64(bucketSize * p10)

		result := truncateFloat64(n, p10, bucketSizeP10, offset)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
	}

	f(0, 100, 0, 0)
	f(99, 100, 0, 0)
	f(-1, 100, 0, -100)
	f(-100, 100, 0, -100)
	f(-101, 100, 0, -200)

	f(1, 100, -10, -90)
	f(1, 100, 10, -10)
	f(0, 100, -30, -70)
	f(0, 100, 30, -30)
	f(120, 100, -30, 30)
	f(120, 100, 30, 70)
	f(130, 100, 30.3, 69.7)
	f(130.3, 100, -30.3, 130.3)
	f(130.3, 100, 30.3, 69.7)
	f(130.4, 100, -30.3, 130.3)
	f(130.4, 100, 30.3, 69.7)

	f(1.25, 0.1, 0, 1.2)
	f(1.3, 0.1, 0, 1.3)
	f(1.312, 0.1, 0, 1.3)
	f(-1.3, 0.1, 0, -1.3)
	f(-1.25, 0.1, 0, -1.3)
	f(-0.25, 0.1, 0, -0.3)
	f(-0.01, 0.1, 0, -0.1)
	f(-0.01, 0.1, 0.05, -0.05)

	f(123, 20, 0, 120)
	f(120, 20, 0, 120)
	f(119, 20, 0, 100)
	f(0.123, 0.02, 0, 0.12)
	f(0.1, 0.02, 0, 0.1)
}

func TestTruncateInt64(t *testing.T) {
	f := func(n, bucketSize, offset, resultExpected int64) {
		t.Helper()

		result := truncateInt64(n, bucketSize, offset)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %d; want %d", result, resultExpected)
		}
	}

	f(0, 100, 0, 0)
	f(99, 100, 0, 0)
	f(-1, 100, 0, -100)
	f(-100, 100, 0, -100)
	f(-101, 100, 0, -200)

	f(1, 100, -10, -90)
	f(1, 100, 10, -10)
	f(0, 100, -30, -70)
	f(0, 100, 30, -30)
	f(120, 100, -30, 30)
	f(120, 100, 30, 70)
	f(130, 100, -30, 130)
	f(130, 100, 30, 70)
}

func TestTruncateUint64(t *testing.T) {
	f := func(n, bucketSize, offset, resultExpected uint64) {
		t.Helper()

		result := truncateUint64(n, bucketSize, offset)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %d; want %d", result, resultExpected)
		}
	}

	f(0, 100, 0, 0)
	f(99, 100, 0, 0)

	f(1, 100, 10, 0)
	f(0, 100, 30, 0)
	f(120, 100, 70, 30)
	f(120, 100, 30, 70)
	f(130, 100, 70, 130)
	f(130, 100, 30, 70)
}

func TestTruncateUint32(t *testing.T) {
	f := func(n, bucketSize, offset, resultExpected uint32) {
		t.Helper()

		result := truncateUint32(n, bucketSize, offset)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %d; want %d", result, resultExpected)
		}
	}

	f(0, 100, 0, 0)
	f(99, 100, 0, 0)

	f(1, 100, 10, 0)
	f(0, 100, 30, 0)
	f(120, 100, 30, 70)
	f(120, 100, 70, 30)
	f(130, 100, 30, 70)
	f(130, 100, 70, 130)
}

func TestBlockResultMustInitFromRows(t *testing.T) {
	f := func(rowsStr []string) {
		t.Helper()

		// parse rowsStr into rows
		var rows [][]Field
		p := GetJSONParser()
		for _, rowStr := range rowsStr {
			if err := p.ParseLogMessage([]byte(rowStr), nil, ""); err != nil {
				t.Fatalf("cannot parse input row: %s", err)
			}

			fields := make([]Field, len(p.Fields))
			for i, f := range p.Fields {
				fields[i] = Field{
					Name:  strings.Clone(f.Name),
					Value: strings.Clone(f.Value),
				}
			}
			rows = append(rows, fields)
		}
		PutJSONParser(p)

		// Pass rows into mustInitFromRows.
		br := getBlockResult()
		defer putBlockResult(br)

		br.mustInitFromRows(rows)

		// Verify the rows are properly put into br.
		cs := br.getColumns()
		var resultRowsStr []string
		for rowIdx := range rows {
			var fields []Field
			for _, c := range cs {
				v := c.getValueAtRow(br, rowIdx)
				fields = append(fields, Field{
					Name:  c.name,
					Value: v,
				})
			}
			rowStr := MarshalFieldsToJSON(nil, fields)
			resultRowsStr = append(resultRowsStr, string(rowStr))
		}

		if !reflect.DeepEqual(resultRowsStr, rowsStr) {
			t.Fatalf("unexpected rows\ngot\n%s\nwant\n%s", resultRowsStr, rowsStr)
		}
	}

	f(nil)
	f([]string{`{}`})

	// a single row
	f([]string{`{"foo":"bar","a":"b"}`})

	// multiple rows with the same set of fields
	f([]string{`{"a":"b","c":"d"}`, `{"a":"x","c":"y"}`})
	f([]string{`{"a":"b","c":"d"}`, `{"a":"x","c":"y"}`, `{"a":"qwewqr","c":"ieorer"}`})

	// multiple rows with different sets of fields
	f([]string{`{"a":"b","c":"d"}`, `{}`, `{"a":"x","c":"y"}`, `{"q":"z"}`})
}
