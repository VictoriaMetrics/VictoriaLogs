package opentelemetry

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
)

func BenchmarkParseProtobufRequest(b *testing.B) {
	for _, scopes := range []int{1, 2} {
		for _, rows := range []int{100, 1000} {
			for _, attributes := range []int{5, 10} {
				b.Run(fmt.Sprintf("scopes_%d/rows_%d/attributes_%d", scopes, rows, attributes), func(b *testing.B) {
					benchmarkParseProtobufRequest(b, scopes, rows, attributes)
				})
			}
		}
	}
}

func benchmarkParseProtobufRequest(b *testing.B, streams, rows, labels int) {
	blp := &insertutil.BenchmarkLogMessageProcessor{}
	b.ReportAllocs()
	b.SetBytes(int64(streams * rows))
	b.RunParallel(func(pb *testing.PB) {
		body := getProtobufBody(streams, rows, labels)
		for pb.Next() {
			if err := pushProtobufRequest(body, blp, nil, false); err != nil {
				panic(fmt.Errorf("unexpected error: %w", err))
			}
		}
	})
}

func getProtobufBody(scopesCount, rowsCount, attributesCount int) []byte {
	msg := "12345678910"

	attrValues := []*anyValue{
		{StringValue: ptrTo("string-attribute")},
		{IntValue: ptrTo[int64](12345)},
		{DoubleValue: ptrTo(3.14)},
	}
	attrs := make([]*keyValue, attributesCount)
	for j := 0; j < attributesCount; j++ {
		attrs[j] = &keyValue{
			Key:   fmt.Sprintf("key-%d", j),
			Value: attrValues[j%3],
		}
	}
	entries := make([]logRecord, rowsCount)
	for j := 0; j < rowsCount; j++ {
		entries[j] = logRecord{
			TimeUnixNano: 12345678910, ObservedTimeUnixNano: 12345678910, Body: anyValue{StringValue: &msg},
		}
	}
	scopes := make([]scopeLogs, scopesCount)

	for j := 0; j < scopesCount; j++ {
		scopes[j] = scopeLogs{
			LogRecords: entries,
		}
	}

	pr := exportLogsServiceRequest{
		ResourceLogs: []resourceLogs{
			{
				Resource: resource{
					Attributes: attrs,
				},
				ScopeLogs: scopes,
			},
		},
	}

	return pr.marshalProtobuf(nil)
}

func ptrTo[T any](s T) *T {
	return &s
}
