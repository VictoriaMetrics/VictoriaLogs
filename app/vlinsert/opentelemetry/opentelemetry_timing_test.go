package opentelemetry

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
)

func BenchmarkPushProtobufRequest(b *testing.B) {
	for _, scopes := range []int{1, 2} {
		for _, rows := range []int{1, 10, 100, 1000} {
			for _, attributes := range []int{5, 10} {
				b.Run(fmt.Sprintf("scopes_%d/rows_%d/attributes_%d", scopes, rows, attributes), func(b *testing.B) {
					benchmarkPushProtobufRequest(b, scopes, rows, attributes)
				})
			}
		}
	}
}

func benchmarkPushProtobufRequest(b *testing.B, streams, rows, labels int) {
	body := getProtobufBody(streams, rows, labels)

	blp := &insertutil.BenchmarkLogMessageProcessor{}
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := pushProtobufRequest(body, blp, nil, false); err != nil {
				panic(fmt.Errorf("unexpected error: %w", err))
			}
		}
	})
}

func getProtobufBody(scopesCount, rowsCount, attributesCount int) []byte {
	attrValues := []*anyValue{
		{StringValue: new("string-attribute")},
		{BoolValue: new(true)},
		{IntValue: new(int64(12345))},
		{DoubleValue: new(3.14)},
		{
			ArrayValue: &arrayValue{
				Values: []*anyValue{
					{StringValue: new("abc")},
				},
			},
		},
		{
			KeyValueList: &keyValueList{
				Values: []*keyValue{
					{
						Key: "foobarbaz",
						Value: &anyValue{
							StringValue: new("xyzqwe"),
						},
					},
				},
			},
		},
	}

	attrs := make([]*keyValue, attributesCount)
	for j := range attributesCount {
		attrs[j] = &keyValue{
			Key:   fmt.Sprintf("key-%d", j),
			Value: attrValues[j%len(attrValues)],
		}
	}
	entries := make([]logRecord, rowsCount)
	for j := range rowsCount {
		entries[j] = logRecord{
			TimeUnixNano:         12345678910,
			ObservedTimeUnixNano: 12345678910,
			Body: anyValue{
				StringValue: new("12345678910"),
			},
		}
	}
	scopes := make([]scopeLogs, scopesCount)

	for j := range scopesCount {
		scopes[j] = scopeLogs{
			Scope: &instrumentationScope{
				Name:    "abc",
				Version: "v1.2.345",
				Attributes: []*keyValue{
					{
						Key: "qwe",
						Value: &anyValue{
							StringValue: new("ierweo"),
						},
					},
				},
			},
			LogRecords: entries,
		}
	}

	pr := logsData{
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
