package opentelemetry

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/opentelemetry/pb"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
)

func TestPushProtoOk(t *testing.T) {
	f := func(src []pb.ResourceLogs, timestampsExpected []int64, resultExpected string) {
		t.Helper()
		lr := pb.ExportLogsServiceRequest{
			ResourceLogs: src,
		}

		pData := lr.MarshalProtobuf(nil)
		tlp := &insertutil.TestLogMessageProcessor{}
		if err := pushProtobufRequest(pData, tlp, nil, false); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if err := tlp.Verify(timestampsExpected, resultExpected); err != nil {
			t.Fatal(err)
		}
	}

	// single line without resource attributes
	f([]pb.ResourceLogs{
		{
			ScopeLogs: []pb.ScopeLogs{
				{
					LogRecords: []pb.LogRecord{
						{Attributes: []*pb.KeyValue{}, TimeUnixNano: 1234, SeverityNumber: 1, Body: pb.AnyValue{StringValue: ptrTo("log-line-message")}},
					},
				},
			},
		},
	},
		[]int64{1234},
		`{"_msg":"log-line-message","severity":"Trace"}`,
	)

	// severities mapping
	f([]pb.ResourceLogs{
		{
			ScopeLogs: []pb.ScopeLogs{
				{
					LogRecords: []pb.LogRecord{
						{Attributes: []*pb.KeyValue{}, TimeUnixNano: 1234, SeverityNumber: 1, Body: pb.AnyValue{StringValue: ptrTo("log-line-message")}},
						{Attributes: []*pb.KeyValue{}, TimeUnixNano: 1234, SeverityNumber: 13, Body: pb.AnyValue{StringValue: ptrTo("log-line-message")}},
						{Attributes: []*pb.KeyValue{}, TimeUnixNano: 1234, SeverityNumber: 24, Body: pb.AnyValue{StringValue: ptrTo("log-line-message")}},
					},
				},
			},
		},
	},
		[]int64{1234, 1234, 1234},
		`{"_msg":"log-line-message","severity":"Trace"}
{"_msg":"log-line-message","severity":"Warn"}
{"_msg":"log-line-message","severity":"Fatal4"}`,
	)

	// multi-line with resource attributes
	f([]pb.ResourceLogs{
		{
			Resource: pb.Resource{
				Attributes: []*pb.KeyValue{
					{Key: "logger", Value: &pb.AnyValue{StringValue: ptrTo("context")}},
					{Key: "instance_id", Value: &pb.AnyValue{IntValue: ptrTo[int64](10)}},
					{Key: "node_taints", Value: &pb.AnyValue{KeyValueList: &pb.KeyValueList{
						Values: []*pb.KeyValue{
							{Key: "role", Value: &pb.AnyValue{StringValue: ptrTo("dev")}},
							{Key: "cluster_load_percent", Value: &pb.AnyValue{DoubleValue: ptrTo(0.55)}},
						},
					}}},
				},
			},
			ScopeLogs: []pb.ScopeLogs{
				{
					LogRecords: []pb.LogRecord{
						{Attributes: []*pb.KeyValue{}, TimeUnixNano: 1234, SeverityNumber: 1, Body: pb.AnyValue{StringValue: ptrTo("log-line-message")}},
						{Attributes: []*pb.KeyValue{}, TimeUnixNano: 1235, SeverityNumber: 25, Body: pb.AnyValue{StringValue: ptrTo("log-line-message-msg-2")}},
						{Attributes: []*pb.KeyValue{}, TimeUnixNano: 1236, SeverityNumber: -1, Body: pb.AnyValue{StringValue: ptrTo("log-line-message-msg-2")}},
					},
				},
			},
		},
	},
		[]int64{1234, 1235, 1236},
		`{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","_msg":"log-line-message","severity":"Trace"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","_msg":"log-line-message-msg-2","severity":"Unspecified"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","_msg":"log-line-message-msg-2","severity":"Unspecified"}`,
	)

	// multi-scope with resource attributes and multi-line
	f([]pb.ResourceLogs{
		{
			Resource: pb.Resource{
				Attributes: []*pb.KeyValue{
					{Key: "logger", Value: &pb.AnyValue{StringValue: ptrTo("context")}},
					{Key: "instance_id", Value: &pb.AnyValue{IntValue: ptrTo[int64](10)}},
					{Key: "node_taints", Value: &pb.AnyValue{KeyValueList: &pb.KeyValueList{
						Values: []*pb.KeyValue{
							{Key: "role", Value: &pb.AnyValue{StringValue: ptrTo("dev")}},
							{Key: "cluster_load_percent", Value: &pb.AnyValue{DoubleValue: ptrTo(0.55)}},
						},
					}}},
				},
			},
			ScopeLogs: []pb.ScopeLogs{
				{
					LogRecords: []pb.LogRecord{
						{TimeUnixNano: 1234, SeverityNumber: 1, Body: pb.AnyValue{StringValue: ptrTo("log-line-message")}},
						{TimeUnixNano: 1235, SeverityNumber: 5, Body: pb.AnyValue{StringValue: ptrTo("log-line-message-msg-2")}},
					},
				},
			},
		},
		{
			ScopeLogs: []pb.ScopeLogs{
				{
					LogRecords: []pb.LogRecord{
						{TimeUnixNano: 2345, SeverityNumber: 10, Body: pb.AnyValue{StringValue: ptrTo("log-line-resource-scope-1-0-0")}},
						{TimeUnixNano: 2346, SeverityNumber: 10, Body: pb.AnyValue{StringValue: ptrTo("log-line-resource-scope-1-0-1")}},
					},
				},
				{
					LogRecords: []pb.LogRecord{
						{TimeUnixNano: 2347, SeverityNumber: 12, Body: pb.AnyValue{StringValue: ptrTo("log-line-resource-scope-1-1-0")}},
						{TraceID: "1234", SpanID: "45", ObservedTimeUnixNano: 2348, SeverityNumber: 12, Body: pb.AnyValue{StringValue: ptrTo("log-line-resource-scope-1-1-1")}},
						{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7", ObservedTimeUnixNano: 3333, Body: pb.AnyValue{StringValue: ptrTo("log-line-resource-scope-1-1-2")}},
					},
				},
			},
		},
	},
		[]int64{1234, 1235, 2345, 2346, 2347, 2348, 3333},
		`{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","_msg":"log-line-message","severity":"Trace"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","_msg":"log-line-message-msg-2","severity":"Debug"}
{"_msg":"log-line-resource-scope-1-0-0","severity":"Info2"}
{"_msg":"log-line-resource-scope-1-0-1","severity":"Info2"}
{"_msg":"log-line-resource-scope-1-1-0","severity":"Info4"}
{"_msg":"log-line-resource-scope-1-1-1","trace_id":"1234","span_id":"45","severity":"Info4"}
{"_msg":"log-line-resource-scope-1-1-2","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","severity":"Unspecified"}`,
	)

	// nested fields
	f([]pb.ResourceLogs{
		{
			ScopeLogs: []pb.ScopeLogs{
				{
					LogRecords: []pb.LogRecord{
						{
							TimeUnixNano: 1234,
							Body:         pb.AnyValue{StringValue: ptrTo("nested fields")},
							Attributes: []*pb.KeyValue{
								{Key: "error", Value: &pb.AnyValue{KeyValueList: &pb.KeyValueList{Values: []*pb.KeyValue{
									{
										Key:   "type",
										Value: &pb.AnyValue{StringValue: ptrTo("document_parsing_exception")},
									},
									{
										Key:   "reason",
										Value: &pb.AnyValue{StringValue: ptrTo("failed to parse field [_msg] of type [text]")},
									},
									{
										Key: "caused_by",
										Value: &pb.AnyValue{KeyValueList: &pb.KeyValueList{Values: []*pb.KeyValue{
											{
												Key:   "type",
												Value: &pb.AnyValue{StringValue: ptrTo("x_content_parse_exception")},
											},
											{
												Key:   "reason",
												Value: &pb.AnyValue{StringValue: ptrTo("unexpected end-of-input in VALUE_STRING")},
											},
											{
												Key: "caused_by",
												Value: &pb.AnyValue{KeyValueList: &pb.KeyValueList{Values: []*pb.KeyValue{
													{
														Key:   "type",
														Value: &pb.AnyValue{StringValue: ptrTo("json_e_o_f_exception")},
													},
													{
														Key:   "reason",
														Value: &pb.AnyValue{StringValue: ptrTo("eof")},
													},
												}}},
											},
										}}},
									},
								}}}},
							},
						},
					},
				},
			},
		},
	}, []int64{1234},
		`{"_msg":"nested fields","error.type":"document_parsing_exception","error.reason":"failed to parse field [_msg] of type [text]",`+
			`"error.caused_by.type":"x_content_parse_exception","error.caused_by.reason":"unexpected end-of-input in VALUE_STRING",`+
			`"error.caused_by.caused_by.type":"json_e_o_f_exception","error.caused_by.caused_by.reason":"eof","severity":"Unspecified"}`)
}

func ptrTo[T any](s T) *T {
	return &s
}
