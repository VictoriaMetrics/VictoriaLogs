package opentelemetry

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/easyproto"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
)

func TestPushProtobufRequest(t *testing.T) {
	f := func(src string, timestampsExpected []int64, resultExpected string) {
		t.Helper()

		var rls []resourceLogs
		dec := json.NewDecoder(strings.NewReader(src))
		// Throw an error if there are unknown fields in the JSON.
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rls); err != nil {
			t.Fatalf("unexpected error when parsing JSON: %s", err)
		}

		lr := logsData{
			ResourceLogs: rls,
		}

		pData := lr.marshalProtobuf(nil)
		tlp := &insertutil.TestLogMessageProcessor{}
		if err := pushProtobufRequest(pData, tlp, nil, false); err != nil {
			t.Fatalf("unexpected error when parsing protobuf data: %s", err)
		}

		if err := tlp.Verify(timestampsExpected, resultExpected); err != nil {
			t.Fatal(err)
		}
	}

	// single line without resource attributes
	data := `[{
		"scopeLogs": [{
			"logRecords": [{
				"timeUnixNano": 1234,
				"severityNumber": 1,
				"body": {
					"stringValue": "log-line-message"
				}
			}]
		}]
	}]`
	timestampsExpected := []int64{1234}
	resultsExpected := `{"_msg":"log-line-message","severity":"Trace"}`
	f(data, timestampsExpected, resultsExpected)

	// single line with scope attributes
	data = `[{
		"scopeLogs": [{
			"scope": {
				"name": "foo",
				"version": "v1.234.5",
				"attributes": [
					{"key":"abc","value":{"stringValue":"de"}},
					{"key":"x","value":{"stringValue":"aaa"}}
				]
			},
			"logRecords": [{
				"timeUnixNano": 1234,
				"severityNumber": 1,
				"body": {
					"stringValue": "log-line-message"
				}
			}]
		}]
	}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"scope.name":"foo","scope.version":"v1.234.5","scope.attributes.abc":"de","scope.attributes.x":"aaa","_msg":"log-line-message","severity":"Trace"}`
	f(data, timestampsExpected, resultsExpected)

	// severities mapping
	data = `[{
		"scopeLogs": [{
			"logRecords": [
				{"timeUnixNano":1234,"severityNumber":1,"body":{"stringValue":"log-line-message"}},
				{"timeUnixNano":1235,"severityNumber":13,"body":{"stringValue":"log-line-message"}},
				{"timeUnixNano":1236,"severityNumber":24,"body":{"stringValue":"log-line-message"}}
			]
		}]
	}]`
	timestampsExpected = []int64{1234, 1235, 1236}
	resultsExpected = `{"_msg":"log-line-message","severity":"Trace"}
{"_msg":"log-line-message","severity":"Warn"}
{"_msg":"log-line-message","severity":"Fatal4"}`
	f(data, timestampsExpected, resultsExpected)

	// multi-line with resource attributes
	data = `[{
		"resource": {
			"attributes": [
				{"key":"logger","value":{"stringValue":"context"}},
				{"key":"instance_id","value":{"intValue":10}},
				{"key":"","value":{"stringValue":"missing-key"}},
				{"key":"missing-value","value":{"stringValue":""}},
				{"key":"node_taints","value":{"keyValueList":{"values":
					[{"key":"role","value":{"stringValue":"dev"}},{"key":"cluster_load_percent","value":{"doubleValue":0.55}}]
				}}}
			]
		},
		"scopeLogs": [{
			"scope": {
				"name": "foo",
				"attributes": [
					{"key":"x","value":{"stringValue":"aaa"}}
				]
			},
			"logRecords": [
				{"timeUnixNano":1234,"severityNumber":1,"body":{"intValue":833}},
				{"timeUnixNano":1235,"severityNumber":25,"body":{"stringValue":"log-line-message-msg-2"}},
				{"timeUnixNano":1236,"severityNumber":-1,"body":{"stringValue":"log-line-message-msg-3"}},
				{"timeUnixNano":1237,"eventName":"abc","body":{"intValue":10}}
			]
		}]
	}]`
	timestampsExpected = []int64{1234, 1235, 1236, 1237}
	resultsExpected = `{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"foo","scope.version":"unknown","scope.attributes.x":"aaa","_msg":"833","severity":"Trace"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"foo","scope.version":"unknown","scope.attributes.x":"aaa","_msg":"log-line-message-msg-2","severity":"Unspecified"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"foo","scope.version":"unknown","scope.attributes.x":"aaa","_msg":"log-line-message-msg-3","severity":"Unspecified"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","event_name":"abc","scope.name":"foo","scope.version":"unknown","scope.attributes.x":"aaa","_msg":"10","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// multi-scope with resource attributes and multi-line
	data = `[{
		"resource": {
			"attributes": [
				{"key":"logger","value":{"stringValue":"context"}},
				{"key":"instance_id","value":{"intValue":10}},
				{"key":"node_taints","value":{"keyValueList":{"values":[
					{"key":"role","value":{"stringValue":"dev"}},
					{"key":"cluster_load_percent","value":{"doubleValue":0.55}}
				]}}}
			]
		},
		"scopeLogs": [{
			"scope": {
				"attributes": [
					{"key":"abc","value":{"stringValue":"de"}}
				]
			},
			"logRecords": [
				{"timeUnixNano":1234,"severityNumber":1,"body":{"stringValue":"log-line-message"}},
				{"timeUnixNano":1235,"severityNumber":5,"body":{"stringValue":"log-line-message-msg-2"}}
			]
		}]
	},{
		"scopeLogs": [
			{
				"logRecords": [
					{"timeUnixNano":2345,"severityNumber":10,"body":{"stringValue":"log-line-resource-scope-1-0-0"}},
					{"timeUnixNano":2346,"severityNumber":10,"body":{"stringValue":"log-line-resource-scope-1-0-1"}}
				]
			},{
				"logRecords": [
					{"timeUnixNano":2347,"severityNumber":12,"body":{"stringValue":"log-line-resource-scope-1-1-0"}},
					{"observedTimeUnixNano":2348,"severityNumber":12,"body":{"stringValue":"log-line-resource-scope-1-1-1"},"traceID":"1234","spanID":"45"},
					{"observedTimeUnixNano":3333,"body":{"stringValue":"log-line-resource-scope-1-1-2"},"traceID":"4bf92f3577b34da6a3ce929d0e0e4736","spanID":"00f067aa0ba902b7"},
					{"timeUnixNano":432,"body":{"stringValue":"abcd"},"eventName":"foobar"}
				]
			}
		]
	}]`
	timestampsExpected = []int64{1234, 1235, 2345, 2346, 2347, 2348, 3333, 432}
	resultsExpected = `{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"unknown","scope.version":"unknown","scope.attributes.abc":"de","_msg":"log-line-message","severity":"Trace"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"unknown","scope.version":"unknown","scope.attributes.abc":"de","_msg":"log-line-message-msg-2","severity":"Debug"}
{"_msg":"log-line-resource-scope-1-0-0","severity":"Info2"}
{"_msg":"log-line-resource-scope-1-0-1","severity":"Info2"}
{"_msg":"log-line-resource-scope-1-1-0","severity":"Info4"}
{"_msg":"log-line-resource-scope-1-1-1","trace_id":"1234","span_id":"45","severity":"Info4"}
{"_msg":"log-line-resource-scope-1-1-2","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","severity":"Unspecified"}
{"event_name":"foobar","_msg":"abcd","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// nested fields
	data = `[{
		"scopeLogs": [{
			"logRecords": [{
				"timeUnixNano": 1234,
				"body": {
					"stringValue": "nested fields"
				},
				"attributes": [{
					"key": "error",
					"value": {
						"keyValueList": {
							"values": [{
								"key": "type",
								"value": {
									"stringValue": "document_parsing_exception"
								}
							}, {
								"key": "missing-value",
								"value": {
									"stringValue": ""
								}
							}, {
								"key": "",
								"value": {
									"stringValue": "missing-key"
								}
							}, {
								"key": "reason",
								"value": {
									"stringValue": "failed to parse field [_msg] of type [text]"
								}
							}, {
								"key": "caused_by",
								"value": {
									"keyValueList": {
										"values": [{
											"key": "type",
											"value": {
												"stringValue": "x_content_parse_exception"
											}
										}, {
											"key": "reason",
											"value": {
												"stringValue": "unexpected end-of-input in VALUE_STRING"
											}
										}, {
											"key": "caused_by",
											"value": {
												"keyValueList": {
													"values": [{
														"key": "type",
														"value": {
															"stringValue": "json_e_o_f_exception"
														}
													}, {
														"key": "reason",
														"value": {
															"stringValue": "eof"
														}
													}]
												}
											}
										}]
									}
								}
							}]
						}
					}
				}]
			}]
		}]
	}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"nested fields","error.type":"document_parsing_exception","error.reason":"failed to parse field [_msg] of type [text]",` +
		`"error.caused_by.type":"x_content_parse_exception","error.caused_by.reason":"unexpected end-of-input in VALUE_STRING",` +
		`"error.caused_by.caused_by.type":"json_e_o_f_exception","error.caused_by.caused_by.reason":"eof","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode BytesValue
	data = `[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"bytesValue":"Zm9vIGJhcg=="}}]}]}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"Zm9vIGJhcg==","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode KeyValueList
	data = `[{
		"scopeLogs": [{
			"logRecords": [{
				"timeUnixNano": 1234,
				"body": {
					"keyValueList": {
						"values": [{
							"key": "foo",
							"value": {
								"stringValue": "bar"
							}
						}, {
							"key": "bar",
							"value": {
								"stringValue": "buz"
							}
						}]
					}
				}
			}]
		}]
	}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"foo":"bar","bar":"buz","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode BoolValue
	data = `[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"boolValue":true}}]}]}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"true","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode StringValue of ArrayValue
	data = `[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"stringValue":"foo bar"}]}}}]}]}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"[\"foo bar\"]","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode ArrayValue of ArrayValue
	data = `[{
		"scopeLogs": [{
			"logRecords": [{
				"timeUnixNano": 1234,
				"body": {
					"arrayValue": {
						"values": [{
							"arrayValue": {
								"values": [{
									"stringValue": "foo"
								}]
							}
						}, {
							"arrayValue": {
								"values": [{
									"stringValue": "bar"
								}]
							}
						}, {
							"arrayValue": {
								"values": [{
									"stringValue": "buz"
								}]
							}
						}]
					}
				}
			}]
		}]
	}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"[[\"foo\"],[\"bar\"],[\"buz\"]]","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode BoolValue of ArrayValue
	data = `[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"boolValue":true}]}}}]}]}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"[true]","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode IntValue of ArrayValue
	data = `[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"intValue":123}]}}}]}]}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"[123]","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode DoubleValue of ArrayValue
	data = `[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"doubleValue":123.45}]}}}]}]}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"[123.45]","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode KeyValueList of ArrayValue
	data = `[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"keyValueList":{"values":[{"key":"foo","value":{"stringValue":"bar"}}]}}]}}}]}]}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"[{\"foo\":\"bar\"}]","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode bytes of ArrayValue
	data = `[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"bytesValue":"Zm9vIGJhcg=="}]}}}]}]}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"[\"Zm9vIGJhcg==\"]","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)

	// decode null in ArrayValue
	data = `[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[null,{}]}}}]}]}]`
	timestampsExpected = []int64{1234}
	resultsExpected = `{"_msg":"[null,null]","severity":"Unspecified"}`
	f(data, timestampsExpected, resultsExpected)
}

var mp easyproto.MarshalerPool

// logsData represents the corresponding OTEL protobuf message.
type logsData struct {
	ResourceLogs []resourceLogs `json:"resourceLogs,omitzero"`
}

// MarshalProtobuf marshals r to a protobuf message, appends it to dst and returns the result.
func (r *logsData) marshalProtobuf(dst []byte) []byte {
	m := mp.Get()
	r.marshalProtobufInternal(m.MessageMarshaler())
	dst = m.Marshal(dst)
	mp.Put(m)
	return dst
}

func (r *logsData) marshalProtobufInternal(mm *easyproto.MessageMarshaler) {
	for _, rm := range r.ResourceLogs {
		rm.marshalProtobuf(mm.AppendMessage(1))
	}
}

// resourceLogs represents the corresponding OTEL protobuf message.
type resourceLogs struct {
	Resource  resource    `json:"resource,omitzero"`
	ScopeLogs []scopeLogs `json:"scopeLogs,omitzero"`
}

func (rl *resourceLogs) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	rl.Resource.marshalProtobuf(mm.AppendMessage(1))
	for _, sm := range rl.ScopeLogs {
		sm.marshalProtobuf(mm.AppendMessage(2))
	}
}

// resource represents the corresponding OTEL protobuf message.
type resource struct {
	Attributes []*keyValue `json:"attributes,omitzero"`
}

// marshalProtobuf marshals
func (r *resource) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	for _, a := range r.Attributes {
		a.marshalProtobuf(mm.AppendMessage(1))
	}
}

// keyValue represents the corresponding OTEL protobuf message.
type keyValue struct {
	Key   string    `json:"key,omitzero"`
	Value *anyValue `json:"value,omitzero"`
}

func (kv *keyValue) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	if kv.Key != "" {
		mm.AppendString(1, kv.Key)
	}
	if kv.Value != nil {
		kv.Value.marshalProtobuf(mm.AppendMessage(2))
	}
}

// anyValue represents the corresponding OTEL protobuf message.
type anyValue struct {
	StringValue  *string       `json:"stringValue,omitzero"`
	BoolValue    *bool         `json:"boolValue,omitzero"`
	IntValue     *int64        `json:"intValue,omitzero"`
	DoubleValue  *float64      `json:"doubleValue,omitzero"`
	ArrayValue   *arrayValue   `json:"arrayValue,omitzero"`
	KeyValueList *keyValueList `json:"keyValueList,omitzero"`
	BytesValue   *[]byte       `json:"bytesValue,omitzero"`
}

func (av *anyValue) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	if av == nil {
		return
	}
	switch {
	case av.StringValue != nil:
		mm.AppendString(1, *av.StringValue)
	case av.BoolValue != nil:
		mm.AppendBool(2, *av.BoolValue)
	case av.IntValue != nil:
		mm.AppendInt64(3, *av.IntValue)
	case av.DoubleValue != nil:
		mm.AppendDouble(4, *av.DoubleValue)
	case av.ArrayValue != nil:
		av.ArrayValue.marshalProtobuf(mm.AppendMessage(5))
	case av.KeyValueList != nil:
		av.KeyValueList.marshalProtobuf(mm.AppendMessage(6))
	case av.BytesValue != nil:
		mm.AppendBytes(7, *av.BytesValue)
	}
}

// arrayValue represents the corresponding OTEL protobuf message.
type arrayValue struct {
	Values []*anyValue `json:"values,omitzero"`
}

func (av *arrayValue) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	for _, v := range av.Values {
		v.marshalProtobuf(mm.AppendMessage(1))
	}
}

// keyValueList represents the corresponding OTEL protobuf message.
type keyValueList struct {
	Values []*keyValue `json:"values,omitzero"`
}

func (kvl *keyValueList) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	for _, v := range kvl.Values {
		v.marshalProtobuf(mm.AppendMessage(1))
	}
}

// scopeLogs represents the corresponding OTEL protobuf message
type scopeLogs struct {
	Scope      *instrumentationScope `json:"scope,omitzero"`
	LogRecords []logRecord           `json:"logRecords,omitzero"`
}

func (sl *scopeLogs) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	if sl.Scope != nil {
		sl.Scope.marshalProtobuf(mm.AppendMessage(1))
	}
	for _, m := range sl.LogRecords {
		m.marshalProtobuf(mm.AppendMessage(2))
	}
}

// instrumentationScope represents the corresponding OTEL protobuf message.
// See https://github.com/open-telemetry/opentelemetry-proto/blob/a5f0eac5b802f7ae51dfe41e5116fe5548955e64/opentelemetry/proto/common/v1/common.proto#L76
type instrumentationScope struct {
	Name       string      `json:"name,omitzero"`
	Version    string      `json:"version,omitzero"`
	Attributes []*keyValue `json:"attributes,omitzero"`
}

func (s *instrumentationScope) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	if s.Name != "" {
		mm.AppendString(1, s.Name)
	}
	if s.Version != "" {
		mm.AppendString(2, s.Version)
	}
	for _, m := range s.Attributes {
		m.marshalProtobuf(mm.AppendMessage(3))
	}
}

// logRecord represents the corresponding OTEL protobuf message.
// See https://github.com/open-telemetry/oteps/blob/main/text/logs/0097-log-data-model.md
type logRecord struct {
	// time_unix_nano is the time when the event occurred.
	// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
	// Value of 0 indicates unknown or missing timestamp.
	TimeUnixNano uint64 `json:"timeUnixNano,omitzero"`
	// Time when the event was observed by the collection system.
	// For events that originate in OpenTelemetry (e.g. using OpenTelemetry Logging SDK)
	// this timestamp is typically set at the generation time and is equal to Timestamp.
	// For events originating externally and collected by OpenTelemetry (e.g. using
	// Collector) this is the time when OpenTelemetry's code observed the event measured
	// by the clock of the OpenTelemetry code. This field MUST be set once the event is
	// observed by OpenTelemetry.
	//
	// For converting OpenTelemetry log data to formats that support only one timestamp or
	// when receiving OpenTelemetry log data by recipients that support only one timestamp
	// internally the following logic is recommended:
	//   - Use time_unix_nano if it is present, otherwise use observed_time_unix_nano.
	//
	// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
	// Value of 0 indicates unknown or missing timestamp.
	ObservedTimeUnixNano uint64 `json:"observedTimeUnixNano,omitzero"`
	// Numerical value of the severity, normalized to values described in Log Data Model.
	SeverityNumber int32       `json:"severityNumber,omitzero"`
	SeverityText   string      `json:"severityText,omitzero"`
	Body           anyValue    `json:"body"`
	Attributes     []*keyValue `json:"attributes,omitzero"`
	TraceID        string      `json:"traceID,omitzero"`
	SpanID         string      `json:"spanID,omitzero"`
	EventName      string      `json:"eventName,omitzero"`
}

func (lr *logRecord) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendFixed64(1, lr.TimeUnixNano)
	mm.AppendInt32(2, lr.SeverityNumber)
	mm.AppendString(3, lr.SeverityText)
	lr.Body.marshalProtobuf(mm.AppendMessage(5))
	for _, a := range lr.Attributes {
		a.marshalProtobuf(mm.AppendMessage(6))
	}

	traceID, err := hex.DecodeString(lr.TraceID)
	if err != nil {
		traceID = []byte(lr.TraceID)
	}
	mm.AppendBytes(9, traceID)

	spanID, err := hex.DecodeString(lr.SpanID)
	if err != nil {
		spanID = []byte(lr.SpanID)
	}
	mm.AppendBytes(10, spanID)

	mm.AppendFixed64(11, lr.ObservedTimeUnixNano)

	mm.AppendString(12, lr.EventName)
}
