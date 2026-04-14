package loki

import (
	"time"

	"github.com/VictoriaMetrics/easyproto"
)

var mp easyproto.MarshalerPool

// pushRequest represents Loki PushRequest
//
// See https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L14
type pushRequest struct {
	Streams []stream
}

// MarshalProtobuf marshals pr to protobuf message, appends it to dst and returns the result.
func (pr *pushRequest) MarshalProtobuf(dst []byte) []byte {
	m := mp.Get()
	pr.marshalProtobuf(m.MessageMarshaler())
	dst = m.Marshal(dst)
	mp.Put(m)
	return dst
}

func (pr *pushRequest) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	for _, s := range pr.Streams {
		s.marshalProtobuf(mm.AppendMessage(1))
	}
}

// stream represents Loki stream.
//
// See https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L23
type stream struct {
	Labels  string
	Entries []entry
}

func (s *stream) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendString(1, s.Labels)
	for _, e := range s.Entries {
		e.marshalProtobuf(mm.AppendMessage(2))
	}
}

// entry represents Loki entry.
//
// See https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L38
type entry struct {
	Timestamp          time.Time
	Line               string
	StructuredMetadata []labelPair
}

func (e *entry) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	marshalTime(mm, 1, e.Timestamp)
	mm.AppendString(2, e.Line)
	for _, lp := range e.StructuredMetadata {
		lp.marshalProtobuf(mm.AppendMessage(3))
	}
}

// labelPair represents Loki label pair.
//
// See https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L33
type labelPair struct {
	Name  string
	Value string
}

func (lp *labelPair) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendString(1, lp.Name)
	mm.AppendString(2, lp.Value)
}

func marshalTime(mm *easyproto.MessageMarshaler, fieldNum uint32, t time.Time) {
	nsecs := t.UnixNano()
	ts := timestamp{
		Seconds: nsecs / 1e9,
		Nanos:   int32(nsecs % 1e9),
	}
	ts.marshalProtobuf(mm.AppendMessage(fieldNum))
}

// timestamp is protobuf well-known timestamp type.
type timestamp struct {
	Seconds int64
	Nanos   int32
}

func (ts *timestamp) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendInt64(1, ts.Seconds)
	mm.AppendInt32(2, ts.Nanos)
}
