package opentelemetry

import (
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/valyala/fastjson"
)

type fmtBuffer struct {
	buf []byte
}

var fmtBufferPool sync.Pool

func getFmtBuffer() *fmtBuffer {
	v := fmtBufferPool.Get()
	if v == nil {
		return &fmtBuffer{}
	}
	return v.(*fmtBuffer)
}

func putFmtBuffer(fb *fmtBuffer) {
	fb.reset()
	fmtBufferPool.Put(fb)
}

func (fb *fmtBuffer) reset() {
	fb.buf = fb.buf[:0]
}

func (fb *fmtBuffer) formatInt(v int64) string {
	n := len(fb.buf)
	fb.buf = strconv.AppendInt(fb.buf, v, 10)
	return bytesutil.ToUnsafeString(fb.buf[n:])
}

func (fb *fmtBuffer) formatFloat(v float64) string {
	n := len(fb.buf)
	fb.buf = strconv.AppendFloat(fb.buf, v, 'f', -1, 64)
	return bytesutil.ToUnsafeString(fb.buf[n:])
}

func (fb *fmtBuffer) formatSubFieldName(prefix, suffix string) string {
	if prefix == "" {
		// There is no prefix, so just return the suffix as is.
		return suffix
	}

	n := len(fb.buf)
	fb.buf = append(fb.buf, prefix...)
	fb.buf = append(fb.buf, '.')
	fb.buf = append(fb.buf, suffix...)

	return bytesutil.ToUnsafeString(fb.buf[n:])
}

func (fb *fmtBuffer) formatHex(src []byte) string {
	n := len(fb.buf)
	fb.buf = hex.AppendEncode(fb.buf, src)
	return bytesutil.ToUnsafeString(fb.buf[n:])
}

func (fb *fmtBuffer) formatBase64(src []byte) string {
	n := len(fb.buf)
	fb.buf = base64.StdEncoding.AppendEncode(fb.buf, src)
	return bytesutil.ToUnsafeString(fb.buf[n:])
}

func (fb *fmtBuffer) encodeJSONValue(v *fastjson.Value) string {
	n := len(fb.buf)
	fb.buf = v.MarshalTo(fb.buf)
	return bytesutil.ToUnsafeString(fb.buf[n:])
}
