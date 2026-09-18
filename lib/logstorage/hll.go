package logstorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/axiomhq/hyperloglog"
	"github.com/cespare/xxhash/v2"
)

// count_uniq_hll wire / hash schema constants.
//
// Wire layout:
//
//	magic(4) | vlWireVersion(1) | algorithmID(1) | precision(1) | hashSchemaVersion(1) | payloadLen(varuint) | axiomPayload
const (
	hllPrecision         = 14
	hllWireVersion       = 1
	hllAlgorithmID       = 1 // HLL_V1 — VictoriaLogs semantics, not a third-party brand id
	hllHashSchemaVersion = 1

	// Conservative per-sketch query budget charged once on first init / non-empty import.
	// Covers dense registers (~16KiB), sparse containers, and merge/estimate temporaries.
	hllStateBudgetBytes = 64 << 10
)

var hllMagic = []byte("VLHL")

// Hash schema v1 domain tags. Changing any encoding requires bumping hllHashSchemaVersion.
const (
	hllDomainUnsigned  byte = 1
	hllDomainNegative  byte = 2
	hllDomainString    byte = 3
	hllDomainTimestamp byte = 4
	hllDomainTuple     byte = 5
)

// hllSketch wraps axiomhq/hyperloglog with VictoriaLogs hash schema and wire framing.
type hllSketch struct {
	sk *hyperloglog.Sketch
}

func (h *hllSketch) isEmpty() bool {
	return h == nil || h.sk == nil
}

func (h *hllSketch) ensureInit() int {
	if h.sk != nil {
		return 0
	}
	sk, err := hyperloglog.NewSketch(hllPrecision, true)
	if err != nil {
		logger.Panicf("BUG: cannot create HLL sketch with p=%d: %s", hllPrecision, err)
	}
	h.sk = sk
	return hllStateBudgetBytes
}

func (h *hllSketch) addHash(hash uint64) int {
	n := h.ensureInit()
	h.sk.InsertHash(hash)
	return n
}

func (h *hllSketch) merge(src *hllSketch) {
	if src.isEmpty() {
		return
	}
	if h.isEmpty() {
		h.ensureInit()
	}
	if err := h.sk.Merge(src.sk); err != nil {
		logger.Panicf("BUG: HLL merge failed: %s", err)
	}
	src.sk = nil
}

func (h *hllSketch) estimate() uint64 {
	if h.isEmpty() {
		return 0
	}
	return h.sk.Estimate()
}

func (h *hllSketch) appendState(dst []byte) []byte {
	dst = append(dst, hllMagic...)
	dst = append(dst, hllWireVersion, hllAlgorithmID, hllPrecision, hllHashSchemaVersion)

	if h.isEmpty() {
		return encoding.MarshalVarUint64(dst, 0)
	}
	payload, err := h.sk.AppendBinary(nil)
	if err != nil {
		logger.Panicf("BUG: cannot marshal HLL sketch: %s", err)
	}
	dst = encoding.MarshalVarUint64(dst, uint64(len(payload)))
	dst = append(dst, payload...)
	return dst
}

func (h *hllSketch) unmarshalState(src []byte) (int, error) {
	if len(src) < len(hllMagic)+4 {
		return 0, fmt.Errorf("hll state too short: %d bytes", len(src))
	}
	if !bytes.Equal(src[:len(hllMagic)], hllMagic) {
		return 0, fmt.Errorf("invalid hll magic")
	}
	src = src[len(hllMagic):]

	wireVersion := src[0]
	algorithmID := src[1]
	precision := src[2]
	hashSchema := src[3]
	src = src[4:]

	if wireVersion != hllWireVersion {
		return 0, fmt.Errorf("unsupported hll wire version %d; want %d", wireVersion, hllWireVersion)
	}
	if algorithmID != hllAlgorithmID {
		return 0, fmt.Errorf("unsupported hll algorithm id %d; want %d", algorithmID, hllAlgorithmID)
	}
	if precision != hllPrecision {
		return 0, fmt.Errorf("unsupported hll precision %d; want %d", precision, hllPrecision)
	}
	if hashSchema != hllHashSchemaVersion {
		return 0, fmt.Errorf("unsupported hll hash schema version %d; want %d", hashSchema, hllHashSchemaVersion)
	}

	payloadLen, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		return 0, fmt.Errorf("cannot read hll payload length")
	}
	src = src[nSize:]
	if uint64(len(src)) != payloadLen {
		return 0, fmt.Errorf("unexpected hll payload length; got %d; want %d", len(src), payloadLen)
	}
	// Empty payload is a no-op: local mode may import multiple remote states into the
	// same group processor, so never clear previously accumulated sketch data.
	if payloadLen == 0 {
		return 0, nil
	}

	sk, err := hyperloglog.NewSketch(hllPrecision, true)
	if err != nil {
		return 0, fmt.Errorf("cannot create hll sketch: %w", err)
	}
	if err := sk.UnmarshalBinary(src); err != nil {
		return 0, fmt.Errorf("cannot unmarshal axiom hll payload: %w", err)
	}

	// Merge into existing state so repeated importState for the same group key
	// (multiple vlstorage nodes) unions sketches instead of replacing them.
	var tmp hllSketch
	tmp.sk = sk
	stateSizeIncrease := 0
	if h.isEmpty() {
		stateSizeIncrease = h.ensureInit()
	}
	if err := h.sk.Merge(tmp.sk); err != nil {
		logger.Panicf("BUG: HLL merge during importState failed: %s", err)
	}
	return stateSizeIncrease, nil
}

func hllHashUnsigned(n uint64) uint64 {
	var buf [9]byte
	buf[0] = hllDomainUnsigned
	binary.BigEndian.PutUint64(buf[1:], n)
	return xxhash.Sum64(buf[:])
}

func hllHashNegative(n int64) uint64 {
	var buf [9]byte
	buf[0] = hllDomainNegative
	binary.BigEndian.PutUint64(buf[1:], uint64(n))
	return xxhash.Sum64(buf[:])
}

func hllHashTimestamp(ts int64) uint64 {
	var buf [9]byte
	buf[0] = hllDomainTimestamp
	binary.BigEndian.PutUint64(buf[1:], uint64(ts))
	return xxhash.Sum64(buf[:])
}

func hllHashString(v []byte) uint64 {
	buf := make([]byte, 0, 1+len(v)+binary.MaxVarintLen64)
	buf = append(buf, hllDomainString)
	buf = encoding.MarshalBytes(buf, v)
	return xxhash.Sum64(buf)
}

// hllHashGenericString maps a field value into hash-schema v1 domains.
// Leading zeros are accepted so numeric strings like "01" and "1" share the unsigned domain,
// matching the documented logical-value normalization for count_uniq_hll.
func hllHashGenericString(v string) uint64 {
	if n, ok := tryParseUint64AllowLeadingZeros(v); ok {
		return hllHashUnsigned(n)
	}
	if len(v) > 0 && v[0] == '-' {
		if n, ok := tryParseInt64AllowLeadingZeros(v); ok {
			// "-0" / "-00" normalize to unsigned zero, matching numeric zero.
			if n == 0 {
				return hllHashUnsigned(0)
			}
			return hllHashNegative(n)
		}
	}
	return hllHashString(bytesutil.ToUnsafeBytes(v))
}

// appendHLLCanonicalField appends a normalized encoding of v for multi-field tuples,
// so numeric strings like "01" and "1" collide the same way as in the single-field path.
func appendHLLCanonicalField(dst []byte, v string) []byte {
	if n, ok := tryParseUint64AllowLeadingZeros(v); ok {
		dst = append(dst, hllDomainUnsigned)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], n)
		return append(dst, b[:]...)
	}
	if len(v) > 0 && v[0] == '-' {
		if n, ok := tryParseInt64AllowLeadingZeros(v); ok {
			if n == 0 {
				dst = append(dst, hllDomainUnsigned)
				var b [8]byte
				return append(dst, b[:]...)
			}
			dst = append(dst, hllDomainNegative)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(n))
			return append(dst, b[:]...)
		}
	}
	dst = append(dst, hllDomainString)
	return encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(v))
}

func tryParseUint64AllowLeadingZeros(s string) (uint64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	n := uint64(0)
	digits := 0
	significant := false
	for i := range len(s) {
		ch := s[i]
		if ch == '_' {
			continue
		}
		if ch < '0' || ch > '9' {
			return 0, false
		}
		digits++
		if !significant {
			if ch == '0' {
				continue
			}
			significant = true
		}
		if n > ((1<<64)-1)/10 {
			return 0, false
		}
		n *= 10
		d := uint64(ch - '0')
		n1 := n + d
		if n1 < n {
			return 0, false
		}
		n = n1
	}
	return n, digits > 0
}

func tryParseInt64AllowLeadingZeros(s string) (int64, bool) {
	if len(s) == 0 || s[0] != '-' {
		return 0, false
	}
	n, ok := tryParseUint64AllowLeadingZeros(s[1:])
	if !ok {
		return 0, false
	}
	if n > 1<<63 {
		return 0, false
	}
	if n == 1<<63 {
		return math.MinInt64, true
	}
	return -int64(n), true
}

func hllHashTuple(keyBuf []byte) uint64 {
	buf := make([]byte, 0, 1+len(keyBuf))
	buf = append(buf, hllDomainTuple)
	buf = append(buf, keyBuf...)
	return xxhash.Sum64(buf)
}
