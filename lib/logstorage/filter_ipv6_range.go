package logstorage

import (
	"fmt"
	"net/netip"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// filterIPv6Range matches the given ipv6 range [minValue..maxValue].
//
// Example LogsQL: `fieldName:ipv6_range(::1, ::2)`
type filterIPv6Range struct {
	fieldName string
	minValue  [16]byte
	maxValue  [16]byte
}

func (fr *filterIPv6Range) String() string {
	minValue := netip.AddrFrom16(fr.minValue).String()
	maxValue := netip.AddrFrom16(fr.maxValue).String()
	return fmt.Sprintf("%sipv6_range(%s, %s)", quoteFieldNameIfNeeded(fr.fieldName), minValue, maxValue)
}

func (fr *filterIPv6Range) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter(fr.fieldName)
}

func (fr *filterIPv6Range) matchRow(fields []Field) bool {
	v := getFieldValueByName(fields, fr.fieldName)
	return matchIPv6Range(v, fr.minValue, fr.maxValue)
}

func (fr *filterIPv6Range) applyToBlockResult(br *blockResult, bm *bitmap) {
	minValue := fr.minValue
	maxValue := fr.maxValue

	if ipv6Less(maxValue, minValue) {
		bm.resetBits()
		return
	}

	c := br.getColumnByName(fr.fieldName)
	if c.isConst {
		v := c.valuesEncoded[0]
		if !matchIPv6Range(v, minValue, maxValue) {
			bm.resetBits()
		}
		return
	}
	if c.isTime {
		bm.resetBits()
		return
	}

	switch c.valueType {
	case valueTypeString:
		values := c.getValues(br)
		bm.forEachSetBit(func(idx int) bool {
			v := values[idx]
			return matchIPv6Range(v, minValue, maxValue)
		})
	case valueTypeDict:
		bb := bbPool.Get()
		for _, v := range c.dictValues {
			c := byte(0)
			if matchIPv6Range(v, minValue, maxValue) {
				c = 1
			}
			bb.B = append(bb.B, c)
		}
		valuesEncoded := c.getValuesEncoded(br)
		bm.forEachSetBit(func(idx int) bool {
			n := valuesEncoded[idx][0]
			return bb.B[n] == 1
		})
		bbPool.Put(bb)
	case valueTypeUint8:
		bm.resetBits()
	case valueTypeUint16:
		bm.resetBits()
	case valueTypeUint32:
		bm.resetBits()
	case valueTypeUint64:
		bm.resetBits()
	case valueTypeInt64:
		bm.resetBits()
	case valueTypeFloat64:
		bm.resetBits()
	case valueTypeIPv4:
		bm.resetBits()
	case valueTypeTimestampISO8601:
		bm.resetBits()
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
}

func (fr *filterIPv6Range) applyToBlockSearch(bs *blockSearch, bm *bitmap) {
	fieldName := fr.fieldName
	minValue := fr.minValue
	maxValue := fr.maxValue

	if ipv6Less(maxValue, minValue) {
		bm.resetBits()
		return
	}

	v := bs.getConstColumnValue(fieldName)
	if v != "" {
		if !matchIPv6Range(v, minValue, maxValue) {
			bm.resetBits()
		}
		return
	}

	// Verify whether filter matches other columns
	ch := bs.getColumnHeader(fieldName)
	if ch == nil {
		// Fast path - there are no matching columns.
		bm.resetBits()
		return
	}

	switch ch.valueType {
	case valueTypeString:
		matchStringByIPv6Range(bs, ch, bm, minValue, maxValue)
	case valueTypeDict:
		matchValuesDictByIPv6Range(bs, ch, bm, minValue, maxValue)
	case valueTypeUint8:
		bm.resetBits()
	case valueTypeUint16:
		bm.resetBits()
	case valueTypeUint32:
		bm.resetBits()
	case valueTypeUint64:
		bm.resetBits()
	case valueTypeInt64:
		bm.resetBits()
	case valueTypeFloat64:
		bm.resetBits()
	case valueTypeIPv4:
		bm.resetBits()
	case valueTypeTimestampISO8601:
		bm.resetBits()
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
}

func matchValuesDictByIPv6Range(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue [16]byte) {
	bb := bbPool.Get()
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if matchIPv6Range(v, minValue, maxValue) {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	matchEncodedValuesDict(bs, ch, bm, bb.B)
	bbPool.Put(bb)
}

func matchStringByIPv6Range(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue [16]byte) {
	visitValues(bs, ch, bm, func(v string) bool {
		return matchIPv6Range(v, minValue, maxValue)
	})
}

func matchIPv6Range(s string, minValue, maxValue [16]byte) bool {
	ip, ok := tryParseIPv6(s)
	if !ok {
		return false
	}
	if ipv6Less(ip, minValue) || ipv6Less(maxValue, ip) {
		return false
	}
	return true
}

func ipv6Less(a, b [16]byte) bool {
	for i := 0; i < 16; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return false
}
