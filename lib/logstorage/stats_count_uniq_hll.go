package logstorage

import (
	"fmt"
	"strconv"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

type statsCountUniqHLL struct {
	fields []string
}

func (su *statsCountUniqHLL) String() string {
	return "count_uniq_hll(" + fieldNamesString(su.fields) + ")"
}

func (su *statsCountUniqHLL) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilters(su.fields)
}

func (su *statsCountUniqHLL) newStatsProcessor(a *chunkedAllocator) statsProcessor {
	return a.newStatsCountUniqHLLProcessor()
}

type statsCountUniqHLLProcessor struct {
	sketch hllSketch

	columnValues [][]string
	keyBuf       []byte
	tmpNum       int
}

func (sup *statsCountUniqHLLProcessor) updateStatsForAllRows(sf statsFunc, br *blockResult) int {
	su := sf.(*statsCountUniqHLL)

	if len(su.fields) == 1 {
		return sup.updateStatsForAllRowsSingleColumn(br, su.fields[0])
	}

	stateSizeIncrease := 0
	columnValues := sup.columnValues[:0]
	for _, f := range su.fields {
		c := br.getColumnByName(f)
		values := c.getValues(br)
		columnValues = append(columnValues, values)
	}
	sup.columnValues = columnValues

	keyBuf := sup.keyBuf[:0]
	for i := range br.rowsLen {
		seenKey := true
		for _, values := range columnValues {
			if i == 0 || values[i-1] != values[i] {
				seenKey = false
				break
			}
		}
		if seenKey {
			continue
		}

		allEmptyValues := true
		keyBuf = keyBuf[:0]
		for _, values := range columnValues {
			v := values[i]
			if v != "" {
				allEmptyValues = false
			}
			keyBuf = encoding.MarshalBytes(keyBuf, bytesutil.ToUnsafeBytes(v))
		}
		if allEmptyValues {
			continue
		}
		stateSizeIncrease += sup.addHash(hllHashTuple(keyBuf))
	}
	sup.keyBuf = keyBuf
	return stateSizeIncrease
}

func (sup *statsCountUniqHLLProcessor) updateStatsForRow(sf statsFunc, br *blockResult, rowIdx int) int {
	su := sf.(*statsCountUniqHLL)

	if len(su.fields) == 1 {
		return sup.updateStatsForRowSingleColumn(br, su.fields[0], rowIdx)
	}

	allEmptyValues := true
	keyBuf := sup.keyBuf[:0]
	for _, f := range su.fields {
		c := br.getColumnByName(f)
		v := c.getValueAtRow(br, rowIdx)
		if v != "" {
			allEmptyValues = false
		}
		keyBuf = encoding.MarshalBytes(keyBuf, bytesutil.ToUnsafeBytes(v))
	}
	sup.keyBuf = keyBuf
	if allEmptyValues {
		return 0
	}
	return sup.addHash(hllHashTuple(keyBuf))
}

func (sup *statsCountUniqHLLProcessor) updateStatsForAllRowsSingleColumn(br *blockResult, columnName string) int {
	stateSizeIncrease := 0
	c := br.getColumnByName(columnName)
	if c.isTime {
		timestamps := br.getTimestamps()
		for i := range timestamps {
			if i > 0 && timestamps[i-1] == timestamps[i] {
				continue
			}
			stateSizeIncrease += sup.addHash(hllHashTimestamp(timestamps[i]))
		}
		return stateSizeIncrease
	}
	if c.isConst {
		v := c.valuesEncoded[0]
		if v == "" {
			return 0
		}
		return sup.addHash(hllHashGenericString(v))
	}

	switch c.valueType {
	case valueTypeDict:
		sup.tmpNum = 0
		c.forEachDictValue(br, func(v string) {
			if v == "" {
				return
			}
			sup.tmpNum += sup.addHash(hllHashGenericString(v))
		})
		return sup.tmpNum
	case valueTypeUint8:
		values := c.getValuesEncoded(br)
		for i, v := range values {
			if i > 0 && values[i-1] == v {
				continue
			}
			stateSizeIncrease += sup.addHash(hllHashUnsigned(uint64(unmarshalUint8(v))))
		}
		return stateSizeIncrease
	case valueTypeUint16:
		values := c.getValuesEncoded(br)
		for i, v := range values {
			if i > 0 && values[i-1] == v {
				continue
			}
			stateSizeIncrease += sup.addHash(hllHashUnsigned(uint64(unmarshalUint16(v))))
		}
		return stateSizeIncrease
	case valueTypeUint32:
		values := c.getValuesEncoded(br)
		for i, v := range values {
			if i > 0 && values[i-1] == v {
				continue
			}
			stateSizeIncrease += sup.addHash(hllHashUnsigned(uint64(unmarshalUint32(v))))
		}
		return stateSizeIncrease
	case valueTypeUint64:
		values := c.getValuesEncoded(br)
		for i, v := range values {
			if i > 0 && values[i-1] == v {
				continue
			}
			stateSizeIncrease += sup.addHash(hllHashUnsigned(unmarshalUint64(v)))
		}
		return stateSizeIncrease
	case valueTypeInt64:
		values := c.getValuesEncoded(br)
		for i, v := range values {
			if i > 0 && values[i-1] == v {
				continue
			}
			n := unmarshalInt64(v)
			if n >= 0 {
				stateSizeIncrease += sup.addHash(hllHashUnsigned(uint64(n)))
			} else {
				stateSizeIncrease += sup.addHash(hllHashNegative(n))
			}
		}
		return stateSizeIncrease
	default:
		values := c.getValues(br)
		for i, v := range values {
			if v == "" {
				continue
			}
			if i > 0 && values[i-1] == v {
				continue
			}
			stateSizeIncrease += sup.addHash(hllHashGenericString(v))
		}
		return stateSizeIncrease
	}
}

func (sup *statsCountUniqHLLProcessor) updateStatsForRowSingleColumn(br *blockResult, columnName string, rowIdx int) int {
	c := br.getColumnByName(columnName)
	if c.isTime {
		timestamps := br.getTimestamps()
		return sup.addHash(hllHashTimestamp(timestamps[rowIdx]))
	}
	if c.isConst {
		v := c.valuesEncoded[0]
		if v == "" {
			return 0
		}
		return sup.addHash(hllHashGenericString(v))
	}

	switch c.valueType {
	case valueTypeDict:
		valuesEncoded := c.getValuesEncoded(br)
		dictIdx := valuesEncoded[rowIdx][0]
		v := c.dictValues[dictIdx]
		if v == "" {
			return 0
		}
		return sup.addHash(hllHashGenericString(v))
	case valueTypeUint8:
		v := c.getValuesEncoded(br)[rowIdx]
		return sup.addHash(hllHashUnsigned(uint64(unmarshalUint8(v))))
	case valueTypeUint16:
		v := c.getValuesEncoded(br)[rowIdx]
		return sup.addHash(hllHashUnsigned(uint64(unmarshalUint16(v))))
	case valueTypeUint32:
		v := c.getValuesEncoded(br)[rowIdx]
		return sup.addHash(hllHashUnsigned(uint64(unmarshalUint32(v))))
	case valueTypeUint64:
		v := c.getValuesEncoded(br)[rowIdx]
		return sup.addHash(hllHashUnsigned(unmarshalUint64(v)))
	case valueTypeInt64:
		v := c.getValuesEncoded(br)[rowIdx]
		n := unmarshalInt64(v)
		if n >= 0 {
			return sup.addHash(hllHashUnsigned(uint64(n)))
		}
		return sup.addHash(hllHashNegative(n))
	default:
		v := c.getValueAtRow(br, rowIdx)
		if v == "" {
			return 0
		}
		return sup.addHash(hllHashGenericString(v))
	}
}

func (sup *statsCountUniqHLLProcessor) addHash(h uint64) int {
	return sup.sketch.addHash(h)
}

func (sup *statsCountUniqHLLProcessor) mergeState(_ *chunkedAllocator, _ statsFunc, sfp statsProcessor) {
	src := sfp.(*statsCountUniqHLLProcessor)
	sup.sketch.merge(&src.sketch)
}

func (sup *statsCountUniqHLLProcessor) exportState(dst []byte, _ <-chan struct{}) []byte {
	return sup.sketch.appendState(dst)
}

func (sup *statsCountUniqHLLProcessor) importState(src []byte, _ <-chan struct{}) (int, error) {
	return sup.sketch.unmarshalState(src)
}

func (sup *statsCountUniqHLLProcessor) finalizeStats(_ statsFunc, dst []byte, _ <-chan struct{}) []byte {
	return strconv.AppendUint(dst, sup.sketch.estimate(), 10)
}

func parseStatsCountUniqHLL(lex *lexer) (statsFunc, error) {
	fields, err := parseStatsFuncFields(lex, "count_uniq_hll")
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("expecting at least a single field")
	}
	return &statsCountUniqHLL{
		fields: fields,
	}, nil
}
