package opentelemetry

import (
	"encoding/base64"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/easyproto"
	"github.com/valyala/fastjson"
)

// decodeArrayValueToJSON decodes a protobuf ArrayValue message
// into a JSON array represented by fastjson.Value.
func decodeArrayValueToJSON(a *fastjson.Arena, src []byte) (_ *fastjson.Value, err error) {
	// message ArrayValue {
	//   repeated AnyValue values = 1;
	// }

	dst := a.NewArray()

	var fc easyproto.FieldContext
	for i := 0; len(src) > 0; i++ {
		src, err = fc.NextField(src)
		if err != nil {
			return nil, fmt.Errorf("cannot read next field in ArrayValue")
		}

		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return nil, fmt.Errorf("cannot read Value data")
			}

			v, err := decodeAnyValueToJSON(a, data)
			if err != nil {
				return nil, fmt.Errorf("cannot decode AnyValue: %s", err)
			}
			dst.SetArrayItem(i, v)
		}
	}

	return dst, nil
}

func decodeAnyValueToJSON(a *fastjson.Arena, src []byte) (_ *fastjson.Value, err error) {
	// message AnyValue {
	//   oneof value {
	//     string string_value = 1;
	//     bool bool_value = 2;
	//     int64 int_value = 3;
	//     double double_value = 4;
	//     ArrayValue array_value = 5;
	//     KeyValueList kvlist_value = 6;
	//     bytes bytes_value = 7;
	//   }
	// }

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return nil, fmt.Errorf("cannot read next field in AnyValue")
		}
		switch fc.FieldNum {
		case 1:
			stringValue, ok := fc.String()
			if !ok {
				return nil, fmt.Errorf("cannot read StringValue")
			}
			return a.NewString(stringValue), nil
		case 2:
			boolValue, ok := fc.Bool()
			if !ok {
				return nil, fmt.Errorf("cannot read BoolValue")
			}
			if boolValue {
				return a.NewTrue(), nil
			} else {
				return a.NewFalse(), nil
			}
		case 3:
			intValue, ok := fc.Int64()
			if !ok {
				return nil, fmt.Errorf("cannot read IntValue")
			}
			return a.NewNumberInt(int(intValue)), nil
		case 4:
			doubleValue, ok := fc.Double()
			if !ok {
				return nil, fmt.Errorf("cannot read DoubleValue")
			}
			return a.NewNumberFloat64(doubleValue), nil
		case 5:
			data, ok := fc.MessageData()
			if !ok {
				return nil, fmt.Errorf("cannot read ArrayValue")
			}
			arr, err := decodeArrayValueToJSON(a, data)
			if err != nil {
				return nil, fmt.Errorf("cannot decode ArrayValue: %s", err)
			}
			return arr, nil
		case 6:
			data, ok := fc.MessageData()
			if !ok {
				return nil, fmt.Errorf("cannot read KeyValueList")
			}
			obj, err := decodeKeyValueListToJSON(a, data)
			if err != nil {
				return nil, fmt.Errorf("cannot decode KeyValueList: %s", err)
			}
			return obj, nil
		case 7:
			bytesValue, ok := fc.Bytes()
			if !ok {
				return nil, fmt.Errorf("cannot read BytesValue")
			}
			b64 := base64.StdEncoding.EncodeToString(bytesValue)
			return a.NewString(b64), nil
		default:
			logger.Warnf("unsupported AnyValue type %d, please create an issue: https://github.com/VictoriaMetrics/VictoriaLogs/issues", fc.FieldNum)
		}
	}
	return nil, nil
}

func decodeKeyValueListToJSON(a *fastjson.Arena, src []byte) (_ *fastjson.Value, err error) {
	// message KeyValueList {
	//   repeated KeyValue values = 1;
	// }

	dst := a.NewObject()

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return nil, fmt.Errorf("cannot read next field in KeyValueList")
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return nil, fmt.Errorf("cannot read Value data")
			}

			if err := decodeKeyValueToJSON(a, dst, data); err != nil {
				return nil, fmt.Errorf("cannot decode KeyValue: %s", err)
			}
		}
	}
	return dst, nil
}

func decodeKeyValueToJSON(a *fastjson.Arena, dst *fastjson.Value, src []byte) (err error) {
	// message KeyValue {
	//   string key = 1;
	//   AnyValue value = 2;
	// }

	// Decode key
	data, ok, err := findMessageData(src, 1)
	if err != nil {
		return fmt.Errorf("cannot find Key in KeyValue: %s", err)
	}
	if !ok {
		return fmt.Errorf("key is missing in KeyValue")
	}
	fieldName := bytesutil.ToUnsafeString(data)

	// Decode value
	data, ok, err = findMessageData(src, 2)
	if err != nil {
		return fmt.Errorf("cannot find Value in KeyValue: %s", err)
	}
	if !ok {
		// Value is null, skip it.
		return nil
	}

	v, err := decodeAnyValueToJSON(a, data)
	if err != nil {
		return fmt.Errorf("cannot decode AnyValue: %s", err)
	}

	dst.Set(fieldName, v)

	return nil
}

var jsonArenaPool fastjson.ArenaPool
