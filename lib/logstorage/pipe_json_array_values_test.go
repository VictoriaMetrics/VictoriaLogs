package logstorage

import (
	"testing"
)

func TestParsePipeJSONArrayValuesSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`json_array_values message`)
	f(`json_array_values message from parts`)
	f(`json_array_values message as messages`)
	f(`json_array_values message from parts as messages`)
}

func TestParsePipeJSONArrayValuesFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`json_array_values`)
	f(`json_array_values *`)
	f(`json_array_values msg*`)
	f(`json_array_values message from`)
	f(`json_array_values message from *`)
	f(`json_array_values message from parts as *`)
	f(`json_array_values message from parts as messages*`)
	f(`json_array_values message from parts as messages extra`)
}

func TestPipeJSONArrayValues(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// Happy path - the example from issue #1370.
	f(`json_array_values message from parts as messages`, [][]Field{
		{
			{"app", "vlagent"},
			{"parts", `[{"ts":"2026-04-30T10:00:00Z","message":"failed to connect"},{"ts":"2026-04-30T10:00:01Z","message":" to remote storage"},{"ts":"2026-04-30T10:00:02Z","message":": timeout"}]`},
		},
	}, [][]Field{
		{
			{"app", "vlagent"},
			{"parts", `[{"ts":"2026-04-30T10:00:00Z","message":"failed to connect"},{"ts":"2026-04-30T10:00:01Z","message":" to remote storage"},{"ts":"2026-04-30T10:00:02Z","message":": timeout"}]`},
			{"messages", `["failed to connect"," to remote storage",": timeout"]`},
		},
	})

	// Source field missing -> empty array.
	f(`json_array_values message from parts as messages`, [][]Field{
		{
			{"app", "vlagent"},
		},
	}, [][]Field{
		{
			{"app", "vlagent"},
			{"messages", `[]`},
		},
	})

	// Source field is not a JSON array -> empty array.
	f(`json_array_values message from parts as messages`, [][]Field{
		{
			{"parts", `not-json`},
		},
		{
			{"parts", `{"message":"hello"}`},
		},
		{
			{"parts", `[invalid`},
		},
	}, [][]Field{
		{
			{"parts", `not-json`},
			{"messages", `[]`},
		},
		{
			{"parts", `{"message":"hello"}`},
			{"messages", `[]`},
		},
		{
			{"parts", `[invalid`},
			{"messages", `[]`},
		},
	})

	// Some array elements lack the requested field -> they're skipped.
	f(`json_array_values message from parts as messages`, [][]Field{
		{
			{"parts", `[{"message":"a"},{"other":"x"},{"message":"b"}]`},
		},
	}, [][]Field{
		{
			{"parts", `[{"message":"a"},{"other":"x"},{"message":"b"}]`},
			{"messages", `["a","b"]`},
		},
	})

	// Non-object array elements are skipped.
	f(`json_array_values message from parts as messages`, [][]Field{
		{
			{"parts", `[{"message":"a"},"plain",42,null,{"message":"b"}]`},
		},
	}, [][]Field{
		{
			{"parts", `[{"message":"a"},"plain",42,null,{"message":"b"}]`},
			{"messages", `["a","b"]`},
		},
	})

	// Non-string field values - numbers, booleans, nested objects, nested arrays - preserved as JSON.
	f(`json_array_values v from parts as values`, [][]Field{
		{
			{"parts", `[{"v":1},{"v":2.5},{"v":true},{"v":null},{"v":{"k":"x"}},{"v":[1,2]}]`},
		},
	}, [][]Field{
		{
			{"parts", `[{"v":1},{"v":2.5},{"v":true},{"v":null},{"v":{"k":"x"}},{"v":[1,2]}]`},
			{"values", `[1,2.5,true,null,{"k":"x"},[1,2]]`},
		},
	})

	// Empty source array -> empty result array.
	f(`json_array_values message from parts as messages`, [][]Field{
		{
			{"parts", `[]`},
		},
	}, [][]Field{
		{
			{"parts", `[]`},
			{"messages", `[]`},
		},
	})

	// Default fromField is _msg, default resultField is _msg.
	f(`json_array_values message`, [][]Field{
		{
			{"_msg", `[{"message":"a"},{"message":"b"}]`},
		},
	}, [][]Field{
		{
			{"_msg", `["a","b"]`},
		},
	})
}

func TestPipeJSONArrayValuesUpdateNeededFields(t *testing.T) {
	f := func(s string, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f(`json_array_values message from parts as messages`, "*", "", "*", "messages")
	f(`json_array_values message from messages as messages`, "*", "", "*", "")

	// unneeded fields do not intersect with output field
	f(`json_array_values message from parts as messages`, "*", "f1,f2", "*", "f1,f2,messages")
	f(`json_array_values message from messages as messages`, "*", "f1,f2", "*", "f1,f2")

	// unneeded fields intersect with output field
	f(`json_array_values message from parts as messages`, "*", "messages,y", "*", "messages,y")

	// needed fields do not intersect with output field
	f(`json_array_values message from parts as messages`, "x,y", "", "x,y", "")

	// needed fields intersect with output field
	f(`json_array_values message from parts as messages`, "messages,y", "", "parts,y", "")
}
