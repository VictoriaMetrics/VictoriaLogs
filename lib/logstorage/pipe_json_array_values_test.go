package logstorage

import (
	"testing"
)

func TestParsePipeJSONArrayValuesSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`json_array_values foo`)
	f(`json_array_values foo from bar`)
	f(`json_array_values foo from bar as baz`)
	f(`json_array_values foo as bar`)
	f(`json_array_values user.name from arr as x`)
}

func TestParsePipeJSONArrayValuesFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`json_array_values`)
	f(`json_array_values *`)
	f(`json_array_values foo*`)
	f(`json_array_values foo from`)
	f(`json_array_values foo from *`)
	f(`json_array_values foo from x*`)
	f(`json_array_values foo as`)
	f(`json_array_values foo as *`)
	f(`json_array_values foo from bar as *`)
	f(`json_array_values foo from bar as baz x`)
	f(`json_array_values foo, bar`)
}

func TestPipeJSONArrayValues(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// missing source field
	f(`json_array_values user from arr as x`, [][]Field{
		{
			{"q", "w"},
		},
	}, [][]Field{
		{
			{"q", "w"},
			{"x", "[]"},
		},
	})

	// leading whitespace before JSON array
	f(`json_array_values user from arr as x`, [][]Field{
		{
			{"arr", `  [{"user":"alice"},{"user":"bob"}]`},
		},
	}, [][]Field{
		{
			{"arr", `  [{"user":"alice"},{"user":"bob"}]`},
			{"x", `["alice","bob"]`},
		},
	})

	// non-array input
	f(`json_array_values user from arr as x`, [][]Field{
		{
			{"arr", `{"user":"alice"}`},
			{"q", "w"},
		},
	}, [][]Field{
		{
			{"arr", `{"user":"alice"}`},
			{"q", "w"},
			{"x", "[]"},
		},
	})

	// empty array
	f(`json_array_values user from arr as x`, [][]Field{
		{
			{"arr", `[]`},
		},
	}, [][]Field{
		{
			{"arr", `[]`},
			{"x", `[]`},
		},
	})

	// basic string extraction
	f(`json_array_values user from arr as x`, [][]Field{
		{
			{"arr", `[{"user":"alice"},{"user":"bob"}]`},
			{"q", "w"},
		},
		{
			{"arr", `[{"user":"charlie"}]`},
		},
	}, [][]Field{
		{
			{"arr", `[{"user":"alice"},{"user":"bob"}]`},
			{"q", "w"},
			{"x", `["alice","bob"]`},
		},
		{
			{"arr", `[{"user":"charlie"}]`},
			{"x", `["charlie"]`},
		},
	})

	// missing field in object is skipped
	f(`json_array_values user from arr as x`, [][]Field{
		{
			{"arr", `[{"user":"alice"},{"age":30}]`},
		},
	}, [][]Field{
		{
			{"arr", `[{"user":"alice"},{"age":30}]`},
			{"x", `["alice"]`},
		},
	})

	// null field value is skipped
	f(`json_array_values user from arr as x`, [][]Field{
		{
			{"arr", `[{"user":null},{"user":"bob"}]`},
		},
	}, [][]Field{
		{
			{"arr", `[{"user":null},{"user":"bob"}]`},
			{"x", `["bob"]`},
		},
	})

	// non-object item in array is skipped
	f(`json_array_values user from arr as x`, [][]Field{
		{
			{"arr", `[{"user":"alice"},42,{"user":"bob"}]`},
		},
	}, [][]Field{
		{
			{"arr", `[{"user":"alice"},42,{"user":"bob"}]`},
			{"x", `["alice","bob"]`},
		},
	})

	// number field value
	f(`json_array_values age from arr as x`, [][]Field{
		{
			{"arr", `[{"age":42},{"age":25}]`},
		},
	}, [][]Field{
		{
			{"arr", `[{"age":42},{"age":25}]`},
			{"x", `["42","25"]`},
		},
	})

	// bool field value
	f(`json_array_values active from arr as x`, [][]Field{
		{
			{"arr", `[{"active":true},{"active":false}]`},
		},
	}, [][]Field{
		{
			{"arr", `[{"active":true},{"active":false}]`},
			{"x", `["true","false"]`},
		},
	})

	// from _msg in place
	f(`json_array_values user`, [][]Field{
		{
			{"_msg", `[{"user":"alice"},{"user":"bob"}]`},
			{"q", "w"},
		},
	}, [][]Field{
		{
			{"_msg", `["alice","bob"]`},
			{"q", "w"},
		},
	})

	// from _msg into other field
	f(`json_array_values user as x`, [][]Field{
		{
			{"_msg", `[{"user":"alice"}]`},
			{"q", "w"},
		},
	}, [][]Field{
		{
			{"_msg", `[{"user":"alice"}]`},
			{"q", "w"},
			{"x", `["alice"]`},
		},
	})

	// from field in place
	f(`json_array_values user from arr`, [][]Field{
		{
			{"arr", `[{"user":"alice"},{"user":"bob"}]`},
			{"q", "w"},
		},
	}, [][]Field{
		{
			{"arr", `["alice","bob"]`},
			{"q", "w"},
		},
	})

	// dotted field name navigates nested objects
	f(`json_array_values user.name from arr as x`, [][]Field{
		{
			{"arr", `[{"user":{"name":"alice"}},{"user":{"name":"bob"}}]`},
		},
	}, [][]Field{
		{
			{"arr", `[{"user":{"name":"alice"}},{"user":{"name":"bob"}}]`},
			{"x", `["alice","bob"]`},
		},
	})

	// dotted field name matches literal key containing a dot
	f(`json_array_values user.name from arr as x`, [][]Field{
		{
			{"arr", `[{"user.name":"alice"},{"user.name":"bob"}]`},
		},
	}, [][]Field{
		{
			{"arr", `[{"user.name":"alice"},{"user.name":"bob"}]`},
			{"x", `["alice","bob"]`},
		},
	})

	// literal key takes priority over nested when both exist
	f(`json_array_values user.name from arr as x`, [][]Field{
		{
			{"arr", `[{"user.name":"literal","user":{"name":"nested"}}]`},
		},
	}, [][]Field{
		{
			{"arr", `[{"user.name":"literal","user":{"name":"nested"}}]`},
			{"x", `["literal"]`},
		},
	})
}

func TestPipeJSONArrayValuesUpdateNeededFields(t *testing.T) {
	f := func(s string, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f(`json_array_values user from y as x`, "*", "", "*", "x")
	f(`json_array_values user from x`, "*", "", "*", "")

	// unneeded fields do not intersect with output field
	f(`json_array_values user from y as x`, "*", "f1,f2", "*", "f1,f2,x")
	f(`json_array_values user from x`, "*", "f1,f2", "*", "f1,f2")

	// unneeded fields intersect with output field
	f(`json_array_values user from z as x`, "*", "x,y", "*", "x,y")
	f(`json_array_values user from y as x`, "*", "x,y", "*", "x,y")
	f(`json_array_values user from x`, "*", "x,y", "*", "x,y")

	// needed fields do not intersect with output field
	f(`json_array_values user from y as z`, "x,y", "", "x,y", "")
	f(`json_array_values user from z`, "x,y", "", "x,y", "")

	// needed fields intersect with output field
	f(`json_array_values user from z as f2`, "f2,y", "", "y,z", "")
	f(`json_array_values user from y as f2`, "f2,y", "", "y", "")
	f(`json_array_values user from y`, "f2,y", "", "f2,y", "")
}
