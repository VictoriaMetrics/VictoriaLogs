package logstorage

import (
	"reflect"
	"testing"
)

func TestParseStatsJSONValuesSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncSuccess(t, pipeStr)
	}

	f(`json_values(*)`)
	f(`json_values(a)`)
	f(`json_values(a, b)`)
	f(`json_values(a*, b)`)
	f(`json_values(a*, b) sort by (x desc, y)`)
	f(`json_values(a, b) limit 10`)
	f(`json_values(a*, b) sort by (x desc, y) limit 10`)
}

func TestParseStatsJSONValuesFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncFailure(t, pipeStr)
	}

	f(`json_values`)
	f(`json_values(a b)`)
	f(`json_values(x) y`)
	f(`json_values(a, b) limit`)
	f(`json_values(a, b) limit foo`)
}

func TestStatsJSONValues(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// all the log fields without limit
	f("stats json_values() order by (a) as x", [][]Field{
		{
			{"b", `3`},
			{"_msg", `abc`},
			{"a", `2`},
		},
		{
			{"a", `1`},
			{"_msg", `def`},
		},
		{
			{"a", `3`},
			{"b", `54`},
		},
	}, [][]Field{
		{
			{"x", `[{"_msg":"def","a":"1"},{"_msg":"abc","a":"2","b":"3"},{"a":"3","b":"54"}]`},
		},
	})

	// all the log fields
	f("stats json_values() order (a) limit 2 as x", [][]Field{
		{
			{"b", `3`},
			{"_msg", `abc`},
			{"a", `2`},
		},
		{
			{"a", `1`},
			{"_msg", `def`},
		},
		{
			{"a", `3`},
			{"b", `54`},
		},
	}, [][]Field{
		{
			{"x", `[{"_msg":"def","a":"1"},{"_msg":"abc","a":"2","b":"3"}]`},
		},
	})

	// the selected log fields
	f("stats json_values(b,_msg) order (a) limit 2 as x", [][]Field{
		{
			{"a", `2`},
			{"_msg", `abc`},
			{"b", `3`},
		},
		{
			{"_msg", `def`},
			{"a", `1`},
		},
		{
			{"b", `54`},
			{"a", `3`},
		},
	}, [][]Field{
		{
			{"x", `[{"_msg":"def"},{"_msg":"abc","b":"3"}]`},
		},
	})

	// reverse order
	f("stats json_values() sort by (a desc) limit 1 as x", [][]Field{
		{
			{"b", `3`},
			{"_msg", `abc`},
			{"a", `2`},
		},
		{
			{"_msg", `def`},
			{"a", `1`},
		},
		{
			{"a", `3`},
			{"b", `54`},
		},
	}, [][]Field{
		{
			{"x", `[{"a":"3","b":"54"}]`},
		},
	})

	// multiple sorting columns without limit
	f("stats json_values() sort by (a desc, b) as x", [][]Field{
		{
			{"a", `3`},
			{"b", `123`},
		},
		{
			{"a", `1`},
		},
		{
			{"b", `54`},
			{"a", `3`},
		},
	}, [][]Field{
		{
			{"x", `[{"a":"3","b":"54"},{"a":"3","b":"123"},{"a":"1"}]`},
		},
	})

	// multiple sorting columns with limit
	f("stats json_values() sort by (a desc, b) limit 2 as x", [][]Field{
		{
			{"a", `3`},
			{"b", `123`},
		},
		{
			{"a", `1`},
		},
		{
			{"b", `54`},
			{"a", `3`},
		},
	}, [][]Field{
		{
			{"x", `[{"a":"3","b":"54"},{"a":"3","b":"123"}]`},
		},
	})
}

func TestStatsJSONValuesProcessor_ExportImportState(t *testing.T) {
	var a chunkedAllocator
	newStatsJSONValuesProcessor := func() *statsJSONValuesProcessor {
		return a.newStatsJSONValuesProcessor()
	}

	f := func(sjp *statsJSONValuesProcessor, dataLenExpected int) {
		t.Helper()

		data := sjp.exportState(nil, nil)
		dataLen := len(data)
		if dataLen != dataLenExpected {
			t.Fatalf("unexpected dataLen; got %d; want %d", dataLen, dataLenExpected)
		}

		sjp2 := newStatsJSONValuesProcessor()
		_, err := sjp2.importState(data, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if !reflect.DeepEqual(sjp, sjp2) {
			t.Fatalf("unexpected state imported\ngot\n%#v\nwant\n%#v", sjp2, sjp)
		}
	}

	// empty state
	sjp := newStatsJSONValuesProcessor()
	f(sjp, 1)

	// non-empty state
	sjp = newStatsJSONValuesProcessor()
	sjp.entries = []string{"foo", "bar", "baz"}
	f(sjp, 13)
}

func TestStatsJSONValuesSortedProcessor_ExportImportState(t *testing.T) {
	var a chunkedAllocator
	newStatsJSONValuesSortedProcessor := func() *statsJSONValuesSortedProcessor {
		return a.newStatsJSONValuesSortedProcessor()
	}

	f := func(sjp *statsJSONValuesSortedProcessor, sortFieldsLen, dataLenExpected int) {
		t.Helper()

		sjp.sortFieldsLen = sortFieldsLen
		data := sjp.exportState(nil, nil)
		dataLen := len(data)
		if dataLen != dataLenExpected {
			t.Fatalf("unexpected dataLen; got %d; want %d", dataLen, dataLenExpected)
		}

		sjp2 := newStatsJSONValuesSortedProcessor()
		sjp2.sortFieldsLen = sortFieldsLen
		_, err := sjp2.importState(data, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if !reflect.DeepEqual(sjp, sjp2) {
			t.Fatalf("unexpected state imported\ngot\n%#v\nwant\n%#v", sjp2, sjp)
		}
	}

	// empty state
	sjp := newStatsJSONValuesSortedProcessor()
	f(sjp, 0, 1)

	// non-empty state
	sjp = newStatsJSONValuesSortedProcessor()
	sjp.entries = []*statsJSONValuesSortedEntry{
		{
			value:      "foo",
			sortValues: []string{"v1-for-foo", "v2-for-foo"},
		},
		{
			value:      "bar",
			sortValues: []string{"v1-for-bar", "v2-for-bar"},
		},
	}
	f(sjp, 2, 53)
}

func TestStatsJSONValuesTopkProcessor_ExportImportState(t *testing.T) {
	var a chunkedAllocator
	newStatsJSONValuesTopkProcessor := func() *statsJSONValuesTopkProcessor {
		return a.newStatsJSONValuesTopkProcessor()
	}

	f := func(sjp *statsJSONValuesTopkProcessor, sortFieldsLen, dataLenExpected int) {
		t.Helper()

		sjp.sortFieldsLen = sortFieldsLen
		data := sjp.exportState(nil, nil)
		dataLen := len(data)
		if dataLen != dataLenExpected {
			t.Fatalf("unexpected dataLen; got %d; want %d", dataLen, dataLenExpected)
		}

		sjp2 := newStatsJSONValuesTopkProcessor()
		sjp2.sortFieldsLen = sortFieldsLen
		_, err := sjp2.importState(data, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if !reflect.DeepEqual(sjp, sjp2) {
			t.Fatalf("unexpected state imported\ngot\n%#v\nwant\n%#v", sjp2, sjp)
		}
	}

	// empty state
	sjp := newStatsJSONValuesTopkProcessor()
	f(sjp, 0, 1)

	// non-empty state
	sjp = newStatsJSONValuesTopkProcessor()
	sjp.h.entries = []*statsJSONValuesSortedEntry{
		{
			value:      "foo",
			sortValues: []string{"v1-for-foo", "v2-for-foo"},
		},
		{
			value:      "bar",
			sortValues: []string{"v1-for-bar", "v2-for-bar"},
		},
	}
	f(sjp, 2, 53)
}
