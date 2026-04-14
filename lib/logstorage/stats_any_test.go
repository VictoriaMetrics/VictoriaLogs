package logstorage

import (
	"reflect"
	"testing"
)

func TestParseStatsAnySuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncSuccess(t, pipeStr)
	}

	f(`any(foo)`)
}

func TestParseStatsAnyFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncFailure(t, pipeStr)
	}

	f(`any`)
	f(`any()`)
	f(`any(x) bar`)
	f(`any(x, y)`)
}

func TestStatsAny(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	f("any(a)", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
		},
	}, [][]Field{
		{
			{"any(a)", `2`},
		},
	})

	f("stats any(_msg) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
		},
	}, [][]Field{
		{
			{"x", `abc`},
		},
	})

	f("stats any(a) if (b:'') as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"_msg", `def`},
			{"a", `1`},
		},
	}, [][]Field{
		{
			{"x", `1`},
		},
	})

	f("stats by (b) any(a) if (b:*) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"a", `3`},
			{"c", `54`},
		},
	}, [][]Field{
		{
			{"b", "3"},
			{"x", `2`},
		},
		{
			{"b", ""},
			{"x", ``},
		},
	})

	f("stats by (a) any(b) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `1`},
			{"b", `3`},
		},
		{
			{"a", `3`},
			{"b", `5`},
		},
	}, [][]Field{
		{
			{"a", "1"},
			{"x", `3`},
		},
		{
			{"a", "3"},
			{"x", `5`},
		},
	})

	f("stats by (a) any(c) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `1`},
			{"b", `3`},
		},
		{
			{"a", `3`},
			{"c", `foo`},
		},
	}, [][]Field{
		{
			{"a", "1"},
			{"x", ``},
		},
		{
			{"a", "3"},
			{"x", `foo`},
		},
	})

	f("stats by (a, b) any(c) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `1`},
			{"b", `3`},
		},
		{
			{"_msg", `def`},
			{"a", `1`},
			{"c", "foo"},
		},
		{
			{"a", `3`},
			{"b", `5`},
			{"c", "4"},
		},
	}, [][]Field{
		{
			{"a", "1"},
			{"b", "3"},
			{"x", ``},
		},
		{
			{"a", "1"},
			{"b", ""},
			{"x", `foo`},
		},
		{
			{"a", "3"},
			{"b", "5"},
			{"x", `4`},
		},
	})
}

func TestStatsAny_ExportImportState(t *testing.T) {
	f := func(sap *statsAnyProcessor, dataLenExpected int) {
		t.Helper()

		data := sap.exportState(nil, nil)
		dataLen := len(data)
		if dataLen != dataLenExpected {
			t.Fatalf("unexpected dataLen; got %d; want %d", dataLen, dataLenExpected)
		}

		var sap2 statsAnyProcessor
		_, err := sap2.importState(data, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if !reflect.DeepEqual(sap, &sap2) {
			t.Fatalf("unexpected state imported; got %#v; want %#v", &sap2, sap)
		}
	}

	var sap statsAnyProcessor

	// zero state
	f(&sap, 1)

	// non-zero state
	sap = statsAnyProcessor{
		value: "foobar",
	}
	f(&sap, 7)
}
