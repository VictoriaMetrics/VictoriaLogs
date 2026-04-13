package logstorage

import (
	"reflect"
	"testing"
)

func TestParseStatsFieldMaxSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncSuccess(t, pipeStr)
	}

	f(`field_max(foo, bar)`)
}

func TestParseStatsFieldMaxFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncFailure(t, pipeStr)
	}

	f(`field_max`)
	f(`field_max()`)
	f(`field_max(x)`)
	f(`field_max(x, y, z)`)
}

func TestStatsFieldMax(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	f("stats field_max(b, a) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
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
			{"x", `3`},
		},
	})

	f("stats field_max(foo, a) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
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
			{"x", ``},
		},
	})

	f("stats field_max(b, a) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"_msg", `def`},
			{"a", `1`},
		},
		{
			{"a", `3`},
			{"b", `54`},
			{"c", "1232"},
		},
	}, [][]Field{
		{
			{"x", `3`},
		},
	})

	f("stats field_max(a, b) if (b:*) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
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
			{"x", `54`},
		},
	})

	f("stats by (b) field_max(a, b) if (b:*) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"_msg", `def`},
			{"a", `-12.34`},
			{"b", "3"},
		},
		{
			{"a", `3`},
			{"c", `54`},
		},
	}, [][]Field{
		{
			{"b", "3"},
			{"x", `3`},
		},
		{
			{"b", ""},
			{"x", ``},
		},
	})

	f("stats by (a) field_max(b, b) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `1`},
			{"b", `3`},
		},
		{
			{"_msg", `def`},
			{"a", `1`},
		},
		{
			{"a", `3`},
			{"b", `5`},
		},
		{
			{"a", `3`},
			{"b", `7`},
		},
	}, [][]Field{
		{
			{"a", "1"},
			{"x", `3`},
		},
		{
			{"a", "3"},
			{"x", `7`},
		},
	})

	f("stats by (a) field_max(c, a) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `1`},
			{"b", `3`},
		},
		{
			{"_msg", `def`},
			{"a", `1`},
		},
		{
			{"a", `3`},
			{"c", `foo`},
		},
		{
			{"a", `3`},
			{"b", `7`},
		},
	}, [][]Field{
		{
			{"a", "1"},
			{"x", ``},
		},
		{
			{"a", "3"},
			{"x", `3`},
		},
	})

	f("stats by (a) field_max(b, c) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `1`},
			{"b", `34`},
		},
		{
			{"_msg", `def`},
			{"a", `1`},
			{"c", "3"},
		},
		{
			{"a", `3`},
			{"b", `5`},
			{"c", "foo"},
		},
		{
			{"a", `3`},
			{"b", `7`},
		},
	}, [][]Field{
		{
			{"a", "1"},
			{"x", ``},
		},
		{
			{"a", "3"},
			{"x", ``},
		},
	})

	f("stats by (a, b) field_max(c,a) as x", [][]Field{
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
			{"x", `1`},
		},
		{
			{"a", "3"},
			{"b", "5"},
			{"x", `3`},
		},
	})
}

func TestStatsFieldMax_ExportImportState(t *testing.T) {
	f := func(smp *statsFieldMaxProcessor, dataLenExpected int) {
		t.Helper()

		data := smp.exportState(nil, nil)
		dataLen := len(data)
		if dataLen != dataLenExpected {
			t.Fatalf("unexpected dataLen; got %d; want %d", dataLen, dataLenExpected)
		}

		var smp2 statsFieldMaxProcessor
		_, err := smp2.importState(data, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if !reflect.DeepEqual(smp, &smp2) {
			t.Fatalf("unexpected state imported; got %#v; want %#v", &smp2, smp)
		}
	}

	var smp statsFieldMaxProcessor

	// zero state
	f(&smp, 2)

	// non-zero state
	smp = statsFieldMaxProcessor{
		max:   "abcded",
		value: "ilojoerDSF",
	}
	f(&smp, 18)
}
