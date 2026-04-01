package logstorage

import (
	"reflect"
	"testing"
)

func TestParseStatsStddevSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncSuccess(t, pipeStr)
	}

	f(`stddev(*)`)
	f(`stddev(a)`)
	f(`stddev(a, b)`)
	f(`stddev(a*, b)`)
}

func TestParseStatsStddevFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncFailure(t, pipeStr)
	}

	f(`stddev`)
	f(`stddev(a b)`)
	f(`stddev(x) y`)
}

func TestStatsStddev(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	f("stats stddev(*) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"_msg", `def`},
			{"a", `0`},
		},
		{
			{"a", `6`},
			{"b", `4`},
		},
	}, [][]Field{
		{
			{"x", "2"},
		},
	})

	f("stats stddev(_msg) as x", [][]Field{
		{
			{"_msg", `abc`},
			{"a", `2`},
			{"b", `3`},
		},
		{
			{"_msg", `def`},
			{"a", `0`},
		},
		{
			{"a", `6`},
			{"b", `4`},
		},
	}, [][]Field{
		{
			{"x", "NaN"},
		},
	})
}

func TestStatsStddev_ExportImportState(t *testing.T) {
	f := func(ssp *statsStddevProcessor, dataLenExpected, stateSizeExpected int) {
		t.Helper()

		data := ssp.exportState(nil, nil)
		dataLen := len(data)
		if dataLen != dataLenExpected {
			t.Fatalf("unexpected dataLen; got %d; want %d", dataLen, dataLenExpected)
		}

		var ssp2 statsStddevProcessor
		stateSize, err := ssp2.importState(data, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if stateSize != stateSizeExpected {
			t.Fatalf("unexpected state size; got %d bytes; want %d bytes", stateSize, stateSizeExpected)
		}

		if !reflect.DeepEqual(ssp, &ssp2) {
			t.Fatalf("unexpected state imported; got %#v; want %#v", &ssp2, ssp)
		}
	}

	var ssp statsStddevProcessor

	f(&ssp, 24, 0)

	ssp = statsStddevProcessor{
		avg:   123,
		q:     123.3243,
		count: 234,
	}
	f(&ssp, 24, 0)
}
