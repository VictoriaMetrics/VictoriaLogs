package logstorage

import (
	"reflect"
	"testing"
)

func TestParsePipeLimitSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`limit 10`)
	f(`limit 10000`)
}

func TestParsePipeLimitFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`limit -10`)
	f(`limit foo`)
}

func TestPipeLimit(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}
	f("limit", [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
	}, [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
	})

	f("limit 100", [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
	}, [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
	})

	f("limit 1", [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
	}, [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
	})

	f("limit 0", [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
	}, [][]Field{})

	f("limit 1", [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
		{
			{"_msg", `abc`},
			{"a", `aiewr`},
		},
	}, [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
	})

	f("limit 1", [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
		{
			{"_msg", `abc`},
			{"a", `aiewr`},
			{"asdf", "fsf"},
		},
	}, [][]Field{
		{
			{"_msg", `{"foo":"bar"}`},
			{"a", `test`},
		},
	})
}

func TestPipeLimitTruncatesBucketedValues(t *testing.T) {
	f := func(limit uint64, valuesExpected, valuesBucketedExpected []string) {
		t.Helper()

		br := getBlockResult()
		defer putBlockResult(br)

		br.setResultColumns([]resultColumn{
			{
				name:   "_time",
				values: []string{"2025-01-01T00:00:00Z", "2025-01-01T00:00:01Z", "2025-01-01T00:00:02Z"},
			},
			{
				name:   "foo",
				values: []string{"15", "25", "35"},
			},
		}, 3)

		c := br.getColumnByName("foo")
		bf := &byStatsField{
			name:          "foo",
			bucketSizeStr: "10",
			bucketSize:    10,
		}
		_ = c.getValuesBucketed(br, bf)

		ppNext := &capturePipeProcessor{}
		canceled := false
		plp := (&pipeLimit{limit: limit}).newPipeProcessor(1, nil, func() {
			canceled = true
		}, ppNext)
		plp.writeBlock(0, br)

		if !canceled {
			t.Fatalf("missing cancel call")
		}
		if !reflect.DeepEqual(ppNext.values, valuesExpected) {
			t.Fatalf("unexpected values; got %q; want %q", ppNext.values, valuesExpected)
		}
		if !reflect.DeepEqual(ppNext.valuesBucketed, valuesBucketedExpected) {
			t.Fatalf("unexpected valuesBucketed; got %q; want %q", ppNext.valuesBucketed, valuesBucketedExpected)
		}
	}

	f(0, nil, nil)
	f(1, []string{"15"}, []string{"10"})
	f(2, []string{"15", "25"}, []string{"10", "20"})
	f(3, []string{"15", "25", "35"}, []string{"10", "20", "30"})
}

func TestPipeLimitUpdateNeededFields(t *testing.T) {
	f := func(s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f("limit 10", "*", "", "*", "")

	// all the needed fields, plus unneeded fields
	f("limit 10", "*", "f1,f2", "*", "f1,f2")

	// needed fields
	f("limit 10", "f1,f2", "", "f1,f2", "")
}

type capturePipeProcessor struct {
	values         []string
	valuesBucketed []string
}

func (pp *capturePipeProcessor) writeBlock(_ uint, br *blockResult) {
	c := br.getColumnByName("foo")
	pp.values = append(pp.values[:0], c.getValues(br)...)
	pp.valuesBucketed = append(pp.valuesBucketed[:0], c.valuesBucketed...)
}

func (pp *capturePipeProcessor) flush() error {
	return nil
}
