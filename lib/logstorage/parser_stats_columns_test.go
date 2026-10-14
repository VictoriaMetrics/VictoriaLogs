package logstorage

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func TestQueryGetStatsLabelsAndResultColumns_Success(t *testing.T) {
	f := func(qStr string, resultColumnsExpected []StatsResultColumn) {
		t.Helper()

		q, err := ParseQueryAtTimestamp(qStr, 0)
		if err != nil {
			t.Fatalf("cannot parse query [%s]: %s", qStr, err)
		}
		labelFields, resultColumns, err := q.GetStatsLabelsAndResultColumns(0, 0)
		if err != nil {
			t.Fatalf("unexpected error in GetStatsLabelsAndResultColumns(): %s", err)
		}
		_ = labelFields

		// Metric result columns come from a map, so their order is nondeterministic.
		// Sort both lists by column name before comparing.
		sortResultColumns := func(cols []StatsResultColumn) {
			sort.Slice(cols, func(i, j int) bool {
				return cols[i].Name < cols[j].Name
			})
		}
		sortResultColumns(resultColumns)
		sortResultColumns(resultColumnsExpected)

		resultColumnsStr := fmt.Sprintf("%v", resultColumns)
		resultColumnsExpectedStr := fmt.Sprintf("%v", resultColumnsExpected)
		if resultColumnsStr != resultColumnsExpectedStr {
			t.Fatalf("unexpected result columns for query [%s];\ngot: %s\nwant: %s", qStr, resultColumnsStr, resultColumnsExpectedStr)
		}
	}

	// Scalar stats funcs must be non-histogram result columns.
	f(`* | stats by (x) sum(y) as s, count() as c`, []StatsResultColumn{
		{Name: "x"},
		{Name: "s"},
		{Name: "c"},
	})

	// A histogram() result column must be marked as histogram.
	f(`* | stats by (x) histogram(y)`, []StatsResultColumn{
		{Name: "x"},
		{Name: `histogram(y)`, IsHistogram: true},
	})

	// histogram() with an alias.
	f(`* | stats by (x) histogram(y) as h`, []StatsResultColumn{
		{Name: "x"},
		{Name: "h", IsHistogram: true},
	})

	// A mix of histogram() and scalar funcs.
	f(`* | stats by (x) histogram(y) as h, sum(y) as s`, []StatsResultColumn{
		{Name: "x"},
		{Name: "h", IsHistogram: true},
		{Name: "s"},
	})

	// histogram() with switch() - every case becomes a histogram result column.
	f(`* | stats by (x) histogram(y) switch(case (y:>0) as pos, default as neg)`, []StatsResultColumn{
		{Name: "x"},
		{Name: "pos", IsHistogram: true},
		{Name: "neg", IsHistogram: true},
	})

	// Multiple histogram() funcs.
	f(`* | stats by (x) histogram(y) as h1, histogram(z) as h2`, []StatsResultColumn{
		{Name: "x"},
		{Name: "h1", IsHistogram: true},
		{Name: "h2", IsHistogram: true},
	})

	// The copy pipe duplicates the column, so both the source and the copy keep the histogram type.
	f(`* | stats by (x) histogram(y) as h | copy h as h2`, []StatsResultColumn{
		{Name: "x"},
		{Name: "h", IsHistogram: true},
		{Name: "h2", IsHistogram: true},
	})

	// A histogram() result column renamed to another column must keep the histogram type.
	f(`* | stats by (x) histogram(y) as h | rename h as h2`, []StatsResultColumn{
		{Name: "x"},
		{Name: "h2", IsHistogram: true},
	})

	// A deleted histogram() result column must disappear from result columns.
	f(`* | stats by (x) histogram(y) as h, sum(y) as s | delete h`, []StatsResultColumn{
		{Name: "x"},
		{Name: "s"},
	})

	// A histogram() result column dropped by fields must disappear from result columns.
	f(`* | stats by (x) histogram(y) as h, sum(y) as s | fields x, s`, []StatsResultColumn{
		{Name: "x"},
		{Name: "s"},
	})

	// A math pipe adds non-histogram result columns.
	f(`* | stats by (x) histogram(y) as h | math h * 2 as h2x`, []StatsResultColumn{
		{Name: "x"},
		{Name: "h", IsHistogram: true},
		{Name: "h2x"},
	})
}

func TestQueryGetStatsLabelsAndResultColumns_Errors(t *testing.T) {
	f := func(qStr, errMsgExpected string) {
		t.Helper()

		q, err := ParseQueryAtTimestamp(qStr, 0)
		if err != nil {
			t.Fatalf("cannot parse query [%s]: %s", qStr, err)
		}
		_, _, err = q.GetStatsLabelsAndResultColumns(0, 0)
		if err == nil {
			t.Fatalf("expecting error in GetStatsLabelsAndResultColumns() for query [%s]", qStr)
		}
		if errMsgExpected != "" && !strings.Contains(err.Error(), errMsgExpected) {
			t.Fatalf("unexpected error for query [%s];\ngot: %s\nwant substring: %s", qStr, err.Error(), errMsgExpected)
		}
	}

	// Missing `stats` pipe.
	f(`* | sort by (_time)`, "missing `| stats ...` pipe")

	// Query without any metric fields after `fields` pipe.
	f(`* | stats by (x) sum(y) as s | fields x`, "missing metric fields")
}
