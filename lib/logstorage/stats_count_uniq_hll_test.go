package logstorage

import (
	"fmt"
	"math"
	"testing"
)

func TestParseStatsCountUniqHLLSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncSuccess(t, pipeStr)
	}

	f(`count_uniq_hll(a)`)
	f(`count_uniq_hll(a, b)`)
}

func TestParseStatsCountUniqHLLFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParseStatsFuncFailure(t, pipeStr)
	}

	f(`count_uniq_hll`)
	f(`count_uniq_hll()`)
	f(`count_uniq_hll(*)`)
	f(`count_uniq_hll(a*, b)`)
	f(`count_uniq_hll(a b)`)
	f(`count_uniq_hll(x) y`)
	f(`count_uniq_hll(x) limit 10`)
}

func TestStatsCountUniqHLL(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	f("stats count_uniq_hll(a) as x", [][]Field{
		{{"a", "1"}},
		{{"a", "2"}},
		{{"a", "1"}},
		{{"a", ""}},
		{},
	}, [][]Field{
		{{"x", "2"}},
	})

	// Numeric string normalization: "1" and "01" share the unsigned domain.
	f("stats count_uniq_hll(a) as x", [][]Field{
		{{"a", "1"}},
		{{"a", "01"}},
		{{"a", "2"}},
	}, [][]Field{
		{{"x", "2"}},
	})

	f("stats count_uniq_hll(a, b) as x", [][]Field{
		{{"a", "1"}, {"b", "x"}},
		{{"a", "1"}, {"b", "y"}},
		{{"a", "1"}, {"b", "x"}},
		{{"a", ""}, {"b", ""}},
	}, [][]Field{
		{{"x", "2"}},
	})

	f("stats by (a) count_uniq_hll(b) as x", [][]Field{
		{{"a", "foo"}, {"b", "1"}},
		{{"a", "foo"}, {"b", "2"}},
		{{"a", "bar"}, {"b", "1"}},
		{{"a", "bar"}, {"b", "1"}},
	}, [][]Field{
		{{"a", "bar"}, {"x", "1"}},
		{{"a", "foo"}, {"x", "2"}},
	})

	f("stats count_uniq_hll(a) if (b:foo) as x", [][]Field{
		{{"a", "1"}, {"b", "foo"}},
		{{"a", "2"}, {"b", "bar"}},
		{{"a", "3"}, {"b", "foo"}},
	}, [][]Field{
		{{"x", "2"}},
	})
}

func TestHLLHashSchemaGolden(t *testing.T) {
	if hllHashUnsigned(1) != hllHashGenericString("1") {
		t.Fatalf("uint 1 and string 1 must share unsigned domain hash")
	}
	if hllHashUnsigned(1) != hllHashGenericString("01") {
		t.Fatalf("string 01 must parse as unsigned 1")
	}
	if hllHashUnsigned(1) == hllHashGenericString("1.0") {
		t.Fatalf("string 1.0 must stay in string domain")
	}
	if hllHashNegative(-1) != hllHashGenericString("-1") {
		t.Fatalf("negative int and string -1 must share negative domain hash")
	}
	if hllHashTimestamp(1) == hllHashUnsigned(1) {
		t.Fatalf("_time domain must differ from unsigned domain")
	}
}

func TestHLLExportImportMerge(t *testing.T) {
	var a, b, merged hllSketch
	for i := 0; i < 1000; i++ {
		a.addHash(hllHashUnsigned(uint64(i)))
	}
	for i := 500; i < 1500; i++ {
		b.addHash(hllHashUnsigned(uint64(i)))
	}

	stateA := a.appendState(nil)
	stateB := b.appendState(nil)

	var a2, b2 hllSketch
	if _, err := a2.unmarshalState(stateA); err != nil {
		t.Fatalf("import a: %s", err)
	}
	if _, err := b2.unmarshalState(stateB); err != nil {
		t.Fatalf("import b: %s", err)
	}
	merged.merge(&a2)
	merged.merge(&b2)

	got := merged.estimate()
	const want = 1500
	relErr := math.Abs(float64(got)-want) / want
	if relErr > 0.05 {
		t.Fatalf("merged estimate=%d want~%d relErr=%.4f", got, want, relErr)
	}

	sumEstimates := a.estimate() + b.estimate()
	if math.Abs(float64(sumEstimates)-want) <= math.Abs(float64(got)-want) {
		t.Fatalf("sum of node estimates (%d) unexpectedly not worse than merged (%d) for want=%d", sumEstimates, got, want)
	}
}

func TestHLLWireValidation(t *testing.T) {
	var h hllSketch
	h.addHash(hllHashUnsigned(42))
	good := h.appendState(nil)

	f := func(name string, mutate func([]byte) []byte) {
		t.Helper()
		bad := mutate(append([]byte(nil), good...))
		var dst hllSketch
		if _, err := dst.unmarshalState(bad); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}

	f("trunc", func(b []byte) []byte { return b[:len(b)/2] })
	f("tail", func(b []byte) []byte { return append(b, 0) })
	f("magic", func(b []byte) []byte { b[0] ^= 0xff; return b })
	f("wireVersion", func(b []byte) []byte { b[4] = 99; return b })
	f("algo", func(b []byte) []byte { b[5] = 99; return b })
	f("precision", func(b []byte) []byte { b[6] = 13; return b })
	f("hashSchema", func(b []byte) []byte { b[7] = 99; return b })
}

func TestHLLEmptyStateRoundTrip(t *testing.T) {
	var h hllSketch
	state := h.appendState(nil)
	var h2 hllSketch
	n, err := h2.unmarshalState(state)
	if err != nil {
		t.Fatalf("import empty: %s", err)
	}
	if n != 0 {
		t.Fatalf("empty import must not charge budget; got %d", n)
	}
	if h2.estimate() != 0 {
		t.Fatalf("empty estimate must be 0; got %d", h2.estimate())
	}
}

func TestStatsCountUniqHLL_ExportImportState(t *testing.T) {
	sup := &statsCountUniqHLLProcessor{}
	for i := 0; i < 10; i++ {
		sup.addHash(hllHashUnsigned(uint64(i)))
	}
	data := sup.exportState(nil, nil)

	sup2 := &statsCountUniqHLLProcessor{}
	stateSize, err := sup2.importState(data, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if stateSize != hllStateBudgetBytes {
		t.Fatalf("unexpected state size; got %d; want %d", stateSize, hllStateBudgetBytes)
	}

	sf := &statsCountUniqHLL{fields: []string{"a"}}
	got := string(sup2.finalizeStats(sf, nil, nil))
	want := string(sup.finalizeStats(sf, nil, nil))
	if got != want {
		t.Fatalf("unexpected estimate after import; got %q; want %q", got, want)
	}
}

func TestHLLAccuracySample(t *testing.T) {
	cardinalities := []int{1000, 10000, 100000}
	for _, n := range cardinalities {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			var h hllSketch
			for i := 0; i < n; i++ {
				h.addHash(hllHashUnsigned(uint64(i)))
			}
			got := h.estimate()
			relErr := math.Abs(float64(got)-float64(n)) / float64(n)
			if relErr > 0.03 {
				t.Fatalf("estimate=%d want=%d relErr=%.4f", got, n, relErr)
			}
		})
	}
}
