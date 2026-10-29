package logstorage

import (
	"bytes"
	"fmt"
	"math"
	"runtime"
	"sort"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func TestHLLColumnTypeHashConsistency(t *testing.T) {
	sf := &statsCountUniqHLL{fields: []string{"a"}}

	estimateFrom := func(update func(p *statsCountUniqHLLProcessor)) uint64 {
		t.Helper()
		p := &statsCountUniqHLLProcessor{}
		update(p)
		out := p.finalizeStats(sf, nil, nil)
		n, err := parseUint64(string(out))
		if err != nil {
			t.Fatalf("cannot parse estimate %q: %s", out, err)
		}
		return n
	}

	// Same logical value 42 via typed encodings and string path must yield estimate 1.
	wantOne := uint64(1)

	gotString := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLStringBlockResult("a", []string{"42"})
		p.updateStatsForAllRows(sf, br)
	})
	if gotString != wantOne {
		t.Fatalf("string path: got %d; want %d", gotString, wantOne)
	}

	gotU8 := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLTypedUintBlockResult("a", valueTypeUint8, []uint64{42})
		p.updateStatsForAllRows(sf, br)
	})
	if gotU8 != wantOne {
		t.Fatalf("uint8 path: got %d; want %d", gotU8, wantOne)
	}

	gotU16 := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLTypedUintBlockResult("a", valueTypeUint16, []uint64{42})
		p.updateStatsForAllRows(sf, br)
	})
	if gotU16 != wantOne {
		t.Fatalf("uint16 path: got %d; want %d", gotU16, wantOne)
	}

	gotU32 := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLTypedUintBlockResult("a", valueTypeUint32, []uint64{42})
		p.updateStatsForAllRows(sf, br)
	})
	if gotU32 != wantOne {
		t.Fatalf("uint32 path: got %d; want %d", gotU32, wantOne)
	}

	gotU64 := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLTypedUintBlockResult("a", valueTypeUint64, []uint64{42})
		p.updateStatsForAllRows(sf, br)
	})
	if gotU64 != wantOne {
		t.Fatalf("uint64 path: got %d; want %d", gotU64, wantOne)
	}

	gotConst := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLConstBlockResult("a", "42")
		p.updateStatsForAllRows(sf, br)
	})
	if gotConst != wantOne {
		t.Fatalf("const path: got %d; want %d", gotConst, wantOne)
	}

	gotDict := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLDictBlockResult("a", []string{"42"}, []byte{0})
		p.updateStatsForAllRows(sf, br)
	})
	if gotDict != wantOne {
		t.Fatalf("dict path: got %d; want %d", gotDict, wantOne)
	}

	// Leading-zero string must collide with typed uint.
	gotLeading := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLStringBlockResult("a", []string{"042"})
		p.updateStatsForAllRows(sf, br)
	})
	if gotLeading != wantOne {
		t.Fatalf("leading-zero string: got %d; want %d", gotLeading, wantOne)
	}

	// Negative int64 path.
	gotNegTyped := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLTypedInt64BlockResult("a", []int64{-7})
		p.updateStatsForAllRows(sf, br)
	})
	gotNegString := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLStringBlockResult("a", []string{"-7"})
		p.updateStatsForAllRows(sf, br)
	})
	if gotNegTyped != 1 || gotNegString != 1 {
		t.Fatalf("negative paths: typed=%d string=%d; want 1", gotNegTyped, gotNegString)
	}

	// Mixing typed 42 and string 42 in one processor must stay at 1.
	gotMixed := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		p.updateStatsForAllRows(sf, newHLLTypedUintBlockResult("a", valueTypeUint64, []uint64{42}))
		p.updateStatsForAllRows(sf, newHLLStringBlockResult("a", []string{"42", "042"}))
	})
	if gotMixed != wantOne {
		t.Fatalf("mixed typed+string: got %d; want %d", gotMixed, wantOne)
	}

	// _time domain must not collide with unsigned 1.
	sfTime := &statsCountUniqHLL{fields: []string{"_time"}}
	gotTime := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLTimeBlockResult([]int64{1})
		p.updateStatsForAllRows(sfTime, br)
	})
	gotUnsigned1 := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		br := newHLLTypedUintBlockResult("a", valueTypeUint64, []uint64{1})
		p.updateStatsForAllRows(sf, br)
	})
	if gotTime != 1 || gotUnsigned1 != 1 {
		t.Fatalf("time/unsigned estimates: time=%d unsigned=%d", gotTime, gotUnsigned1)
	}
	gotBoth := estimateFrom(func(p *statsCountUniqHLLProcessor) {
		p.addHash(hllHashTimestamp(1))
		p.addHash(hllHashUnsigned(1))
	})
	if gotBoth != 2 {
		t.Fatalf("_time and unsigned 1 must be distinct; got %d", gotBoth)
	}
}

func TestHLLStatsModesRemoteLocalProxy(t *testing.T) {
	sf := &statsCountUniqHLL{fields: []string{"a"}}

	// Simulate two vlstorage remotes with overlapping sets.
	remote1 := &statsCountUniqHLLProcessor{}
	remote2 := &statsCountUniqHLLProcessor{}
	for i := 0; i < 800; i++ {
		remote1.addHash(hllHashUnsigned(uint64(i)))
	}
	for i := 400; i < 1200; i++ {
		remote2.addHash(hllHashUnsigned(uint64(i)))
	}
	state1 := remote1.exportState(nil, nil)
	state2 := remote2.exportState(nil, nil)

	// local: import + merge + finalize
	local := &statsCountUniqHLLProcessor{}
	if _, err := local.importState(state1, nil); err != nil {
		t.Fatalf("local import remote1: %s", err)
	}
	tmp := &statsCountUniqHLLProcessor{}
	if _, err := tmp.importState(state2, nil); err != nil {
		t.Fatalf("tmp import remote2: %s", err)
	}
	local.mergeState(nil, sf, tmp)
	localEst := mustParseEstimate(t, local.finalizeStats(sf, nil, nil))

	const want = 1200
	if relErr(localEst, want) > 0.05 {
		t.Fatalf("local estimate=%d want~%d", localEst, want)
	}

	// proxy: import both, re-export merged sketch, top-level import+finalize
	proxy := &statsCountUniqHLLProcessor{}
	if _, err := proxy.importState(state1, nil); err != nil {
		t.Fatalf("proxy import1: %s", err)
	}
	tmp2 := &statsCountUniqHLLProcessor{}
	if _, err := tmp2.importState(state2, nil); err != nil {
		t.Fatalf("proxy import2: %s", err)
	}
	proxy.mergeState(nil, sf, tmp2)
	proxyState := proxy.exportState(nil, nil)

	top := &statsCountUniqHLLProcessor{}
	if _, err := top.importState(proxyState, nil); err != nil {
		t.Fatalf("top import proxy: %s", err)
	}
	topEst := mustParseEstimate(t, top.finalizeStats(sf, nil, nil))
	if relErr(topEst, want) > 0.05 {
		t.Fatalf("proxy→top estimate=%d want~%d", topEst, want)
	}
	if relErr(topEst, localEst) > 0.02 {
		t.Fatalf("proxy path diverged from local; local=%d top=%d", localEst, topEst)
	}

	// default-equivalent: single processor sees all values
	def := &statsCountUniqHLLProcessor{}
	for i := 0; i < 1200; i++ {
		def.addHash(hllHashUnsigned(uint64(i)))
	}
	defEst := mustParseEstimate(t, def.finalizeStats(sf, nil, nil))
	if relErr(defEst, localEst) > 0.02 {
		t.Fatalf("default vs local; default=%d local=%d", defEst, localEst)
	}
}

func TestHLLMergeAlgebra(t *testing.T) {
	mk := func(lo, hi int) *hllSketch {
		var h hllSketch
		for i := lo; i < hi; i++ {
			h.addHash(hllHashUnsigned(uint64(i)))
		}
		return &h
	}
	clone := func(src *hllSketch) *hllSketch {
		var dst hllSketch
		state := src.appendState(nil)
		if _, err := dst.unmarshalState(state); err != nil {
			t.Fatalf("clone: %s", err)
		}
		return &dst
	}

	a := mk(0, 500)
	b := mk(250, 750)
	c := mk(500, 1000)

	// commutative: merge(a,b) ≈ merge(b,a)
	ab := clone(a)
	ab.merge(clone(b))
	ba := clone(b)
	ba.merge(clone(a))
	if relErr(ab.estimate(), ba.estimate()) > 0.01 {
		t.Fatalf("commutative failed: ab=%d ba=%d", ab.estimate(), ba.estimate())
	}

	// associative: (a∪b)∪c ≈ a∪(b∪c)
	left := clone(a)
	left.merge(clone(b))
	left.merge(clone(c))
	right := clone(b)
	right.merge(clone(c))
	rightOuter := clone(a)
	rightOuter.merge(right)
	if relErr(left.estimate(), rightOuter.estimate()) > 0.01 {
		t.Fatalf("associative failed: left=%d right=%d", left.estimate(), rightOuter.estimate())
	}

	// idempotent: merge(a,a) ≈ a
	aa := clone(a)
	aa.merge(clone(a))
	if relErr(aa.estimate(), a.estimate()) > 0.01 {
		t.Fatalf("idempotent failed: aa=%d a=%d", aa.estimate(), a.estimate())
	}
}

func TestHLLWireGoldenRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		fill func(*hllSketch)
	}{
		{"empty", func(h *hllSketch) {}},
		{"sparse", func(h *hllSketch) {
			for i := 0; i < 100; i++ {
				h.addHash(hllHashUnsigned(uint64(i)))
			}
		}},
		{"dense", func(h *hllSketch) {
			// Force sparse→dense conversion with many distinct hashes.
			for i := 0; i < 20000; i++ {
				h.addHash(hllHashUnsigned(uint64(i)))
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var h hllSketch
			tc.fill(&h)
			state := h.appendState(nil)
			if len(state) < len(hllMagic)+4 {
				t.Fatalf("state too short: %d", len(state))
			}
			if !bytes.Equal(state[:len(hllMagic)], hllMagic) {
				t.Fatalf("bad magic")
			}
			if state[4] != hllWireVersion || state[5] != hllAlgorithmID || state[6] != hllPrecision || state[7] != hllHashSchemaVersion {
				t.Fatalf("unexpected header bytes: %v", state[4:8])
			}

			var h2 hllSketch
			n, err := h2.unmarshalState(append([]byte(nil), state...))
			if err != nil {
				t.Fatalf("unmarshal: %s", err)
			}
			if tc.name == "empty" {
				if n != 0 {
					t.Fatalf("empty budget charge %d", n)
				}
			} else if n != hllStateBudgetBytes {
				t.Fatalf("budget charge %d; want %d", n, hllStateBudgetBytes)
			}
			if h.estimate() != h2.estimate() {
				t.Fatalf("estimate mismatch after round-trip: %d vs %d", h.estimate(), h2.estimate())
			}
			// Re-marshal should keep header; payload may differ for sparse due to compaction.
			state2 := h2.appendState(nil)
			if !bytes.Equal(state[:8], state2[:8]) {
				t.Fatalf("header changed after round-trip")
			}
		})
	}
}

func FuzzHLLUnmarshalState(f *testing.F) {
	var h hllSketch
	for i := 0; i < 50; i++ {
		h.addHash(hllHashUnsigned(uint64(i)))
	}
	f.Add(h.appendState(nil))
	f.Add((&hllSketch{}).appendState(nil))
	f.Add([]byte("VLHL"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var dst hllSketch
		before := append([]byte(nil), data...)
		_, err := dst.unmarshalState(data)
		if err != nil {
			// Must not mutate caller's buffer on failure in a way that panics;
			// and must leave dst empty enough to estimate 0 or stay usable.
			_ = dst.estimate()
			if !bytes.Equal(before, data) {
				// Unmarshal may read without writing; if it writes, that's a bug for fuzz inputs.
				// Our decoder only reads src, so equality should hold.
				t.Fatalf("input buffer mutated on error")
			}
			return
		}
		_ = dst.estimate()
		_ = dst.appendState(nil)
	})
}

func TestHLLAccuracyDistribution(t *testing.T) {
	cardinalities := []int{0, 1, 10, 100, 1_000, 10_000, 100_000}
	if !testing.Short() {
		cardinalities = append(cardinalities, 1_000_000)
	}

	for _, n := range cardinalities {
		namespaces := 20
		if n >= 100_000 {
			namespaces = 10
		}
		if n >= 1_000_000 {
			namespaces = 5
		}
		if n == 0 {
			namespaces = 1
		}

		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			errs := make([]float64, 0, namespaces)
			for ns := 0; ns < namespaces; ns++ {
				var h hllSketch
				for i := 0; i < n; i++ {
					// Deterministic distinct hashes per namespace.
					h.addHash(hllHashUnsigned(uint64(ns)<<40 | uint64(i)))
				}
				got := h.estimate()
				if n == 0 {
					if got != 0 {
						t.Fatalf("empty estimate=%d", got)
					}
					continue
				}
				errs = append(errs, math.Abs(float64(got)-float64(n))/float64(n))
			}
			if n == 0 {
				return
			}
			mean, rmse, p99 := errStats(errs)
			t.Logf("mean=%.4f rmse=%.4f p99=%.4f", mean, rmse, p99)
			if n >= 100_000 {
				if mean > 0.01 {
					t.Fatalf("mean abs rel err %.4f > 0.01", mean)
				}
				if rmse > 0.02 {
					t.Fatalf("rmse %.4f > 0.02", rmse)
				}
				if p99 > 0.03 {
					t.Fatalf("p99 %.4f > 0.03", p99)
				}
			} else if mean > 0.05 {
				t.Fatalf("mean abs rel err %.4f too high for n=%d", mean, n)
			}
		})
	}
}

func TestHLLStateBudgetAndHeap(t *testing.T) {
	var h hllSketch
	n := h.addHash(hllHashUnsigned(1))
	if n != hllStateBudgetBytes {
		t.Fatalf("first insert budget=%d; want %d", n, hllStateBudgetBytes)
	}
	if h.addHash(hllHashUnsigned(2)) != 0 {
		t.Fatalf("subsequent inserts must not re-charge budget")
	}

	// Dense path: insert enough to convert; budget still charged once.
	var dense hllSketch
	charged := dense.addHash(hllHashUnsigned(0))
	for i := 1; i < 20000; i++ {
		if dense.addHash(hllHashUnsigned(uint64(i))) != 0 {
			t.Fatalf("budget re-charged during growth")
		}
	}
	if charged != hllStateBudgetBytes {
		t.Fatalf("dense init budget=%d", charged)
	}

	runtime.GC()
	var ms1, ms2 runtime.MemStats
	runtime.ReadMemStats(&ms1)
	sketches := make([]hllSketch, 50)
	for i := range sketches {
		for j := 0; j < 20000; j++ {
			sketches[i].addHash(hllHashUnsigned(uint64(i)<<32 | uint64(j)))
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&ms2)
	perSketch := float64(ms2.HeapAlloc-ms1.HeapAlloc) / float64(len(sketches))
	t.Logf("approx heap per dense sketch: %.0f bytes (budget %d)", perSketch, hllStateBudgetBytes)
	// Soft check: allow measurement noise; fail only if wildly over budget.
	if perSketch > float64(hllStateBudgetBytes)*2 {
		t.Fatalf("per-sketch heap %.0f exceeds 2x budget %d", perSketch, hllStateBudgetBytes)
	}
	runtime.KeepAlive(sketches)
}

func BenchmarkHLLAdd(b *testing.B) {
	var h hllSketch
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.addHash(hllHashUnsigned(uint64(i)))
	}
}

func BenchmarkHLLMerge(b *testing.B) {
	var left, right hllSketch
	for i := 0; i < 10000; i++ {
		left.addHash(hllHashUnsigned(uint64(i)))
		right.addHash(hllHashUnsigned(uint64(i + 5000)))
	}
	state := right.appendState(nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var r hllSketch
		if _, err := r.unmarshalState(append([]byte(nil), state...)); err != nil {
			b.Fatal(err)
		}
		var l hllSketch
		ls := left.appendState(nil)
		if _, err := l.unmarshalState(ls); err != nil {
			b.Fatal(err)
		}
		l.merge(&r)
	}
}

func BenchmarkHLLEstimateDense(b *testing.B) {
	var h hllSketch
	for i := 0; i < 20000; i++ {
		h.addHash(hllHashUnsigned(uint64(i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.estimate()
	}
}

func newHLLStringBlockResult(name string, values []string) *blockResult {
	br := &blockResult{rowsLen: len(values)}
	vals := append([]string(nil), values...)
	br.csBuf = append(br.csBuf, blockResultColumn{
		name:          name,
		valueType:     valueTypeString,
		valuesEncoded: vals,
		values:        vals,
	})
	br.csInitFast()
	return br
}

func newHLLConstBlockResult(name, v string) *blockResult {
	br := &blockResult{rowsLen: 3}
	br.csBuf = append(br.csBuf, blockResultColumn{
		name:          name,
		isConst:       true,
		valuesEncoded: []string{v},
	})
	br.csInitFast()
	return br
}

func newHLLDictBlockResult(name string, dict []string, idxs []byte) *blockResult {
	br := &blockResult{rowsLen: len(idxs)}
	encoded := make([]string, len(idxs))
	for i, idx := range idxs {
		encoded[i] = string([]byte{idx})
	}
	br.csBuf = append(br.csBuf, blockResultColumn{
		name:          name,
		valueType:     valueTypeDict,
		dictValues:    append([]string(nil), dict...),
		valuesEncoded: encoded,
	})
	br.csInitFast()
	return br
}

func newHLLTypedUintBlockResult(name string, vt valueType, nums []uint64) *blockResult {
	br := &blockResult{rowsLen: len(nums)}
	encoded := make([]string, len(nums))
	for i, n := range nums {
		switch vt {
		case valueTypeUint8:
			encoded[i] = string([]byte{byte(n)})
		case valueTypeUint16:
			var b [2]byte
			bb := encoding.MarshalUint16(b[:0], uint16(n))
			encoded[i] = string(bb)
		case valueTypeUint32:
			var b [4]byte
			bb := encoding.MarshalUint32(b[:0], uint32(n))
			encoded[i] = string(bb)
		case valueTypeUint64:
			var b [8]byte
			bb := encoding.MarshalUint64(b[:0], n)
			encoded[i] = string(bb)
		default:
			panic(vt)
		}
	}
	br.csBuf = append(br.csBuf, blockResultColumn{
		name:          name,
		valueType:     vt,
		valuesEncoded: encoded,
	})
	br.csInitFast()
	return br
}

func newHLLTypedInt64BlockResult(name string, nums []int64) *blockResult {
	br := &blockResult{rowsLen: len(nums)}
	encoded := make([]string, len(nums))
	for i, n := range nums {
		var b [8]byte
		bb := encoding.MarshalInt64(b[:0], n)
		encoded[i] = string(bb)
	}
	br.csBuf = append(br.csBuf, blockResultColumn{
		name:          name,
		valueType:     valueTypeInt64,
		valuesEncoded: encoded,
	})
	br.csInitFast()
	return br
}

func newHLLTimeBlockResult(timestamps []int64) *blockResult {
	br := &blockResult{
		rowsLen:       len(timestamps),
		timestampsBuf: append([]int64(nil), timestamps...),
	}
	br.csBuf = append(br.csBuf, blockResultColumn{
		name:   "_time",
		isTime: true,
	})
	br.csInitFast()
	return br
}

func mustParseEstimate(t *testing.T, b []byte) uint64 {
	t.Helper()
	n, err := parseUint64(string(b))
	if err != nil {
		t.Fatalf("parse estimate %q: %s", b, err)
	}
	return n
}

func parseUint64(s string) (uint64, error) {
	var n uint64
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid digit")
		}
		n = n*10 + uint64(ch-'0')
	}
	return n, nil
}

func relErr(got, want uint64) float64 {
	if want == 0 {
		if got == 0 {
			return 0
		}
		return 1
	}
	return math.Abs(float64(got)-float64(want)) / float64(want)
}

func errStats(errs []float64) (mean, rmse, p99 float64) {
	if len(errs) == 0 {
		return 0, 0, 0
	}
	sum := 0.0
	sumSq := 0.0
	for _, e := range errs {
		sum += e
		sumSq += e * e
	}
	mean = sum / float64(len(errs))
	rmse = math.Sqrt(sumSq / float64(len(errs)))
	sorted := append([]float64(nil), errs...)
	sort.Float64s(sorted)
	idx := int(math.Ceil(0.99*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	p99 = sorted[idx]
	return mean, rmse, p99
}