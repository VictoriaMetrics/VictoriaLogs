package logstorage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestParseJoinLateralOptions(t *testing.T) {
	qStr := `k:foo | join lateral by (k) (* | _time:1h) options(max_subqueries=10,max_per_key=5,concurrency=2,timeout=1s)`
	lex := newLexer(qStr, 0)
	q, err := parseQuery(lex)
	if err != nil {
		t.Fatalf("parseQuery failed: %s", err)
	}
	if len(q.pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(q.pipes))
	}
	pj, ok := q.pipes[0].(*pipeJoin)
	if !ok {
		t.Fatalf("expected pipeJoin, got %T", q.pipes[0])
	}
	if !pj.isLateral {
		t.Fatalf("expected lateral join")
	}
	if pj.maxSubqueries != 10 {
		t.Fatalf("maxSubqueries mismatch: got %d", pj.maxSubqueries)
	}
	if pj.maxPerKey != 5 {
		t.Fatalf("maxPerKey mismatch: got %d", pj.maxPerKey)
	}
	if pj.concurrency != 2 {
		t.Fatalf("concurrency mismatch: got %d", pj.concurrency)
	}
	if pj.subQueryTimeout != time.Second {
		t.Fatalf("timeout mismatch: got %s", pj.subQueryTimeout)
	}
}

func TestInstantiateLateralQueryReplacesPlaceholders(t *testing.T) {
	ft := &filterTime{minTimestamp: 0, maxTimestamp: 1, stringRepr: "[0,1]"}
	q := &Query{
		f: filtersAndJoin([]filter{
			&filterExact{fieldName: "name", value: "${k}"},
			ft,
		}),
	}
	qNew, rendered, err := instantiateLateralQuery(q, []string{"k"}, []string{"abc"}, map[string]string{"pod": "p1"})
	if err != nil {
		t.Fatalf("instantiate failed: %s", err)
	}
	if qNew == nil {
		t.Fatalf("instantiate returned nil")
	}
	qs := qNew.String()
	if strings.Contains(qs, "${") {
		t.Fatalf("placeholder still present, got %q", qs)
	}
	if !strings.Contains(qs, "abc") {
		t.Fatalf("placeholder value missing, got %q", qs)
	}
	if !strings.Contains(rendered, "abc") {
		t.Fatalf("rendered missing value: %q", rendered)
	}
}

func TestRunSubqueryForKeyGuardsTimeFilter(t *testing.T) {
	q := &Query{
		f: &filterExact{fieldName: "name", value: "${k}"},
	}

	called := false
	pj := &pipeJoin{
		isLateral: true,
		q:         q,
		// hasTimeFilter intentionally false to trigger guard
		runSubQuery: func(_ context.Context, q *Query, _ writeBlockResultFunc) error {
			called = true
			return nil
		},
	}
	pjp := &pipeJoinProcessor{pj: pj}

	if rows := pjp.getOrRunSubquery([]string{"abc"}, map[string]string{"k": "abc"}); rows != nil {
		t.Fatalf("expected nil rows when no time filter")
	}
	if called {
		t.Fatalf("runSubQuery should not be called without time filter")
	}
}

func TestRunSubqueryForKeyExecutesWithTimeFilter(t *testing.T) {
	ft := &filterTime{minTimestamp: 0, maxTimestamp: 1, stringRepr: "[0,1]"}
	q := &Query{
		f: filtersAndJoin([]filter{
			&filterExact{fieldName: "name", value: "${k}"},
			ft,
		}),
	}

	called := false
	pj := &pipeJoin{
		isLateral:     true,
		q:             q,
		maxPerKey:     1,
		hasTimeFilter: true,
		byFields:      []string{"k"},
		runSubQuery: func(_ context.Context, q *Query, _ writeBlockResultFunc) error {
			qs := q.String()
			if strings.Contains(qs, "${") {
				t.Fatalf("placeholder still present, got %q", qs)
			}
			if !strings.Contains(qs, "abc") {
				t.Fatalf("expected placeholder value, got %q", qs)
			}
			called = true
			return nil
		},
	}
	pjp := &pipeJoinProcessor{pj: pj}

	_ = pjp.getOrRunSubquery([]string{"abc"}, map[string]string{"k": "abc"})

	if !called {
		t.Fatalf("runSubQuery not called")
	}
}

func TestLateralCacheKeyIncludesByValues(t *testing.T) {
	ft := &filterTime{minTimestamp: 0, maxTimestamp: 1, stringRepr: "[0,1]"}
	q := &Query{f: ft}

	callCount := 0
	pj := &pipeJoin{
		isLateral: true,
		q:         q,
		byFields:  []string{"k"},
		runSubQuery: func(_ context.Context, _ *Query, writeBlock writeBlockResultFunc) error {
			callCount++
			br := &blockResult{
				rowsLen: 1,
			}
			br.csAdd(blockResultColumn{
				name:          "val",
				isConst:       true,
				valueType:     valueTypeString,
				valuesEncoded: []string{fmt.Sprintf("v%d", callCount)},
			})
			writeBlock(0, br)
			return nil
		},
	}
	pjp := &pipeJoinProcessor{pj: pj}

	rowsA := pjp.getOrRunSubquery([]string{"a"}, map[string]string{"k": "a"})
	rowsB := pjp.getOrRunSubquery([]string{"b"}, map[string]string{"k": "b"})

	if callCount != 2 {
		t.Fatalf("expected subquery executed twice for distinct keys; got %d", callCount)
	}
	if gotA, gotB := rowsA[0][0].Value, rowsB[0][0].Value; gotA == gotB {
		t.Fatalf("expected different cached results per key; got %q and %q", gotA, gotB)
	}
}

func TestLateralPrefixApplied(t *testing.T) {
	ft := &filterTime{minTimestamp: 0, maxTimestamp: 1, stringRepr: "[0,1]"}
	q := &Query{f: ft}

	pj := &pipeJoin{
		isLateral: true,
		q:         q,
		byFields:  []string{"k"},
		prefix:    "p.",
		runSubQuery: func(_ context.Context, _ *Query, writeBlock writeBlockResultFunc) error {
			br := &blockResult{
				rowsLen: 1,
			}
			br.csAdd(blockResultColumn{
				name:          "foo",
				isConst:       true,
				valueType:     valueTypeString,
				valuesEncoded: []string{"bar"},
			})
			writeBlock(0, br)
			return nil
		},
	}
	pjp := &pipeJoinProcessor{pj: pj}

	rows := pjp.getOrRunSubquery([]string{"a"}, map[string]string{"k": "a"})
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("unexpected rows: %#v", rows)
	}
	if rows[0][0].Name != "p.foo" {
		t.Fatalf("expected prefixed field name, got %q", rows[0][0].Name)
	}
	if rows[0][0].Value != "bar" {
		t.Fatalf("unexpected value: %q", rows[0][0].Value)
	}
}

func TestLateralSubqueryUsesTimeoutContext(t *testing.T) {
	ft := &filterTime{minTimestamp: 0, maxTimestamp: 1, stringRepr: "[0,1]"}
	q := &Query{f: ft}

	called := false
	pj := &pipeJoin{
		isLateral:       true,
		q:               q,
		byFields:        []string{"k"},
		subQueryTimeout: time.Millisecond,
		runSubQuery: func(ctx context.Context, _ *Query, _ writeBlockResultFunc) error {
			called = true
			if _, ok := ctx.Deadline(); !ok {
				t.Fatalf("expected timeout deadline set on context")
			}
			return nil
		},
	}
	pjp := &pipeJoinProcessor{pj: pj}

	_ = pjp.getOrRunSubquery([]string{"a"}, map[string]string{"k": "a"})
	if !called {
		t.Fatalf("runSubQuery not called")
	}
}

func TestParsePipeJoinSuccess(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeSuccess(t, pipeStr)
	}

	f(`join by (foo) (error)`)
	f(`join by (foo, bar) (a:b | fields x, y)`)
	f(`join by (foo) (a:b) prefix c`)
	f(`join by (foo) (bar | join by (x, z) (y))`)
	f(`join by (x) (y) inner`)
	f(`join by (x) (y) inner prefix a.b`)
}

func TestParsePipeJoinFailure(t *testing.T) {
	f := func(pipeStr string) {
		t.Helper()
		expectParsePipeFailure(t, pipeStr)
	}

	f(`join`)
	f(`join by () (abc)`)
	f(`join by (*) (abc)`)
	f(`join by (f, *) (abc)`)
	f(`join by (x)`)
	f(`join by`)
	f(`join (`)
	f(`join by (foo) bar`)
	f(`join by (x) ()`)
	f(`join by (x) (`)
	f(`join by (x) (abc`)
	f(`join (x) (y) prefix`)
	f(`join (x) (y) prefix |`)
}

func TestParsePipeJoinCanonicalizesOrder(t *testing.T) {
	pipeStr := `join by (x) (y) prefix a inner`
	lex := newLexer(pipeStr, 0)
	p, err := parsePipe(lex)
	if err != nil {
		t.Fatalf("cannot parse [%s]: %s", pipeStr, err)
	}
	if !lex.isEnd() {
		t.Fatalf("unexpected tail after parsing [%s]: %q", pipeStr, lex.s)
	}
	if got := p.String(); got != `join by (x) (y) inner prefix a` {
		t.Fatalf("unexpected canonical string: %q", got)
	}
}

func TestParseLateralPodExample(t *testing.T) {
	qStr := `_time:10m | join lateral by (kubernetes.pod_name) (level:error | filter pod:"${kubernetes.pod_name}" _time:10m) prefix err. options(max_subqueries=200,max_per_key=10,concurrency=3)`
	q, err := ParseQuery(qStr)
	if err != nil {
		t.Fatalf("cannot parse query: %s", err)
	}
	if len(q.pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(q.pipes))
	}
	pj, ok := q.pipes[0].(*pipeJoin)
	if !ok || !pj.isLateral {
		t.Fatalf("expected lateral pipeJoin, got %T", q.pipes[0])
	}
	if pj.prefix != "err." {
		t.Fatalf("unexpected prefix: %q", pj.prefix)
	}
	if pj.maxSubqueries != 200 || pj.maxPerKey != 10 || pj.concurrency != 3 {
		t.Fatalf("unexpected options: maxSubqueries=%d maxPerKey=%d concurrency=%d", pj.maxSubqueries, pj.maxPerKey, pj.concurrency)
	}
}

func TestPipeJoinUpdateNeededFields(t *testing.T) {
	f := func(s string, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected string) {
		t.Helper()
		expectPipeNeededFields(t, s, allowFilters, denyFilters, allowFiltersExpected, denyFiltersExpected)
	}

	// all the needed fields
	f("join on (x, y) (abc)", "*", "", "*", "")

	// all the needed fields, unneeded fields do not intersect with src
	f("join on (x, y) (abc) inner", "*", "f1,f2", "*", "f1,f2")

	// all the needed fields, unneeded fields intersect with src
	f("join on (x, y) (abc)", "*", "f2,x", "*", "f2")

	// needed fields do not intersect with src
	f("join on (x, y) (abc)", "f1,f2", "", "f1,f2,x,y", "")

	// needed fields intersect with src
	f("join on (x, y) (abc)", "f2,x", "", "f2,x,y", "")
}
