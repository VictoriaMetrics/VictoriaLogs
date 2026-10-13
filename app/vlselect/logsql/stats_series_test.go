package logsql

import (
	"reflect"
	"testing"
)

func TestSortAndMergeStatsSeries(t *testing.T) {
	newShard := func(series ...*statsSeries) *statsSeriesShard {
		m := make(map[string]*statsSeries)
		for _, ss := range series {
			m[ss.key] = ss
		}
		return &statsSeriesShard{m: m}
	}
	pts := func(ts ...int64) []statsPoint {
		var a []statsPoint
		for _, t := range ts {
			a = append(a, statsPoint{Timestamp: t, Value: "1"})
		}
		return a
	}

	shards := []*statsSeriesShard{
		newShard(&statsSeries{key: "c", Points: pts(5, 1)}, &statsSeries{key: "a", Points: pts(9)}),
		newShard(&statsSeries{key: "b", Points: pts(2)}, &statsSeries{key: "c", Points: pts(3)}),
		newShard(),
		newShard(&statsSeries{key: "a", Points: pts(4, 2)}, &statsSeries{key: "d", Points: pts(7)}),
		newShard(&statsSeries{key: "c", Points: pts(2)}),
	}
	result := sortAndMergeStatsSeries(shards)

	type keyPoints struct {
		key string
		ts  []int64
	}
	var got []keyPoints
	for _, ss := range result {
		kp := keyPoints{key: ss.key}
		for _, p := range ss.Points {
			kp.ts = append(kp.ts, p.Timestamp)
		}
		got = append(got, kp)
	}
	want := []keyPoints{
		{"a", []int64{2, 4, 9}},
		{"b", []int64{2}},
		{"c", []int64{1, 2, 3, 5}},
		{"d", []int64{7}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected result\ngot\n%v\nwant\n%v", got, want)
	}

	if result := sortAndMergeStatsSeries(nil); len(result) != 0 {
		t.Fatalf("unexpected non-empty result for no shards: %v", result)
	}
}
