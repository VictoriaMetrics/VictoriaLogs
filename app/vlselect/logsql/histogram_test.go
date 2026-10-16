package logsql

import (
	"reflect"
	"testing"
)

func TestAppendHistogramBuckets_Success(t *testing.T) {
	f := func(s string, resultExpected []histogramBucket) {
		t.Helper()

		prefix := []histogramBucket{{VMRange: "prefix", Hits: 1}}
		result, ok := appendHistogramBuckets(prefix, s)
		if !ok {
			t.Fatalf("cannot parse %q", s)
		}
		resultExpected = append([]histogramBucket{{VMRange: "prefix", Hits: 1}}, resultExpected...)
		if !reflect.DeepEqual(result, resultExpected) {
			t.Fatalf("unexpected result for %q\ngot\n%v\nwant\n%v", s, result, resultExpected)
		}
	}

	f(`[]`, nil)
	f(`[{"vmrange":"1.000e+00...1.136e+00","hits":0}]`, []histogramBucket{{VMRange: "1.000e+00...1.136e+00", Hits: 0}})
	f(`[{"vmrange":"8.799e-01...1.000e+00","hits":18446744073709551615},{"vmrange":"1.896e+00...2.154e+00","hits":42},{"vmrange":"","hits":7}]`, []histogramBucket{
		{VMRange: "8.799e-01...1.000e+00", Hits: 18446744073709551615},
		{VMRange: "1.896e+00...2.154e+00", Hits: 42},
		{VMRange: "", Hits: 7},
	})
}

func TestAppendHistogramBuckets_Failure(t *testing.T) {
	f := func(s string) {
		t.Helper()

		prefix := []histogramBucket{{VMRange: "prefix", Hits: 1}}
		result, ok := appendHistogramBuckets(prefix, s)
		if ok {
			t.Fatalf("expecting parse failure for %q; got %v", s, result)
		}
		if !reflect.DeepEqual(result, prefix) {
			t.Fatalf("dst must be left unchanged on failure for %q; got %v", s, result)
		}
	}

	f(``)
	f(`[`)
	f(`]`)
	f(`[{}]`)
	f(`[],`)
	f(`[]x`)
	f(`[{"vmrange":"a","hits":1}`)
	f(`[{"vmrange":"a","hits":1},]`)
	f(`[{"vmrange":"a","hits":1}{"vmrange":"b","hits":2}]`)
	f(`[{"vmrange":"a","hits":}]`)
	f(`[{"vmrange":"a","hits":-1}]`)
	f(`[{"vmrange":"a","hits":1.5}]`)
	f(`[{"vmrange":"a","hits":18446744073709551616}]`)
	f(`[{"vmrange":"a","hits":99999999999999999999}]`)
	f(`[{"vmrange":"a\"b","hits":1}]`)
	f(`[{"vmrange":"a","hits":1}] `)
	f(`[{"vmrange":"a","hits":1,"x":2}]`)
	f(`[{"hits":1,"vmrange":"a"}]`)
}
