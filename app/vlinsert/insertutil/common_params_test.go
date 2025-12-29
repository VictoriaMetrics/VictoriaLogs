package insertutil

import (
	"net/http/httptest"
	"testing"
)

func TestGetCommonParams_RemoveEmptyTokens(t *testing.T) {
	f := func(headers map[string]string, streamFieldsExpected, timeFieldsExpected []string, isTimeFieldSetExpected bool, extraFieldsExpected int) {
		t.Helper()

		r := httptest.NewRequest("POST", "http://example.com/insert", nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}

		cp, err := GetCommonParams(r)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if !slicesEqual(cp.StreamFields, streamFieldsExpected) {
			t.Fatalf("unexpected StreamFields; got %q; want %q", cp.StreamFields, streamFieldsExpected)
		}
		if !slicesEqual(cp.TimeFields, timeFieldsExpected) {
			t.Fatalf("unexpected TimeFields; got %q; want %q", cp.TimeFields, timeFieldsExpected)
		}
		if cp.IsTimeFieldSet != isTimeFieldSetExpected {
			t.Fatalf("unexpected IsTimeFieldSet; got %v; want %v", cp.IsTimeFieldSet, isTimeFieldSetExpected)
		}
		if len(cp.ExtraFields) != extraFieldsExpected {
			t.Fatalf("unexpected ExtraFields len; got %d; want %d", len(cp.ExtraFields), extraFieldsExpected)
		}
	}

	f(map[string]string{
		"VL-Stream-Fields": "collector,,service.name",
	}, []string{"collector", "service.name"}, []string{"_time"}, false, 0)

	f(map[string]string{
		"VL-Time-Field": ",  observedTimestamp, ",
	}, nil, []string{"observedTimestamp"}, true, 0)

	f(map[string]string{
		"VL-Time-Field": ",,",
	}, nil, []string{"_time"}, false, 0)

	f(map[string]string{
		"VL-Extra-Fields": "a=b,, c=d",
	}, nil, []string{"_time"}, false, 2)
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
