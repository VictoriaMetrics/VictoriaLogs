package logstorage

import (
	"testing"
)

func TestJSONScannerFailure(t *testing.T) {
	f := func(data string) {
		t.Helper()

		s := GetJSONScanner()
		s.Init([]byte(data), nil, "")
		for s.NextLogMessage() {
		}
		if err := s.Error(); err == nil {
			t.Fatalf("expecting non-nil error")
		}
		PutJSONScanner(s)
	}
	f("{foo")
	f("[1,2,3]")
	f(`{"foo",}`)
}

func TestJSONScannerSuccess(t *testing.T) {
	f := func(data string, fieldPrefix string, preserveKeys []string, outputExpected string) {
		t.Helper()

		s := GetJSONScanner()
		s.Init([]byte(data), preserveKeys, fieldPrefix)
		var output []byte
		for s.NextLogMessage() {
			output = MarshalFieldsToJSON(output, s.Fields)
		}
		if err := s.Error(); err != nil {
			t.Fatalf("unexpected error error")
		}

		if string(output) != outputExpected {
			t.Fatalf("unexpected fields;\ngot\n%s\nwant\n%s", output, outputExpected)
		}
		PutJSONScanner(s)
	}

	f("{}", "", nil, "{}")
	f(`{"foo":{"bar":"baz"}}{"bar":{"baz":"bar"}}`, "", nil, `{"foo.bar":"baz"}{"bar.baz":"bar"}`)
	f(`{"foo":{"bar":{"x":"y","z":["foo"]}},"a":1,"b":true,"c":[1,2],"d":false,"e":null}`, "", nil, `{"foo.bar.x":"y","foo.bar.z":"[\"foo\"]","a":"1","b":"true","c":"[1,2]","d":"false"}`)

	// preserve foo
	f(`{"foo":{"bar":{"x":"y","z":["foo"]}},"a":1,"b":true,"c":[1,2],"d":false,"e":null}`, "", []string{"foo"}, `{"foo":"{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}}","a":"1","b":"true","c":"[1,2]","d":"false"}`)

	// preserve foo.bar
	f(`{"foo":{"bar":{"x":"y","z":["foo"]}},"a":1,"b":true,"c":[1,2],"d":false,"e":null}`, "", []string{"foo.bar"}, `{"foo.bar":"{\"x\":\"y\",\"z\":[\"foo\"]}","a":"1","b":"true","c":"[1,2]","d":"false"}`)
}
