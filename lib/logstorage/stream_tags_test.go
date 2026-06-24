package logstorage

import (
	"testing"
)

func TestStreamTagsUnmarshalStringInplace_Success(t *testing.T) {
	f := func(s, resultExpected string) {
		t.Helper()

		var st StreamTags
		if err := st.unmarshalStringInplace(s); err != nil {
			t.Fatalf("unexpected error in unmarshalStringInplace(%s): %s", s, err)
		}
		result := st.String()
		if result != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}

	f(`{}`, `{}`)
	f(`{foo="bar"}`, `{foo="bar"}`)
	f(`{a="b",c="d"}`, `{a="b",c="d"}`)
	f(`{c="d",a="b"}`, `{a="b",c="d"}`)
}

func TestStreamTagsUnmarshalStringInplace_Failure(t *testing.T) {
	f := func(s string) {
		t.Helper()

		var st StreamTags
		if err := st.unmarshalStringInplace(s); err == nil {
			t.Fatalf("expecting non-nil error in unmarshalStringInplace(%s)", s)
		}
	}

	f(``)
	f(`{`)
	f(`{foo}`)
	f(`{"foo":"bar"}`)
	f(`{foo=abc`)
	f(`{foo="abc`)
	f(`{foo="abc"`)
	f(`{foo="abc",`)
	f(`{foo="abc",bar}`)
}

func TestStreamTagsNormalize(t *testing.T) {
	f := func(streamTags, fieldsStr, streamTagsExpected string, isNormalizedExpected bool) {
		t.Helper()

		st := GetStreamTags()
		defer PutStreamTags(st)
		if err := st.unmarshalStringInplace(streamTags); err != nil {
			t.Fatalf("cannot unmarshal stream tags: %s", err)
		}

		p := getLogfmtParser()
		defer putLogfmtParser(p)
		p.parse(fieldsStr)

		isNormalized := st.normalize(p.fields)

		result := st.String()
		if result != streamTagsExpected {
			t.Fatalf("unexpected result\ngot\n%q\nwant\n%q", result, streamTagsExpected)
		}

		if isNormalized != isNormalizedExpected {
			t.Fatalf("unexpected isNormalized; got %v; want %v", isNormalized, isNormalizedExpected)
		}
	}

	f(`{}`, ``, `{}`, false)
	f(`{}`, `a=b c=d`, `{}`, false)
	f(`{a="b"}`, `a=b`, `{a="b"}`, false)
	f(`{a="b"}`, `x=y a=b q=w`, `{a="b"}`, false)
	f(`{a="b",c="d"}`, `c=d x=y a=b`, `{a="b",c="d"}`, false)
	f(`{a="b"}`, `a=b x=y a=b`, `{a="b"}`, false)

	// missing value
	f(`{a="b"}`, ``, `{}`, true)
	f(`{a="b",x="y"}`, `x=y`, `{x="y"}`, true)

	// value mismatch
	f(`{a="b"}`, `a=c`, `{a="c"}`, true)
	f(`{c="d",a="b"}`, `c=d x=y a=c`, `{a="c",c="d"}`, true)

	// multiple fields with the same name
	f(`{a="b"}`, `a=b x=y a=c`, `{a="c"}`, true)
	f(`{a="b",q="w"}`, `a=c a=c q=w`, `{a="c",q="w"}`, true)
}
