package logstorage

import (
	"testing"
)

func TestStreamTagsUnmarshalStringInplace_Success(t *testing.T) {
	f := func(s string) {
		t.Helper()

		var st StreamTags
		if err := st.unmarshalStringInplace(s); err != nil {
			t.Fatalf("unexpected error in unmarshalStringInplace(%s): %s", s, err)
		}
		result := st.String()
		if result != s {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", result, s)
		}
	}

	f(`{}`)
	f(`{foo="bar"}`)
	f(`{a="b",c="d"}`)
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

func TestNormalizeStreamTagsCanonical(t *testing.T) {
	f := func(streamTags, expected, fieldsStr string) {
		t.Helper()

		st := GetStreamTags()
		defer PutStreamTags(st)
		if err := st.unmarshalStringInplace(streamTags); err != nil {
			t.Fatalf("cannot unmarshal stream tags: %s", err)
		}

		p := getLogfmtParser()
		defer putLogfmtParser(p)
		p.parse(fieldsStr)

		st.normalize(p.fields)

		got := st.String()
		if got != expected {
			t.Fatalf("unexpected result\ngot\n%q\nwant\n%q", got, expected)
		}
	}

	f(`{}`, `{}`, ``)
	f(`{}`, `{}`, `a=b c=d`)
	f(`{a="b"}`, `{a="b"}`, `a=b`)
	f(`{a="b"}`, `{a="b"}`, `x=y a=b q=w`)
	f(`{a="b",c="d"}`, `{a="b",c="d"}`, `c=d x=y a=b`)
	f(`{a="b"}`, `{a="b"}`, `a=b x=y a=b`)

	// missing value
	f(`{a="b"}`, `{}`, ``)
	f(`{a="b"}`, `{}`, `x=y`)

	// value mismatch
	f(`{a="b"}`, `{a="c"}`, `a=c`)
	f(`{a="b",c="d"}`, `{a="c",c="a"}`, `c=a x=y a=c`)

	// multiple fields with the same name
	f(`{a="b"}`, `{a="b"}`, `a=b x=y a=c`)
	f(`{a="b"}`, `{a="c"}`, `a=c a=b x=y`)
}
