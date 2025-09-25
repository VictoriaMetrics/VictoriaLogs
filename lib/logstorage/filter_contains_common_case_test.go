package logstorage

import (
	"reflect"
	"testing"
)

func TestGetCommonCasePhrases_Success(t *testing.T) {
	f := func(phrases, resultExpected []string) {
		t.Helper()

		result, err := getCommonCasePhrases(phrases)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if !reflect.DeepEqual(result, resultExpected) {
			t.Fatalf("unexpected result\ngot\n%q\nwant\n%q", result, resultExpected)
		}
	}

	f(nil, nil)
	f([]string{""}, []string{""})
	f([]string{"foo"}, []string{"FOO", "foo"})
	f([]string{"Foo"}, []string{"FOO", "Foo", "foo"})
	f([]string{"foo", "Foo"}, []string{"FOO", "Foo", "foo"})
	f([]string{"FOO"}, []string{"FOO", "FOo", "FoO", "Foo", "fOO", "fOo", "foO", "foo"})

	f([]string{"FooBar"}, []string{"FOOBAR", "FooBar", "Foobar", "fooBar", "foobar"})
}

func TestGetCommonCasePhrases_Failure(t *testing.T) {
	f := func(phrases []string) {
		t.Helper()

		result, err := getCommonCasePhrases(phrases)
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		if result != nil {
			t.Fatalf("expecting nil result; got %q", result)
		}
	}

	// More than 10 uppercase chars
	f([]string{"FOOBARBAZAB"})
	f([]string{"FoOOBbARrBAZzABsdf"})
}
