package logstorage

import (
	"reflect"
	"testing"
)

func TestNewPatternMatcher(t *testing.T) {
	f := func(s string, separatorsExpected []string, placeholdersExpected []patternMatcherPlaceholder) {
		t.Helper()

		pm := newPatternMatcher(s, false)

		pmStr := pm.String()
		if s != pmStr {
			t.Fatalf("unexpected string representation of patternMatcher\ngot\n%q\nwant\n%q", pmStr, s)
		}

		if !reflect.DeepEqual(pm.separators, separatorsExpected) {
			t.Fatalf("unexpected separators; got %q; want %q", pm.separators, separatorsExpected)
		}
		if !reflect.DeepEqual(pm.placeholders, placeholdersExpected) {
			t.Fatalf("unexpected placeholders; got %q; want %q", pm.placeholders, placeholdersExpected)
		}
	}

	f("", []string{""}, nil)
	f("foobar", []string{"foobar"}, nil)
	f("<N>", []string{"", ""}, []patternMatcherPlaceholder{patternMatcherPlaceholderNum})
	f("foo<N>", []string{"foo", ""}, []patternMatcherPlaceholder{patternMatcherPlaceholderNum})
	f("<N>foo", []string{"", "foo"}, []patternMatcherPlaceholder{patternMatcherPlaceholderNum})
	f("<N><UUID>foo<IP4><TIME>bar<DATETIME><DATE><W>", []string{"", "", "foo", "", "bar", "", "", ""}, []patternMatcherPlaceholder{
		patternMatcherPlaceholderNum,
		patternMatcherPlaceholderUUID,
		patternMatcherPlaceholderIP4,
		patternMatcherPlaceholderTime,
		patternMatcherPlaceholderDateTime,
		patternMatcherPlaceholderDate,
		patternMatcherPlaceholderWord,
	})

	// unknown placeholders
	f("<foo><BAR> baz<X>y:<M>", []string{"<foo><BAR> baz<X>y:<M>"}, nil)
	f("<foo><BAR> baz<X>y<N>:<M>", []string{"<foo><BAR> baz<X>y", ":<M>"}, []patternMatcherPlaceholder{patternMatcherPlaceholderNum})
}

func TestPatternMatcherMatch(t *testing.T) {
	f := func(pattern, s string, isFull, resultExpected bool) {
		t.Helper()

		pm := newPatternMatcher(pattern, isFull)
		result := pm.Match(s)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
	}

	// an empty pattern matches an empty string
	f("", "", false, true)
	f("", "", true, true)

	// an empty pattern matches any string in non-full mode
	f("", "foo", false, true)

	// an empty pattern doesn't match non-empty string in full mode
	f("", "foo", true, false)

	// pattern without paceholders, which doesn't match the given string
	f("foo", "abcd", false, false)
	f("foo", "abcd", true, false)
	f("foo", "afoo bc", true, false)

	// pattern without placeholders, which matches the given string
	f("foo", "foo", false, true)
	f("foo", "foo", true, true)
	f("foo", "afoo bc", false, true)

	// pattern with placeholders
	f("<N>sec at <DATE>", "123sec at 2025-12-20", false, true)
	f("<N>sec at <DATE>", "123sec at 2025-12-20", true, true)

	// superflouos prefix in the string
	f("<N>sec at <DATE>", "3 123sec at 2025-12-20", true, false)
	f("<N>sec at <DATE>", "3 123sec at 2025-12-20", false, true)

	// superflouous suffix in the string
	f("<N>sec at <DATE>", "123sec at 2025-12-20 sss", true, false)
	f("<N>sec at <DATE>", "123sec at 2025-12-20 sss", false, true)

	// pattern with placeholders doesn't match the string
	f("<N> <DATE> foo", "123 456 foo", true, false)
	f("<N> <DATE> foo", "123 456 foo", false, false)

	// verify all the placeholders
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: '`\"\\', end', end", false, true)
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: `f\"'oo`, end", true, true)
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"some 123 prefix 10:20:30, n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: \"f\\\"o'\", end", false, true)

	// verify different cases for DATE
	f("<DATE>, <DATE>", "foo 2025/10/20, 2025-10-20 bar", false, true)
	f("<DATE>, <DATE>", "foo 2025/10/20, 2025-10-20 bar", true, false)

	// verify different cases for TIME
	f("<TIME>, <TIME>, <TIME>", "foo 10:20:30, 10:20:30.12345, 10:20:30,23434 aaa", false, true)

	// verify different cases for DATETIME
	f("<DATETIME>, <DATETIME>, <DATETIME>, <DATETIME>", "foo 2025-09-20T10:20:30Z, 2025/10/20 10:20:30.2343, 2025-10-20T30:40:50-05:10, 2025-10-20T30:40:50.1324+05:00 bar", false, true)

	// verify different cases for W
	f("email: <W>@<W>", "email: foo@bar.com", false, true)
	f("email: <W>@<W>", "email: foo@bar.com", true, false)
	f("email: <W>@<W>.<W>", "email: foo@bar.com", true, true)
	f("email: <W>@<W>", "a email: foo@bar.com", true, false)
	f("<W> foo", " foo", false, false)
	f("<W> foo", ",,, foo", false, false)
	f("<W> foo", ",,,abc foo", false, true)

	f(`"foo":<W>`, `{"foo":"bar", "baz": 123}`, false, true)
	f(`"foo":<W>`, `{"foo":"bar", "baz": 123}`, true, false)
}
