package logstorage

import (
	"reflect"
	"testing"
)

func TestNewPatternMatcher(t *testing.T) {
	f := func(s string, separatorsExpected []string, placeholdersExpected []patternMatcherPlaceholder) {
		t.Helper()

		pm := newPatternMatcher(s, patternMatcherOptionAny)

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
	f := func(pattern, s string, pmo patternMatcherOption, resultExpected bool) {
		t.Helper()

		pm := newPatternMatcher(pattern, pmo)
		result := pm.Match(s)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
	}

	// an empty pattern matches an empty string
	f("", "", patternMatcherOptionAny, true)
	f("", "", patternMatcherOptionFull, true)
	f("", "", patternMatcherOptionPrefix, true)
	f("", "", patternMatcherOptionSuffix, true)

	// an empty pattern matches any string in non-full mode
	f("", "foo", patternMatcherOptionAny, true)
	f("", "foo", patternMatcherOptionPrefix, true)
	f("", "foo", patternMatcherOptionSuffix, true)

	// an empty pattern doesn't match non-empty string in full mode
	f("", "foo", patternMatcherOptionFull, false)

	// pattern without paceholders, which doesn't match the given string
	f("foo", "abcd", patternMatcherOptionAny, false)
	f("foo", "abcd", patternMatcherOptionFull, false)
	f("foo", "abcd", patternMatcherOptionPrefix, false)
	f("foo", "abcd", patternMatcherOptionSuffix, false)
	f("foo", "afoo bc", patternMatcherOptionFull, false)

	// pattern without placeholders, which matches the given string
	f("foo", "foo", patternMatcherOptionAny, true)
	f("foo", "foo", patternMatcherOptionFull, true)
	f("foo", "foo", patternMatcherOptionPrefix, true)
	f("foo", "foo", patternMatcherOptionSuffix, true)
	f("foo", "afoo bc", patternMatcherOptionAny, true)
	f("afoo", "afoo bc", patternMatcherOptionPrefix, true)
	f("bc", "afoo bc", patternMatcherOptionSuffix, true)

	// pattern with placeholders
	f("<N>sec at <DATE>", "123sec at 2025-12-20", patternMatcherOptionAny, true)
	f("<N>sec at <DATE>", "123sec at 2025-12-20", patternMatcherOptionFull, true)
	f("<N>sec at <DATE>", "123sec at 2025-12-20", patternMatcherOptionPrefix, true)
	f("<N>sec at <DATE>", "123sec at 2025-12-20", patternMatcherOptionSuffix, true)

	// superflouos prefix in the string
	f("<N>sec at <DATE>", "3 123sec at 2025-12-20", patternMatcherOptionFull, false)
	f("<N>sec at <DATE>", "3 123sec at 2025-12-20", patternMatcherOptionAny, true)
	f("<N>sec at <DATE>", "3 123sec at 2025-12-20", patternMatcherOptionPrefix, false)
	f("<N>sec at <DATE>", "3 123sec at 2025-12-20", patternMatcherOptionSuffix, true)

	// superflouous suffix in the string
	f("<N>sec at <DATE>", "123sec at 2025-12-20 sss", patternMatcherOptionFull, false)
	f("<N>sec at <DATE>", "123sec at 2025-12-20 sss", patternMatcherOptionAny, true)
	f("<N>sec at <DATE>", "123sec at 2025-12-20 sss", patternMatcherOptionPrefix, true)
	f("<N>sec at <DATE>", "123sec at 2025-12-20 sss", patternMatcherOptionSuffix, false)

	// pattern with placeholders doesn't match the string
	f("<N> <DATE> foo", "123 456 foo", patternMatcherOptionFull, false)
	f("<N> <DATE> foo", "123 456 foo", patternMatcherOptionAny, false)
	f("<N> <DATE> foo", "123 456 foo", patternMatcherOptionPrefix, false)
	f("<N> <DATE> foo", "123 456 foo", patternMatcherOptionSuffix, false)

	// verify all the placeholders
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: '`\"\\', end', end", patternMatcherOptionAny, true)
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: `f\"'oo`, end", patternMatcherOptionFull, true)
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: `f\"'oo`, end", patternMatcherOptionPrefix, true)
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: `f\"'oo`, end", patternMatcherOptionSuffix, true)
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"some 123 prefix 10:20:30, n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: \"f\\\"o'\", end", patternMatcherOptionAny, true)
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"some 123 prefix 10:20:30, n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: \"f\\\"o'\", end", patternMatcherOptionPrefix, false)
	f("n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
		"some 123 prefix 10:20:30, n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: \"f\\\"o'\", end", patternMatcherOptionSuffix, true)

	// verify different cases for DATE
	f("<DATE>, <DATE>", "foo 2025/10/20, 2025-10-20 bar", patternMatcherOptionAny, true)
	f("<DATE>, <DATE>", "foo 2025/10/20, 2025-10-20 bar", patternMatcherOptionFull, false)
	f("<DATE>, <DATE>", "foo 2025/10/20, 2025-10-20 bar", patternMatcherOptionPrefix, false)
	f("<DATE>, <DATE>", "foo 2025/10/20, 2025-10-20 bar", patternMatcherOptionSuffix, false)

	// verify different cases for TIME
	f("<TIME>, <TIME>, <TIME>", "foo 10:20:30, 10:20:30.12345, 10:20:30,23434 aaa", patternMatcherOptionAny, true)
	f("<TIME>, <TIME>, <TIME>", "foo 10:20:30, 10:20:30.12345, 10:20:30,23434 aaa", patternMatcherOptionFull, false)
	f("<TIME>, <TIME>, <TIME>", "foo 10:20:30, 10:20:30.12345, 10:20:30,23434 aaa", patternMatcherOptionPrefix, false)
	f("<TIME>, <TIME>, <TIME>", "foo 10:20:30, 10:20:30.12345, 10:20:30,23434 aaa", patternMatcherOptionSuffix, false)

	// verify different cases for DATETIME
	f("<DATETIME>, <DATETIME>, <DATETIME>, <DATETIME>", "foo 2025-09-20T10:20:30Z, 2025/10/20 10:20:30.2343, 2025-10-20T30:40:50-05:10, 2025-10-20T30:40:50.1324+05:00 bar", patternMatcherOptionAny, true)

	// verify different cases for W
	f("email: <W>@<W>", "email: foo@bar.com", patternMatcherOptionAny, true)
	f("email: <W>@<W>", "email: foo@bar.com", patternMatcherOptionFull, false)
	f("email: <W>@<W>", "email: foo@bar.com", patternMatcherOptionPrefix, true)
	f("email: <W>@<W>", "email: foo@bar.com", patternMatcherOptionSuffix, false)
	f("email: <W>@<W>.<W>", "email: foo@bar.com", patternMatcherOptionFull, true)
	f("email: <W>@<W>.<W>", "email: foo@bar.com", patternMatcherOptionPrefix, true)
	f("email: <W>@<W>.<W>", "email: foo@bar.com", patternMatcherOptionSuffix, true)
	f("email: <W>@<W>", "a email: foo@bar.com", patternMatcherOptionFull, false)
	f("<W> foo", " foo", patternMatcherOptionAny, false)
	f("<W> foo", ",,, foo", patternMatcherOptionAny, false)
	f("<W> foo", ",,,abc foo", patternMatcherOptionAny, true)

	f(`"foo":<W>`, `{"foo":"bar", "baz": 123}`, patternMatcherOptionAny, true)
	f(`"foo":<W>`, `{"foo":"bar", "baz": 123}`, patternMatcherOptionFull, false)
	f(`"foo":<W>`, `{"foo":"bar", "baz": 123}`, patternMatcherOptionPrefix, false)
	f(`"foo":<W>`, `{"foo":"bar", "baz": 123}`, patternMatcherOptionSuffix, false)
	f(`{"foo":<W>`, `{"foo":"bar", "baz": 123}`, patternMatcherOptionPrefix, true)
	f(`"baz": <N>}`, `{"foo":"bar", "baz": 123}`, patternMatcherOptionSuffix, true)
	f(`"baz": <N>`, `{"foo":"bar", "baz": 123}`, patternMatcherOptionSuffix, false)

	// match the suffix not at the end
	f("foo:<N>", `abc foo:123 abc foo:42`, patternMatcherOptionSuffix, true)
	f("foo:<N>", `abc foo:123 abc foo:`, patternMatcherOptionSuffix, false)
	f("foo:<N>", `abc foo:123 abc`, patternMatcherOptionSuffix, false)
	f("foo:<N> xx", `abc foo:123 xx foo:42 xx`, patternMatcherOptionSuffix, true)
	f("foo:<N> xx", `abc foo:123 xx foo:42`, patternMatcherOptionSuffix, false)

	// regression: leading separator present many times but placeholder doesn't match after it
	f("xx<N>", "xxxxxxxxxxxxxxxx", patternMatcherOptionAny, false)
	f("xx<N>", "xxxxxxxxxxxxxxxx", patternMatcherOptionFull, false)
	f("xx<N>", "xxxxxxxxxxxxxxxx", patternMatcherOptionPrefix, false)
	f("xx<N>", "xxxxxxxxxxxxxxxx", patternMatcherOptionSuffix, false)
	f("xx<N>", "xxxxxx123", patternMatcherOptionAny, true)
	f("xx<N>", "xxxxxx123", patternMatcherOptionFull, false)
	f("xx<N>", "xxxxxx123", patternMatcherOptionPrefix, false)
	f("xx<N>", "xxxxxx123", patternMatcherOptionSuffix, true)
}
