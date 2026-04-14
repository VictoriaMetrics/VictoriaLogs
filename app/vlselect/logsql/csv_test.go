package logsql

import (
	"testing"
)

func TestAppendCSVField(t *testing.T) {
	f := func(s, resultExpected string) {
		t.Helper()

		result := appendCSVField(nil, s)
		if string(result) != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}

	f("", "")
	f(" ", " ")
	f(`"`, `""""`)
	f(`,`, `","`)
	f("\n", "\"\n\"")
	f(`\"`, `"\"""`)
	f(`\n`, `\n`)
	f("\r", "\r")
	f("\t", "\t")

	f(` foo, bar" baz`, `" foo, bar"" baz"`)
	f(`foo bar" "baz`, `"foo bar"" ""baz"`) // test multiple quotes
}

func TestAppendCSVLine(t *testing.T) {
	f := func(fields []string, resultExpected string) {
		t.Helper()

		result := appendCSVLine(nil, fields)
		if string(result) != resultExpected {
			t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", result, resultExpected)
		}
	}

	f(nil, "\n")
	f([]string{"foo"}, "foo\n")
	f([]string{"a", "", "b"}, "a,,b\n")
	f([]string{"a,b", `"cd"`, "a\nb,c\"d"}, `"a,b","""cd""","a`+"\n"+`b,c""d"`+"\n")
}
