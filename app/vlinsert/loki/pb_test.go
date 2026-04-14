package loki

import (
	"fmt"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestParsePromLabels_Success(t *testing.T) {
	f := func(s string) {
		t.Helper()
		var fs logstorage.Fields
		if err := parsePromLabels(&fs, s); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		var a []string
		for _, f := range fs.Fields {
			a = append(a, fmt.Sprintf("%s=%q", f.Name, f.Value))
		}
		result := "{" + strings.Join(a, ", ") + "}"
		if result != s {
			t.Fatalf("unexpected result;\ngot\n%s\nwant\n%s", result, s)
		}
	}

	f("{}")
	f(`{foo="bar"}`)
	f(`{foo="bar", baz="x", y="z"}`)
	f(`{foo="ba\"r\\z\n", a="", b="\"\\"}`)
}

func TestParsePromLabels_Failure(t *testing.T) {
	f := func(s string) {
		t.Helper()
		var fs logstorage.Fields
		if err := parsePromLabels(&fs, s); err == nil {
			t.Fatalf("expecting non-nil error")
		}
	}

	f("")
	f("{")
	f(`{foo}`)
	f(`{foo=bar}`)
	f(`{foo="bar}`)
	f(`{foo="ba\",r}`)
	f(`{foo="bar" baz="aa"}`)
	f(`foobar`)
	f(`foo{bar="baz"}`)
}
