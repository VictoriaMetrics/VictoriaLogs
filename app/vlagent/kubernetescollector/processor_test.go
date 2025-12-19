package kubernetescollector

import (
	"fmt"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestProcessor(t *testing.T) {
	f := func(in []string, resultsExpected []string) {
		t.Helper()

		storage := newTestStorage()
		proc := newLogFileProcessor(storage, nil)

		for _, s := range in {
			proc.tryAddLine([]byte(s))
		}

		expected := strings.Join(resultsExpected, "\n")
		if err := storage.verify(expected); err != nil {
			t.Fatalf("unexpected result: %s", err)
		}
	}

	// Full line
	in := []string{`2025-10-16T15:37:36.330062387Z stderr F foo bar`}
	expectedContents := []string{`{"_msg":"foo bar","_stream":"{}","_time":"2025-10-16T15:37:36.330062387Z"}`}
	f(in, expectedContents)

	// Multiple full lines
	in = []string{
		`2025-10-16T15:37:36.1Z stderr F foo`,
		`2025-10-16T15:37:36.2Z stderr F bar`,
		`2025-10-16T15:37:36.3Z stderr F buz`,
		`2025-10-16T15:37:36.4Z stderr F ping`,
		`2025-10-16T15:37:36.5Z stderr F pong`,
	}
	expectedContents = []string{
		`{"_msg":"foo","_stream":"{}","_time":"2025-10-16T15:37:36.1Z"}`,
		`{"_msg":"bar","_stream":"{}","_time":"2025-10-16T15:37:36.2Z"}`,
		`{"_msg":"buz","_stream":"{}","_time":"2025-10-16T15:37:36.3Z"}`,
		`{"_msg":"ping","_stream":"{}","_time":"2025-10-16T15:37:36.4Z"}`,
		`{"_msg":"pong","_stream":"{}","_time":"2025-10-16T15:37:36.5Z"}`,
	}
	f(in, expectedContents)

	// Partial line
	in = []string{
		`2025-10-16T15:37:36Z stderr P foo`,
		`2025-10-16T15:37:36.330062387Z stderr F bar`,
	}
	expectedContents = []string{`{"_msg":"foobar","_stream":"{}","_time":"2025-10-16T15:37:36.330062387Z"}`}
	f(in, expectedContents)

	// Mixed full and partial lines
	in = []string{
		`2025-10-16T15:37:36Z stderr P foo`,
		`2025-10-16T15:37:36Z stderr P bar`,
		`2025-10-16T15:37:36.330062387Z stderr F buz`,
		`2025-10-16T15:37:36.4Z stderr F ping`,
		`2025-10-16T15:37:36Z stderr P pong`,
		`2025-10-16T15:37:36.5Z stderr F last`,
	}
	expectedContents = []string{
		`{"_msg":"foobarbuz","_stream":"{}","_time":"2025-10-16T15:37:36.330062387Z"}`,
		`{"_msg":"ping","_stream":"{}","_time":"2025-10-16T15:37:36.4Z"}`,
		`{"_msg":"ponglast","_stream":"{}","_time":"2025-10-16T15:37:36.5Z"}`,
	}
	f(in, expectedContents)

	// Max log line size
	firstLine := strings.Repeat("a", maxLogLineSize/2-len("2025-10-16T15:37:36Z stderr P "))
	secondLine := strings.Repeat("b", maxLogLineSize/2-len("2025-10-16T15:37:36.330062387Z stderr F "))
	in = []string{
		`2025-10-16T15:37:36Z stderr P ` + firstLine,
		`2025-10-16T15:37:36.330062387Z stderr F ` + secondLine,
	}
	expectedContents = []string{
		fmt.Sprintf(`{"_msg":%q,"_stream":"{}","_time":"2025-10-16T15:37:36.330062387Z"}`, firstLine+secondLine),
	}
	f(in, expectedContents)

	// Too long partial line
	in = []string{
		`2025-10-16T15:37:36Z stderr P ` + strings.Repeat("a", maxLogLineSize),
		`2025-10-16T15:37:36.330062387Z stderr F ` + strings.Repeat("b", maxLogLineSize),
		`2025-10-16T15:37:36.4Z stderr F complete line`,
	}
	expectedContents = []string{`{"_msg":"complete line","_stream":"{}","_time":"2025-10-16T15:37:36.4Z"}`}
	f(in, expectedContents)

	// Empty line
	in = []string{
		`2025-10-16T15:37:36Z stderr F `,
	}
	expectedContents = []string{}
	f(in, expectedContents)

	// Test driver json-file
	in = []string{
		`{"log":"foo\tbar","stream":"stderr","time":"2025-10-16T15:37:36.330062387Z"}`,
	}
	expectedContents = []string{`{"_msg":"foo\tbar","_stream":"{}","_time":"2025-10-16T15:37:36.330062387Z"}`}
	f(in, expectedContents)
}

func TestParseKlog(t *testing.T) {
	f := func(src, fieldsExpected string, timestampExpected int64) {
		t.Helper()

		timestamp, fields, ok := tryParseKlog(nil, src)
		if !ok {
			t.Fatalf("cannot parse klog line %q", src)
		}

		got := logstorage.MarshalFieldsToJSON(nil, fields)
		if string(got) != fieldsExpected {
			t.Fatalf("unexpected result; got:\n%s\nwant:\n%s", got, fieldsExpected)
		}

		if timestamp != timestampExpected {
			t.Fatalf("unexpected timestamp; got %d; want %d", timestamp, timestampExpected)
		}
	}

	// Parse simple line
	in := `I1215 07:34:12.017826       94 serving.go:374] foobar`
	want := `{"level":"INFO","thread_id":"94","source_line":"serving.go:374","_msg":"foobar"}`
	timestampExpected := int64(1765784052017826000)
	f(in, want, timestampExpected)

	// Parse multiple words
	in = `I1215 07:34:12.017826       24 serving.go:374] Generated self-signed cert (/tmp/apiserver.crt, /tmp/apiserver.key)`
	want = `{"level":"INFO","thread_id":"24","source_line":"serving.go:374","_msg":"Generated self-signed cert (/tmp/apiserver.crt, /tmp/apiserver.key)"}`
	timestampExpected = 1765784052017826000
	f(in, want, timestampExpected)

	// Parse key="value" pair
	in = `I1215 07:34:11.695645       42 controller.go:824] "Starting provisioner controller" component="rancher.io/local-path_local-path-provisioner-5cf85fd84d-bf8vk_626b5057-e081-4b71-9a19-5e371ae0211b"`
	want = `{"level":"INFO","thread_id":"42","source_line":"controller.go:824","_msg":"Starting provisioner controller","component":"rancher.io/local-path_local-path-provisioner-5cf85fd84d-bf8vk_626b5057-e081-4b71-9a19-5e371ae0211b"}`
	timestampExpected = 1765784051695645000
	f(in, want, timestampExpected)

	// Parse key="value" pairs
	in = `I1215 10:34:26.907803       1 server.go:191] "Failed probe" probe="metric-storage-ready" err="no metrics to serve"`
	want = `{"level":"INFO","thread_id":"1","source_line":"server.go:191","_msg":"Failed probe","probe":"metric-storage-ready","err":"no metrics to serve"}`
	timestampExpected = 1765794866907803000
	f(in, want, timestampExpected)

	// Parse quoted msg without additional fields
	in = `I1215 07:34:12.324492       1234 tlsconfig.go:240] "Starting DynamicServingCertificateController"`
	want = `{"level":"INFO","thread_id":"1234","source_line":"tlsconfig.go:240","_msg":"Starting DynamicServingCertificateController"}`
	timestampExpected = 1765784052324492000
	f(in, want, timestampExpected)
}

func TestParseKlogFailure(t *testing.T) {
	f := func(src string) {
		t.Helper()

		_, fields, ok := tryParseKlog(nil, src)
		if ok {
			got := logstorage.MarshalFieldsToJSON(nil, fields)
			t.Fatalf("unexpected success; got\n%s", got)
		}
	}

	// Empty line
	f(``)
	f(`   `)

	// Invalid timestamp
	f(`I foobar`)
	f(`Ifoobar`)
	f(`I1215 01:34:12.000000999 1 main.go:1] foo`)
	f(`I1215 01:34:12.000000`)
	f(`I1215 01:34:12.`)
	f(`I1215 01:34`)
	f(`I1215 01`)
	f(`I1215 `)
	f(`I1215`)
	f(`I12`)
	f(`I`)

	// Missing msg
	f(`I1215 07:34:12.017826       1 serving.go:374] `)
	f(`I1215 07:34:12.017826       1 serving.go:374]`)
	f(`I1215 07:34:12.017826       1 serving.go:374`)

	// Missing thread ID
	f(`I1215 07:34:12.017826`)
	f(`I1215 07:34:12.017826 `)
	f(`I1215 07:34:12.324492 1234tlsconfig.go:240] foo`)

	// Unfinished quoted msg
	f(`I1215 07:34:12.324492       1234 tlsconfig.go:240] "Starting`)

	// Unfinished key="value" pair
	f(`I1215 07:34:12.324309       1 configmap_cafile_content.go:202] "Starting controller" name="client-`)
}

func TestParseCRILine(t *testing.T) {
	f := func(line string, timestampExpected int64, partialExpected bool, contentExpected string) {
		t.Helper()
		criLine, err := parseCRILine([]byte(line))
		if err != nil {
			t.Fatalf("cannot parse CRI log line %q: %s", line, err)
		}
		if criLine.timestamp != timestampExpected {
			t.Fatalf("unexpected timestamp; got %d; want %d", criLine.timestamp, timestampExpected)
		}
		if criLine.partial != partialExpected {
			t.Fatalf("unexpected partial; got %v; want %v", criLine.partial, partialExpected)
		}
		if string(criLine.content) != contentExpected {
			t.Fatalf("unexpected content; got %q; want %q", criLine.content, contentExpected)
		}
	}

	// Full line
	f(`2025-10-16T15:37:36.330062387Z stderr F foo bar`, 1760629056330062387, false, "foo bar")

	// Partial line
	f(`2025-10-16T15:37:36Z stdout P partial log line`, 1760629056000000000, true, "partial log line")

	// Empty content
	f(`2025-10-16T15:37:36Z stdout P `, 1760629056000000000, true, "")

	// Content with spaces
	f(`2025-10-16T15:37:36Z stdout F  `, 1760629056000000000, false, " ")
	f(`2025-10-16T15:37:36Z stdout F      `, 1760629056000000000, false, "     ")
}

// Storage implements insertutil.LogRowsStorage interface
type testStorage struct {
	logRows []string
}

func newTestStorage() *testStorage {
	return &testStorage{}
}

// MustAddRows implements insertutil.LogRowsStorage interface
func (s *testStorage) MustAddRows(lr *logstorage.LogRows) {
	for i := 0; i < lr.RowsCount(); i++ {
		logRow := lr.GetRowString(i)
		s.logRows = append(s.logRows, logRow)
	}
}

// CanWriteData implements insertutil.LogRowsStorage interface
func (s *testStorage) CanWriteData() error {
	return nil
}

func (s *testStorage) verify(expected string) error {
	got := strings.Join(s.logRows, "\n")

	expected = removeRepeats(expected)
	got = removeRepeats(got)

	if got != expected {
		return fmt.Errorf("unexpected rows; got:\n%s\nwant:\n%s", got, expected)
	}
	return nil
}

// removeRepeats replaces repeated characters with text like "[repeated 200000 times]"
// only when a character repeats more than 32 times in sequence.
func removeRepeats(s string) string {
	if len(s) == 0 {
		return ""
	}

	var sb strings.Builder
	var prev rune
	var n int

	for i, r := range s {
		if i == 0 {
			prev = r
			n = 1
			continue
		}

		if r == prev {
			n++
		} else {
			if n > 32 {
				sb.WriteString(fmt.Sprintf("%c[repeated %d times]", prev, n))
			} else {
				sb.WriteString(strings.Repeat(string(prev), n))
			}
			prev = r
			n = 1
		}
	}

	// Handle the last character(s)
	if n > 32 {
		sb.WriteString(fmt.Sprintf("%c[repeated %d times]", prev, n))
	} else {
		sb.WriteString(strings.Repeat(string(prev), n))
	}

	return sb.String()
}
