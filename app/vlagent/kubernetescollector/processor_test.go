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

func BenchmarkProcessorFullLines(b *testing.B) {
	data := [][]byte{
		[]byte(`2025-10-16T15:37:36.330062387Z stderr F foo`),
		[]byte(`2025-10-16T15:37:36.330062387Z stderr F bar`),
		[]byte(`2025-10-16T15:37:36.330062387Z stderr F buz`),
	}
	benchmarkProcessor(b, data)
}

func BenchmarkProcessorPartialLines(b *testing.B) {
	in := [][]byte{
		[]byte(`2025-10-16T15:37:36.330062387Z stderr P foo`),
		[]byte(`2025-10-16T15:37:36.330062387Z stderr P bar`),
		[]byte(`2025-10-16T15:37:36.330062387Z stderr F buz`),
	}
	benchmarkProcessor(b, in)
}

func benchmarkProcessor(b *testing.B, logLines [][]byte) {
	totalSize := 0
	for _, s := range logLines {
		totalSize += len(s)
	}
	b.SetBytes(int64(totalSize))

	storage := &benchmarkStorage{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			proc := newLogFileProcessor(storage, nil)
			for _, s := range logLines {
				proc.tryAddLine(s)
			}
		}
	})
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

func BenchmarkParseCRILine(b *testing.B) {
	line := []byte(`2025-10-16T15:37:36.330062387Z stderr F foo bar baz`)

	for b.Loop() {
		_, err := parseCRILine(line)
		if err != nil {
			b.Fatalf("cannot parse CRI log line: %s", err)
		}
	}
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

type benchmarkStorage struct{}

func (s *benchmarkStorage) MustAddRows(*logstorage.LogRows) {
}

func (s *benchmarkStorage) CanWriteData() error {
	return nil
}
