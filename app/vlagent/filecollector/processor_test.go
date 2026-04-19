package filecollector

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestProcessorParseContent(t *testing.T) {
	f := func(in []string, resultsExpected []string) {
		t.Helper()

		storage := newTestLogRowsStorage()
		proc := newProcessor(0, "test.log", storage)
		for _, s := range in {
			proc.TryAddLine([]byte(s))
		}

		expected := strings.Join(resultsExpected, "\n")
		got := strings.Join(storage.logRows, "\n")
		if expected != got {
			t.Errorf("expected:\n%s\ngot:\n%s", expected, got)
		}
	}

	// Empty content
	in := []string{""}
	expected := []string{}
	f(in, expected)

	// Spaces
	in = []string{"", " ", "  "}
	expected = []string{`{"_msg":" ","file":"test.log"}`, `{"_msg":"  ","file":"test.log"}`}
	f(in, expected)

	// JSON content
	in = []string{`{"_msg":"foo bar","file":"test.log"}`}
	expected = []string{`{"_msg":"foo bar","file":"test.log"}`}
	f(in, expected)

	// Started like JSON object, but it is a regular log line
	in = []string{"{foobar}"}
	expected = []string{`{"_msg":"{foobar}","file":"test.log"}`}
	f(in, expected)

	// Non-JSON content
	in = []string{
		`foo`,
		`bar`,
		`buz`,
	}
	expected = []string{
		`{"_msg":"foo","file":"test.log"}`,
		`{"_msg":"bar","file":"test.log"}`,
		`{"_msg":"buz","file":"test.log"}`,
	}
	f(in, expected)
}

func TestProcessorSetTimestamp(t *testing.T) {
	f := func(in string, timestampsExpected []int64) {
		t.Helper()

		storage := newTestLogRowsStorage()
		proc := newProcessor(0, "test.log", storage)
		proc.TryAddLine([]byte(in))
		proc.MustClose()

		if !reflect.DeepEqual(storage.timestamps, timestampsExpected) {
			t.Fatalf("unexpected timestamps; expected:\n%v\ngot:\n%v", timestampsExpected, storage.timestamps)
		}
	}

	current := time.Now()

	// RFC3339
	in := fmt.Sprintf(`{"_msg":"foo","time":%q}`, current.Format(time.RFC3339))
	expected := []int64{current.Unix() * time.Second.Nanoseconds()}
	f(in, expected)

	// RFC3339 nano
	in = fmt.Sprintf(`{"_msg":"foo","time":%q}`, current.Format(time.RFC3339Nano))
	expected = []int64{current.UnixNano()}
	f(in, expected)

	// Unix timestamp
	in = fmt.Sprintf(`{"_msg":"foo","time":%d}`, current.Unix())
	expected = []int64{current.Unix() * time.Second.Nanoseconds()}
	f(in, expected)

	// Unix timestamp with milliseconds precision
	in = fmt.Sprintf(`{"_msg":"foo","time":%d}`, current.UnixMilli())
	expected = []int64{current.UnixMilli() * time.Millisecond.Nanoseconds()}
	f(in, expected)

	// Unix timestamp with microseconds precision
	in = fmt.Sprintf(`{"_msg":"foo","time":%d}`, current.UnixMicro())
	expected = []int64{current.UnixMicro() * time.Microsecond.Nanoseconds()}
	f(in, expected)

	// Unix timestamp with nanosecond precision
	in = fmt.Sprintf(`{"_msg":"foo","time":%d}`, current.UnixNano())
	expected = []int64{current.UnixNano()}
	f(in, expected)
}

// testLogRowsStorage implements insertutil.LogRowsStorage interface.
type testLogRowsStorage struct {
	logRows    []string
	timestamps []int64
}

func newTestLogRowsStorage() *testLogRowsStorage {
	return &testLogRowsStorage{}
}

// MustAddRows implements insertutil.LogRowsStorage interface
func (s *testLogRowsStorage) MustAddRows(lr *logstorage.LogRows) {
	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		row := logstorage.MarshalFieldsToJSON(nil, r.Fields)
		s.logRows = append(s.logRows, string(row))
		s.timestamps = append(s.timestamps, r.Timestamp)
	})
}

// CanWriteData implements insertutil.LogRowsStorage interface
func (s *testLogRowsStorage) CanWriteData() error {
	return nil
}
