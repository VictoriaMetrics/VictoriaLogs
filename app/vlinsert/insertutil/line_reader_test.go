package insertutil

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
	"testing/iotest"
)

func TestLineReader_Success(t *testing.T) {
	f := func(data string, linesExpected []string) {
		t.Helper()

		r := bytes.NewBufferString(data)
		lr := NewLineReader("foo", r)
		var lines []string
		for lr.NextLine() {
			lines = append(lines, string(lr.Line))
		}
		if err := lr.Err(); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if lr.NextLine() {
			t.Fatalf("expecting error on the second call to NextLine()")
		}
		if len(lr.Line) > 0 {
			t.Fatalf("unexpected non-empty line after failed NextLine(): %q", lr.Line)
		}
		if !reflect.DeepEqual(lines, linesExpected) {
			t.Fatalf("unexpected lines\ngot\n%q\nwant\n%q", lines, linesExpected)
		}
	}

	f("", nil)
	f("\n", []string{""})
	f("\n\n", []string{"", ""})
	f("foo", []string{"foo"})
	f("foo\n", []string{"foo"})
	f("\nfoo", []string{"", "foo"})
	f("foo\n\n", []string{"foo", ""})
	f("foo\nbar", []string{"foo", "bar"})
	f("foo\nbar\n", []string{"foo", "bar"})
	f("\nfoo\n\nbar\n\n", []string{"", "foo", "", "bar", ""})
}

func TestLineReader_SkipUntilNextLine(t *testing.T) {
	f := func(data string, linesExpected []string) {
		t.Helper()

		r := bytes.NewBufferString(data)
		lr := NewLineReader("foo", r)
		var lines []string
		for lr.NextLine() {
			lines = append(lines, string(lr.Line))
		}
		if err := lr.Err(); err != nil {
			t.Fatalf("unexpected error for data=%q: %s", data, err)
		}
		if lr.NextLine() {
			t.Fatalf("expecting error on the second call to NextLine()")
		}
		if !reflect.DeepEqual(lines, linesExpected) {
			t.Fatalf("unexpected lines for data=%q\ngot\n%q\nwant\n%q", data, lines, linesExpected)
		}
	}

	for _, overflow := range []int{0, 100, MaxLineSizeBytes.IntN(), MaxLineSizeBytes.IntN() + 1, 2 * MaxLineSizeBytes.IntN()} {
		longLineLen := MaxLineSizeBytes.IntN() + overflow
		longLine := string(make([]byte, longLineLen))

		// Single long line
		data := longLine
		f(data, nil)

		// Multiple long lines
		data = longLine + "\n" + longLine
		f(data, []string{""})

		data = longLine + "\n" + longLine + "\n"
		f(data, []string{"", ""})

		// Long line in the middle
		data = "foo\n" + longLine + "\nbar"
		f(data, []string{"foo", "", "bar"})

		// Multiple long lines in the middle
		data = "foo\n" + longLine + "\n" + longLine + "\nbar"
		f(data, []string{"foo", "", "", "bar"})

		// Long line in the end
		data = "foo\n" + longLine
		f(data, []string{"foo"})

		// Long line in the end
		data = "foo\n" + longLine + "\n"
		f(data, []string{"foo", ""})
	}
}

func TestLineReader_SkipUntilNextLineDataWithEOF(t *testing.T) {
	f := func(data string, linesExpected []string) {
		t.Helper()

		// iotest.DataErrReader returns the last chunk of data together with io.EOF, as HTTP request body readers may do.
		// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1827
		r := iotest.DataErrReader(bytes.NewBufferString(data))
		lr := NewLineReader("foo", r)
		var lines []string
		for lr.NextLine() {
			lines = append(lines, string(lr.Line))
		}
		if err := lr.Err(); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if lr.NextLine() {
			t.Fatalf("expecting error on the second call to NextLine()")
		}
		if !reflect.DeepEqual(lines, linesExpected) {
			t.Fatalf("unexpected lines\ngot\n%q\nwant\n%q", lines, linesExpected)
		}
	}

	longLine := string(make([]byte, MaxLineSizeBytes.IntN()+100))

	// Short lines after the long line
	f("foo\n"+longLine+"\nbar\nbaz", []string{"foo", "", "bar", "baz"})
	f("foo\n"+longLine+"\nbar\nbaz\n", []string{"foo", "", "bar", "baz"})

	// Multiple long lines
	f("foo\n"+longLine+"\n"+longLine+"\nbar", []string{"foo", "", "", "bar"})
	f(longLine+"\n"+longLine+"\n", []string{"", ""})

	// Long line in the end
	f("foo\n"+longLine, []string{"foo"})
	f("foo\n"+longLine+"\n", []string{"foo", ""})
}

func TestLineReader_Failure(t *testing.T) {
	f := func(data string, linesExpected []string) {
		t.Helper()

		fr := &failureReader{
			r: bytes.NewBufferString(data),
		}
		lr := NewLineReader("foo", fr)
		var lines []string
		for lr.NextLine() {
			lines = append(lines, string(lr.Line))
		}
		if err := lr.Err(); err == nil {
			t.Fatalf("expecting non-nil error")
		}
		if lr.NextLine() {
			t.Fatalf("expecting error on the second call to NextLine()")
		}
		if err := lr.Err(); err == nil {
			t.Fatalf("expecting non-nil error on the second call")
		}
		if !reflect.DeepEqual(lines, linesExpected) {
			t.Fatalf("unexpected lines\ngot\n%q\nwant\n%q", lines, linesExpected)
		}
	}

	f("", nil)
	f("foo", nil)
	f("foo\n", []string{"foo"})
	f("\n", []string{""})
	f("foo\nbar", []string{"foo"})
	f("foo\nbar\n", []string{"foo", "bar"})
	f("\nfoo\nbar\n\n", []string{"", "foo", "bar", ""})

	// long line
	longLineLen := MaxLineSizeBytes.IntN()
	for _, overflow := range []int{0, 100, MaxLineSizeBytes.IntN(), MaxLineSizeBytes.IntN() + 1, 2 * MaxLineSizeBytes.IntN()} {
		longLine := string(make([]byte, longLineLen+overflow))

		data := longLine
		f(data, nil)

		data = "foo\n" + longLine
		f(data, []string{"foo"})

		data = longLine + "\nfoo"
		f(data, []string{""})

		data = longLine + "\nfoo\n"
		f(data, []string{"", "foo"})
	}
}

type failureReader struct {
	r io.Reader
}

func (r *failureReader) Read(p []byte) (int, error) {
	n, _ := r.r.Read(p)
	if n > 0 {
		return n, nil
	}
	return 0, fmt.Errorf("some error")
}
