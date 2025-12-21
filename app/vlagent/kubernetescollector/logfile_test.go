package kubernetescollector

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestReadLines(t *testing.T) {
	f := func(in []string, expected []string, expectedOffset int) {
		t.Helper()

		stopCh := t.Context().Done()
		filePath, _ := createTestLogFile(t)

		writeLinesToFile(t, filePath, in...)
		lf := newLogFile(filePath)

		proc := newTestLogFileProcessor()
		lf.readLines(stopCh, proc)

		if err := proc.verify(expected); err != nil {
			t.Fatalf("unexpected log lines: %s", err)
		}

		if lf.offset != int64(expectedOffset) {
			t.Fatalf("unexpected offset; got %d; want %d", lf.offset, expectedOffset)
		}
		if lf.commitOffset != int64(expectedOffset) {
			t.Fatalf("unexpected commitOffset; got %d; want %d", lf.commitOffset, expectedOffset)
		}
	}

	// Empty file
	in := []string{}
	expected := []string{}
	f(in, expected, 0)

	// Empty lines
	in = []string{"foo", "", "", "", "bar"}
	expected = in
	offset := len("foo\n\n\n\nbar\n")
	f(in, expected, offset)

	in = []string{"foo"}
	expected = in
	offset = len("foo\n")
	f(in, expected, offset)

	in = []string{"one", "two", "tree"}
	expected = in
	offset = len("one\ntwo\ntree\n")
	f(in, expected, offset)

	// Lines with maxLineSize
	in = []string{strings.Repeat("a", maxLogLineSize)}
	expected = in
	offset = maxLogLineSize + len("\n")
	f(in, expected, offset)

	// Lines with maxLineSize in the middle
	in = []string{"foo", strings.Repeat("b", maxLogLineSize), "bar"}
	expected = in
	offset = len("foo\n") + maxLogLineSize + len("\n") + len("bar\n")
	f(in, expected, offset)

	// Line exceeding maxLineSize
	in = []string{"foo", strings.Repeat("b", maxLogLineSize+1), "bar"}
	expected = []string{"foo", "bar"}
	offset = len("foo\n") + maxLogLineSize + 1 + len("\n") + len("bar\n")
	f(in, expected, offset)

	// Multiple lines exceeding maxLineSize
	in = []string{"foo", strings.Repeat("c", maxLogLineSize+10), strings.Repeat("d", maxLogLineSize+20), "bar"}
	expected = []string{"foo", "bar"}
	offset = len("foo\n") + maxLogLineSize + 10 + len("\n") + maxLogLineSize + 20 + len("\n") + len("bar\n")
	f(in, expected, offset)

	// Very long line
	in = []string{strings.Repeat("e", maxLogLineSize*3), "end"}
	expected = []string{"end"}
	offset = maxLogLineSize*3 + len("\n") + len("end\n")
	f(in, expected, offset)
}

var nextFileID atomic.Int64

func createTestLogFile(t *testing.T) (string, uint64) {
	id := nextFileID.Add(1)
	name := fmt.Sprintf("logfile-%d.log", id)

	logFilePath := filepath.Join(t.TempDir(), name)
	symlinkPath := filepath.Join(t.TempDir(), name)

	f, err := os.Create(logFilePath)
	if err != nil {
		t.Fatalf("failed to create log file: %s", err)
	}
	_ = f.Close()

	if err := os.Symlink(logFilePath, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %s", err)
	}

	stat, ok := mustStat(logFilePath)
	if !ok {
		t.Fatalf("failed to stat log file %q", logFilePath)
	}
	inode := getInode(stat)

	return symlinkPath, inode
}

func writeLinesToFile(t testing.TB, filePath string, lines ...string) {
	t.Helper()

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("failed to open file: %s", err)
	}
	defer f.Close()

	for _, s := range lines {
		if _, err := f.WriteString(s + "\n"); err != nil {
			t.Fatalf("failed to write to file: %s", err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("failed to sync file: %s", err)
	}
}

func BenchmarkReadLinesBigSizeLines(b *testing.B) {
	// 10 MiB per iteration: 1024 bytes per line (including newline), 10_240 lines.
	benchmarkReadLines(b, 1023, 10_240)
}

func BenchmarkReadLinesMediumSizeLines(b *testing.B) {
	// 10 MiB per iteration: 512 bytes per line (including newline), 20_480 lines.
	benchmarkReadLines(b, 511, 20_480)
}

func BenchmarkReadLinesShortSizeLines(b *testing.B) {
	// 10 MiB per iteration: 32 bytes per line (including newline), 327_680 lines.
	benchmarkReadLines(b, 31, 327_680)
}

func benchmarkReadLines(b *testing.B, lineLen, count int) {
	logFilePath := filepath.Join(b.TempDir(), "test.log")
	line := strings.Repeat("a", lineLen)
	var lines []string
	for i := 0; i < count; i++ {
		lines = append(lines, line)
	}
	writeLinesToFile(b, logFilePath, lines...)

	// Total bytes processed per iteration (includes newline).
	totalBytes := int64((lineLen + 1) * count)
	b.SetBytes(totalBytes)
	b.ReportAllocs()

	proc := noopLogFileHandler{}

	stopCh := make(chan struct{})
	b.RunParallel(func(pb *testing.PB) {
		lf := newLogFile(logFilePath)
		defer lf.close()

		for pb.Next() {
			// Reset state to re-read the file from the beginning.
			lf.setOffset(0)

			lf.readLines(stopCh, proc)
			if lf.offset != totalBytes {
				b.Fatalf("unexpected offset; got %d; want %d", lf.offset, totalBytes)
			}
		}
	})
}

type noopLogFileHandler struct{}

func (noopLogFileHandler) tryAddLine(_ []byte) bool {
	return true
}

func (noopLogFileHandler) mustClose() {}
