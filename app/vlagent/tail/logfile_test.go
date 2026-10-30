package tail

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestReadLines(t *testing.T) {
	f := func(in []string, expected string, expectedOffset int) {
		t.Helper()

		stopCh := t.Context().Done()
		filePath, _ := createTestLogFile(t)

		writeLinesToFile(t, filePath, in...)
		lf := newLogFile(filePath)

		proc := newTestProcessor(nil)
		proc.expect(len(in))
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
	expected := ""
	f(in, expected, 0)

	// Empty lines
	in = []string{"foo", "", "", "", "bar"}
	expected = strings.Join(in, "\n") + "\n"
	offset := len(expected)
	f(in, expected, offset)

	in = []string{"foo"}
	expected = "foo\n"
	offset = len(expected)
	f(in, expected, offset)

	in = []string{"one", "two", "three"}
	expected = strings.Join(in, "\n") + "\n"
	offset = len(expected)
	f(in, expected, offset)

	// Lines with maxLogLineSize
	in = []string{strings.Repeat("a", maxLogLineSize)}
	expected = strings.Join(in, "\n") + "\n"
	offset = maxLogLineSize + len("\n")
	f(in, expected, offset)

	// Lines with maxLogLineSize in the middle
	in = []string{"foo", strings.Repeat("b", maxLogLineSize), "bar"}
	expected = strings.Join(in, "\n") + "\n"
	offset = len("foo\n") + maxLogLineSize + len("\n") + len("bar\n")
	f(in, expected, offset)

	// Line exceeding maxLogLineSize
	in = []string{"foo", strings.Repeat("b", maxLogLineSize+1), "bar"}
	expected = strings.Join([]string{"foo", "bar"}, "\n") + "\n"
	offset = len("foo\n") + maxLogLineSize + 1 + len("\n") + len("bar\n")
	f(in, expected, offset)

	// Multiple lines exceeding maxLogLineSize
	in = []string{"foo", strings.Repeat("c", maxLogLineSize+10), strings.Repeat("d", maxLogLineSize+20), "bar"}
	expected = strings.Join([]string{"foo", "bar"}, "\n") + "\n"
	offset = len("foo\n") + maxLogLineSize + 10 + len("\n") + maxLogLineSize + 20 + len("\n") + len("bar\n")
	f(in, expected, offset)

	// Very long line
	in = []string{strings.Repeat("e", maxLogLineSize*3), "end"}
	expected = strings.Join([]string{"end"}, "\n") + "\n"
	offset = maxLogLineSize*3 + len("\n") + len("end\n")
	f(in, expected, offset)
}

func TestLogFileStatus(t *testing.T) {
	f := func(lf *logFile, statusWant logFileStatus) {
		t.Helper()

		statusGot := lf.status()
		if statusGot != statusWant {
			t.Fatalf("unexpected status; got %q; want %q", statusToString(statusGot), statusToString(statusWant))
		}

		lf.close()
	}

	dir := t.TempDir()

	// Both file and symlink do not exist.
	lf := newLogFile(dir + "/does-not-exist.log")
	f(lf, logFileStatusDeleted)

	// Symlink exists, file does not.
	filePath := dir + "/symlink-to-non-existent-file.log"
	lf = newLogFile(filePath)
	createSymlink(t, dir+"/does_not_exist.log", filePath)
	f(lf, logFileStatusNotRotated)

	// File by the path is missing, but still has hard links (renamed).
	filePath = dir + "/has-hardlinks.log"
	createFile(t, filePath, 0666)
	lf = newLogFile(filePath)
	_ = lf.tryReopen()
	if err := os.Rename(filePath, filePath+".rotated"); err != nil {
		t.Fatalf("failed to rename log file: %s", err)
	}
	f(lf, logFileStatusNotRotated)

	// File doesn't have hard links (removed).
	filePath = dir + "/has-no-hardlinks.log"
	createFile(t, filePath, 0666)
	lf = newLogFile(filePath)
	_ = lf.tryReopen()
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("failed to remove log file: %s", err)
	}
	f(lf, logFileStatusDeleted)

	// New file with zero size.
	filePath, _ = createTestLogFile(t)
	lf = newLogFile(filePath)
	f(lf, logFileStatusNotRotated)

	// New non-empty file.
	filePath, _ = createTestLogFile(t)
	writeLinesToFile(t, filePath, "foo", "bar")
	lf = newLogFile(filePath)
	f(lf, logFileStatusRotated)

	// File wasn't changed.
	filePath, _ = createTestLogFile(t)
	writeLinesToFile(t, filePath, "foo", "bar")
	lf = newLogFile(filePath)
	_ = lf.tryReopen()
	f(lf, logFileStatusNotRotated)
}

func statusToString(s logFileStatus) string {
	switch s {
	case logFileStatusNotRotated:
		return "not rotated"
	case logFileStatusRotated:
		return "rotated"
	case logFileStatusDeleted:
		return "deleted"
	default:
		panic(fmt.Sprintf("unknown logFileStatus %d", s))
	}
}

var nextFileID atomic.Int64

func createTestLogFile(t *testing.T) (string, uint64) {
	id := nextFileID.Add(1)
	name := fmt.Sprintf("logfile-%d.log", id)

	logFilePath := filepath.Join(t.TempDir(), name)
	symlinkPath := filepath.Join(t.TempDir(), name)

	createFile(t, logFilePath, 0666)
	createSymlink(t, logFilePath, symlinkPath)

	stat, exists := mustStat(logFilePath)
	if !exists {
		t.Fatalf("file %q does not exist", logFilePath)
	}
	inode := getInode(stat)

	return symlinkPath, inode
}

func createSymlink(t *testing.T, oldname, newname string) {
	t.Helper()
	if err := os.Symlink(oldname, newname); err != nil {
		t.Fatalf("failed to create symlink: %s", err)
	}
}

func writeLinesToFile(t testing.TB, filePath string, lines ...string) {
	t.Helper()
	if len(lines) == 0 {
		return
	}
	data := strings.Join(lines, "\n") + "\n"
	writeToFile(t, filePath, data)
}

func writeToFile(t testing.TB, filePath, data string) {
	t.Helper()

	if len(data) == 0 {
		return
	}

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("failed to open file: %s", err)
	}
	defer f.Close()

	if _, err := f.WriteString(data); err != nil {
		t.Fatalf("failed to write to file: %s", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("failed to sync file: %s", err)
	}
}
