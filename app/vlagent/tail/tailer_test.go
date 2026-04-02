package tail

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestTailer(t *testing.T) {
	checkpointsPath := filepath.Join(t.TempDir(), "checkpoints.json")
	logFilePath, inode := createTestLogFile(t)

	f := func(resultExpected string, linesExpected int, inodeExpected uint64, offsetExpected int) {
		t.Helper()

		proc := newTestProcessor(nil)
		proc.expect(linesExpected)
		newProc := func(_ []logstorage.Field) Processor {
			return proc
		}

		fc := Start(checkpointsPath, newProc)

		fc.StartRead(logFilePath, nil)
		proc.wait()
		fc.Stop()

		if err := proc.verify(resultExpected); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		cpGot, ok := fc.checkpointsDB.get(logFilePath)
		if !ok {
			t.Fatalf("checkpoint for %q is missing", logFilePath)
		}

		if cpGot.Inode != inodeExpected {
			t.Fatalf("unexpected inode in checkpoint; got %d; want %d", cpGot.Inode, inodeExpected)
		}
		if cpGot.Offset != int64(offsetExpected) {
			t.Fatalf("unexpected offset in checkpoint; got %d; want %d", cpGot.Offset, offsetExpected)
		}
	}

	// Test that the tailer reads all log lines from the given log file.
	resultExpected := "line1\nline2\nline3\nline4\nline5\n"
	linesExpected := 5
	offsetExpected := len(resultExpected)
	writeLinesToFile(t, logFilePath, resultExpected)
	f(resultExpected, linesExpected, inode, offsetExpected)

	// Test that the tailer continues reading from the last read offset after restart.
	resultExpected = "line6\nline7\n"
	linesExpected = 2
	offsetExpected += len(resultExpected)
	writeLinesToFile(t, logFilePath, resultExpected)
	f(resultExpected, linesExpected, inode, offsetExpected)

	// Test that the tailer switches to the next log file after rotation.
	writeLinesToFile(t, logFilePath, "1", "22")
	newInode := rotateLogFile(t, logFilePath)
	writeLinesToFile(t, logFilePath, "333")
	resultExpected = "1\n22\n333\n"
	linesExpected = 3
	offsetExpected = len("333\n")
	f(resultExpected, linesExpected, newInode, offsetExpected)
}

func TestCommitPartialLines(t *testing.T) {
	checkpointsPath := filepath.Join(t.TempDir(), "checkpoints.json")
	logFilePath, inode := createTestLogFile(t)

	f := func(isFull []bool, readLinesExpected int, inodeExpected uint64, offsetExpected int) {
		t.Helper()

		i := 0
		commitFn := func(line []byte) bool {
			full := isFull[i]
			i++
			return full
		}

		proc := newTestProcessor(commitFn)
		proc.expect(readLinesExpected)
		newProc := func(_ []logstorage.Field) Processor {
			return proc
		}

		fc := Start(checkpointsPath, newProc)
		fc.StartRead(logFilePath, nil)
		proc.wait()
		fc.Stop()

		cpGot, ok := fc.checkpointsDB.get(logFilePath)
		if !ok {
			t.Fatalf("checkpoint for %q is missing", logFilePath)
		}

		if cpGot.Inode != inodeExpected {
			t.Fatalf("unexpected inode in checkpoint; got %d; want %d", cpGot.Inode, inodeExpected)
		}
		if cpGot.Offset != int64(offsetExpected) {
			t.Fatalf("unexpected offset in checkpoint; got %d; want %d", cpGot.Offset, offsetExpected)
		}
	}

	// Verify that the tailer commits only the full line to the checkpointsDB.
	writeLinesToFile(t, logFilePath, "2025-10-16T15:37:36.1Z stderr F full line", "2025-10-16T15:37:36.1Z stderr P foo")
	isFull := []bool{true, false}
	readLinesExpected := 2
	offsetExpected := len("2025-10-16T15:37:36.1Z stderr F full line\n")
	f(isFull, readLinesExpected, inode, offsetExpected)

	// Write another partial line to the rotated log file to ensure that the tailer switches to the new file.
	newInode := rotateLogFile(t, logFilePath)
	writeLinesToFile(t, logFilePath, "2025-10-16T15:37:36.1Z stderr P bar")
	isFull = []bool{false, false}
	readLinesExpected = 2
	f(isFull, readLinesExpected, inode, offsetExpected)

	// Write a final line to the rotated log file and verify that the tailer commits the full line to the checkpointsDB.
	writeLinesToFile(t, logFilePath, "2025-10-16T15:37:36.1Z stderr F buz")
	readLinesExpected = 3
	isFull = []bool{false, false, true}
	offsetExpected = len("2025-10-16T15:37:36.1Z stderr P bar\n" + "2025-10-16T15:37:36.1Z stderr F buz\n")
	f(isFull, readLinesExpected, newInode, offsetExpected)
}

func TestRestoringFromFingerprint(t *testing.T) {
	f := func(file1, file2 string, outExpected string) {
		t.Helper()

		checkpointsPath := filepath.Join(t.TempDir(), "checkpoints.json")
		logFilePath, _ := createTestLogFile(t)

		proc := newTestProcessor(nil)
		newProc := func(_ []logstorage.Field) Processor {
			return proc
		}

		for _, s := range []string{file1, file2} {
			proc.expect(1)

			f, err := os.Create(logFilePath)
			if err != nil {
				t.Fatalf("failed to create log file: %s", err)
			}
			writeToFile(t, f, s)
			_ = f.Sync()
			_ = f.Close()

			fc := Start(checkpointsPath, newProc)
			fc.StartRead(logFilePath, nil)
			proc.wait()
			fc.Stop()
		}

		if err := proc.verify(outExpected); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	// The same fingerprints.
	file1 := "2025-10-16T15:37:36.1Z stderr F foo\n"
	file2 := file1 + "2025-10-16T15:37:36.2Z stderr F bar\n"
	expected := file2
	f(file1, file2, expected)

	// The same fingerprints with empty lines.
	file1 = "\n"
	file2 = file1 + "\n"
	expected = file2
	f(file1, file2, expected)

	// Different fingerprints.
	file1 = "2025-10-16T15:37:36.3Z stderr F foo\n"
	file2 = "2025-10-16T15:37:36.4Z stderr F bar\n"
	expected = file1 + file2
	f(file1, file2, expected)

	// Different fingerprints with empty lines.
	file1 = "2025-10-16T15:37:36.5Z stderr F foo\n"
	file2 = "\n"
	expected = file1 + file2
	f(file1, file2, expected)

	// Content length more than maxFingerprintDataLen.
	file1 = "2025-10-16T15:37:36.6Z stderr F foo bar buz 01234567890123456789001234567890\n"
	file2 = "2025-10-16T15:37:36.7Z stderr F bar\n"
	expected = file1 + file2
	f(file1, file2, expected)

	// Content length exceeds maxLogLineSize.
	file1 = "2025-10-16T15:37:36.1Z stderr F " + strings.Repeat("a", maxLogLineSize) + "\n" +
		"2025-10-16T15:37:35.8Z stderr F foo\n"
	file2 = "2025-10-16T15:37:36.9Z stderr F bar\n"
	expected = `2025-10-16T15:37:35.8Z stderr F foo
2025-10-16T15:37:36.9Z stderr F bar
`
	f(file1, file2, expected)
}

type testProcessor struct {
	lines    []string
	commitFn func([]byte) bool
	wg       sync.WaitGroup
}

func newTestProcessor(commitFn func([]byte) bool) *testProcessor {
	return &testProcessor{
		commitFn: commitFn,
	}
}

func (p *testProcessor) expect(n int) {
	p.wg.Add(n)
}

func (p *testProcessor) TryAddLine(line []byte) bool {
	defer p.wg.Done()
	p.lines = append(p.lines, string(line))
	commit := p.commitFn == nil || p.commitFn(line)
	return commit
}

func (p *testProcessor) Flush() {}

func (p *testProcessor) MustClose() {}

func (p *testProcessor) wait() {
	p.wg.Wait()
}

func (p *testProcessor) verify(expected string) error {
	got := ""
	if len(p.lines) > 0 {
		got = strings.Join(p.lines, "\n") + "\n"
	}
	if got != expected {
		return fmt.Errorf("unexpected log lines;\ngot:\n%q\nwant:\n%q", got, expected)
	}
	return nil
}

func rotateLogFile(t *testing.T, logFilePath string) uint64 {
	t.Helper()

	oldFileName := tryResolveSymlink(logFilePath)
	newFileName := fmt.Sprintf("%s-%d", oldFileName, time.Now().UnixNano())
	if err := os.Rename(oldFileName, newFileName); err != nil {
		t.Fatalf("failed to rename log file: %s", err)
	}
	f, err := os.Create(oldFileName)
	if err != nil {
		t.Fatalf("failed to create new log file: %s", err)
	}
	defer f.Close()

	stat, ok := mustStat(oldFileName)
	if !ok {
		t.Fatalf("failed to stat log file %q", oldFileName)
	}
	inode := getInode(stat)

	return inode
}
