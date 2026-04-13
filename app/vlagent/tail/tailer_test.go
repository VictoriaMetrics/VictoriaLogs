package tail

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTailer(t *testing.T) {
	checkpointsPath := filepath.Join(t.TempDir(), "checkpoints.json")
	logFilePath, inode := createTestLogFile(t)

	f := func(resultExpected string, linesExpected int, inodeExpected uint64, offsetExpected int) {
		t.Helper()

		tailer := Start(checkpointsPath)

		proc := newTestProcessor(nil)
		proc.expect(linesExpected)
		tailer.StartRead(logFilePath, proc)
		proc.wait()

		tailer.Stop()

		if err := proc.verify(resultExpected); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		cpGot, ok := tailer.checkpointsDB.get(logFilePath)
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

	// Verify 'rename-create' rotation: the tailer should detect the new log file and successfully resume reading after a restart.
	writeLinesToFile(t, logFilePath, "1", "22")
	rotateRenameCreate(t, logFilePath)
	inode = updateInode(t, logFilePath, inode)

	writeLinesToFile(t, logFilePath, "333")
	resultExpected = "1\n22\n333\n"
	linesExpected = 3
	offsetExpected = len("333\n")
	f(resultExpected, linesExpected, inode, offsetExpected)

	// Verify 'copy-truncate' rotation: the tailer should detect the truncation and start reading the file from the beginning after a restart.
	writeLinesToFile(t, logFilePath, "foo", "bar")
	rotateCopyTruncate(t, logFilePath)
	writeLinesToFile(t, logFilePath, "buz")
	// It's expected that 'foo' and 'bar' are lost by vlagent due to truncation.
	resultExpected = "buz\n"
	linesExpected = 1
	offsetExpected = len("buz\n")
	f(resultExpected, linesExpected, inode, offsetExpected)
}

// TestHandleRotationRenameCreate verifies that vlagent switches to the new log file by tracking inode changes.
func TestHandleRotationRenameCreate(t *testing.T) {
	checkpointsPath := filepath.Join(t.TempDir(), "checkpoints.json")
	logFilePath, _ := createTestLogFile(t)

	tailer := Start(checkpointsPath)
	defer tailer.Stop()

	proc := newTestProcessor(nil)
	tailer.StartRead(logFilePath, proc)

	for _, s := range []string{"foo", "bar", "buz"} {
		proc.expect(1)
		writeLinesToFile(t, logFilePath, s)
		proc.wait()
	}

	oldPath := rotateRenameCreate(t, logFilePath)

	// Simulate a scenario where the log file was rotated, but the old file was still appended to.
	for _, s := range []string{"1", "2", "3"} {
		proc.expect(1)
		writeLinesToFile(t, oldPath, s)
		proc.wait()
	}

	for _, s := range []string{"VictoriaMetrics", "VictoriaLogs", "VictoriaTraces"} {
		proc.expect(1)
		writeLinesToFile(t, logFilePath, s)
		proc.wait()
	}

	expected := `foo
bar
buz
1
2
3
VictoriaMetrics
VictoriaLogs
VictoriaTraces
`
	if err := proc.verify(expected); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// TestHandleRotationCopyTruncate verifies that vlagent detects log truncation by tracking file size reduction.
func TestHandleRotationCopyTruncate(t *testing.T) {
	checkpointsPath := filepath.Join(t.TempDir(), "checkpoints.json")
	logFilePath, _ := createTestLogFile(t)

	tailer := Start(checkpointsPath)
	defer tailer.Stop()

	proc := newTestProcessor(nil)
	tailer.StartRead(logFilePath, proc)

	for _, s := range []string{"foo", "bar", "buz"} {
		proc.expect(1)
		writeLinesToFile(t, logFilePath, s)
		proc.wait()
	}

	rotateCopyTruncate(t, logFilePath)

	for _, s := range []string{"ping", "pong"} {
		proc.expect(1)
		writeLinesToFile(t, logFilePath, s)
		proc.wait()
	}

	expected := `foo
bar
buz
ping
pong
`
	if err := proc.verify(expected); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
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

		tailer := Start(checkpointsPath)

		proc := newTestProcessor(commitFn)
		proc.expect(readLinesExpected)
		tailer.StartRead(logFilePath, proc)
		proc.wait()

		tailer.Stop()

		cpGot, ok := tailer.checkpointsDB.get(logFilePath)
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
	rotateRenameCreate(t, logFilePath)
	newInode := updateInode(t, logFilePath, inode)
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

		for _, s := range []string{file1, file2} {
			proc.expect(1)

			f, err := os.Create(logFilePath)
			if err != nil {
				t.Fatalf("failed to create log file: %s", err)
			}
			writeToFile(t, f, s)
			_ = f.Sync()
			_ = f.Close()

			tailer := Start(checkpointsPath)

			tailer.StartRead(logFilePath, proc)
			proc.wait()

			tailer.Stop()
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

// rotateRenameCreate rotates the currentFile using "rename-create" (aka "create" in logrotate) rotation method,
// and returns the new name of the rotated log file.
func rotateRenameCreate(t *testing.T, currentFile string) string {
	t.Helper()

	currentFile = tryResolveSymlink(currentFile)
	rotatedFile := fmt.Sprintf("%s-%d", currentFile, time.Now().UnixNano())

	if err := os.Rename(currentFile, rotatedFile); err != nil {
		t.Fatalf("failed to rename log file: %s", err)
	}
	f, err := os.Create(currentFile)
	if err != nil {
		t.Fatalf("failed to create new log file: %s", err)
	}
	defer f.Close()

	return rotatedFile
}

func rotateCopyTruncate(t *testing.T, currentFile string) {
	t.Helper()

	currentFile = tryResolveSymlink(currentFile)
	rotatedFile := fmt.Sprintf("%s-%d", currentFile, time.Now().UnixNano())

	current, err := os.Open(currentFile)
	if err != nil {
		t.Fatalf("failed to create new log file: %s", err)
	}
	defer current.Close()

	rotated, err := os.Create(rotatedFile)
	if err != nil {
		t.Fatalf("failed to create new log file: %s", err)
	}
	defer rotated.Close()

	if _, err := io.Copy(rotated, current); err != nil {
		t.Fatalf("failed to copy log file: %s", err)
	}

	if err := os.Truncate(currentFile, 0); err != nil {
		t.Fatalf("failed to truncate log file: %s", err)
	}
}

func updateInode(t *testing.T, filename string, oldInode uint64) uint64 {
	t.Helper()

	stat, ok := mustStat(filename)
	if !ok {
		t.Fatalf("file %q does not exist", filename)
	}
	inode := getInode(stat)
	if oldInode == inode {
		t.Fatalf("file %q already has inode %d", filename, inode)
	}
	return inode
}
