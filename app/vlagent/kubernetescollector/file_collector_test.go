package kubernetescollector

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestFileCollector(t *testing.T) {
	checkpointsPath := filepath.Join(t.TempDir(), "checkpoints.json")
	logFilePath, inode := createTestLogFile(t)

	f := func(resultsExpected []string, inodeExpected uint64, offsetExpected int) {
		t.Helper()

		proc := newTestLogFileProcessor()
		pw := newProcessorWrapper(proc, len(resultsExpected))
		newProc := func(_ []logstorage.Field) processor {
			return pw
		}

		fc := startFileCollector(checkpointsPath, nil, newProc)

		fc.startRead(logFilePath, nil)
		pw.wait()
		fc.stop()

		if err := proc.verify(resultsExpected); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		cpExpected := checkpoint{
			Path:   logFilePath,
			Inode:  inodeExpected,
			Offset: int64(offsetExpected),
		}
		cpGot, ok := fc.checkpointsDB.get(logFilePath)
		if !ok || !reflect.DeepEqual(cpGot, cpExpected) {
			t.Fatalf("unexpected checkpoint; got: %+v;want: %+v", cpGot, cpExpected)
		}
	}

	// Test that the collector reads all log lines from the given log file.
	in := []string{"line1", "line2", "line3", "line4", "line5"}
	resultsExpected := in
	offsetExpected := len("line1\nline2\nline3\nline4\nline5\n")
	writeLinesToFile(t, logFilePath, in...)
	f(resultsExpected, inode, offsetExpected)

	// Test that the collector continues reading from the last read offset after restart.
	writeLinesToFile(t, logFilePath, "line6", "line7")
	resultsExpected = []string{"line6", "line7"}
	offsetExpected = len("line1\nline2\nline3\nline4\nline5\nline6\nline7\n")
	f(resultsExpected, inode, offsetExpected)

	// Test that the collector switches to the next log file after rotation.
	writeLinesToFile(t, logFilePath, "1", "22")
	newInode := rotateLogFile(t, logFilePath)
	writeLinesToFile(t, logFilePath, "333")
	resultsExpected = []string{"1", "22", "333"}
	offsetExpected = len("333\n")
	f(resultsExpected, newInode, offsetExpected)
}

func TestCommitPartialLines(t *testing.T) {
	checkpointsPath := filepath.Join(t.TempDir(), "checkpoints.json")
	logFilePath, inode := createTestLogFile(t)

	f := func(readLinesExpected int, resultsExpected string, inodeExpected uint64, offsetExpected int) {
		t.Helper()

		storage := newTestStorage()
		lfp := newLogFileProcessor(storage, nil)
		pw := newProcessorWrapper(lfp, readLinesExpected)
		newProc := func(_ []logstorage.Field) processor {
			return pw
		}

		fc := startFileCollector(checkpointsPath, nil, newProc)
		fc.startRead(logFilePath, nil)

		pw.wait()
		fc.stop()

		if err := storage.verify(resultsExpected); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		cpExpected := checkpoint{
			Path:   logFilePath,
			Inode:  inodeExpected,
			Offset: int64(offsetExpected),
		}
		cpGot, ok := fc.checkpointsDB.get(logFilePath)
		if !ok || !reflect.DeepEqual(cpGot, cpExpected) {
			t.Fatalf("unexpected checkpoint; got: %+v;want: %+v", cpGot, cpExpected)
		}
	}

	// Verify that the collector commits only the full line to the checkpointsDB.
	writeLinesToFile(t, logFilePath, "2025-10-16T15:37:36.1Z stderr F full line", "2025-10-16T15:37:36.1Z stderr P foo ")
	readLinesExpected := 2
	resultsExpected := `{"_msg":"full line","_stream":"{}","_time":"2025-10-16T15:37:36.1Z"}`
	offsetExpected := len("2025-10-16T15:37:36.1Z stderr F full line\n")
	f(readLinesExpected, resultsExpected, inode, offsetExpected)

	// Write another partial line to the rotated log file to ensure that the collector switches to the new file.
	newInode := rotateLogFile(t, logFilePath)
	writeLinesToFile(t, logFilePath, "2025-10-16T15:37:36.1Z stderr P bar ")
	readLinesExpected = 2
	resultsExpected = ""
	f(readLinesExpected, resultsExpected, inode, offsetExpected)

	// Write a final line to the rotated log file and verify that the collector commits the full line to the checkpointsDB.
	writeLinesToFile(t, logFilePath, "2025-10-16T15:37:36.1Z stderr F buz")
	readLinesExpected = 3
	resultsExpected = `{"_msg":"foo bar buz","_stream":"{}","_time":"2025-10-16T15:37:36.1Z"}`
	offsetExpected = len("2025-10-16T15:37:36.1Z stderr P bar \n" + "2025-10-16T15:37:36.1Z stderr F buz\n")
	f(readLinesExpected, resultsExpected, newInode, offsetExpected)
}

type processorWrapper struct {
	proc processor
	wg   *sync.WaitGroup
}

func newProcessorWrapper(proc processor, n int) *processorWrapper {
	wg := &sync.WaitGroup{}
	wg.Add(n)

	return &processorWrapper{
		proc: proc,
		wg:   wg,
	}
}

func (pw *processorWrapper) tryAddLine(line []byte) bool {
	ok := pw.proc.tryAddLine(line)
	pw.wg.Done()
	return ok
}

func (pw *processorWrapper) mustClose() {
	pw.proc.mustClose()
}

func (pw *processorWrapper) wait() {
	pw.wg.Wait()
}

type testLogFileProcessor struct {
	logLines []string
}

func newTestLogFileProcessor() *testLogFileProcessor {
	return &testLogFileProcessor{}
}

func (lfp *testLogFileProcessor) tryAddLine(line []byte) bool {
	lfp.logLines = append(lfp.logLines, string(line))
	return true
}

func (lfp *testLogFileProcessor) mustClose() {}

func (lfp *testLogFileProcessor) verify(expected []string) error {
	got := strings.Join(lfp.logLines, "\n")
	want := strings.Join(expected, "\n")
	if got != want {
		return fmt.Errorf("unexpected log lines;\ngot:\n%q\nwant:\n%q", got, want)
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
