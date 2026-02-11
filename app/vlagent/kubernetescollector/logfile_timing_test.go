package kubernetescollector

import (
	"path/filepath"
	"strings"
	"testing"
)

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
			lf.readLines(stopCh, proc)
			if lf.offset != totalBytes {
				b.Fatalf("unexpected offset; got %d; want %d", lf.offset, totalBytes)
			}

			// Reset state to re-read the file from the beginning in the next iteration.
			lf.setOffset(0)
		}
	})
}

type noopLogFileHandler struct{}

func (noopLogFileHandler) tryAddLine(_ []byte) (bool, error) {
	return true, nil
}

func (noopLogFileHandler) mustClose() {}
