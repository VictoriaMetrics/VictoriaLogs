package logstorage

import (
	"strings"
	"testing"
)

/*
go test -test.fullpath=true -benchmem -run=^$ -bench ^BenchmarkTokenizeHashes$ github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage

goos: darwin
goarch: arm64
pkg: github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage
cpu: Apple M2
=== RUN   BenchmarkTokenizeHashes
BenchmarkTokenizeHashes
BenchmarkTokenizeHashes-8         774218              1476 ns/op        2267.84 MB/s           0 B/op          0 allocs/op
PASS
*/
func BenchmarkTokenizeHashes(b *testing.B) {
	a := strings.Split(benchLogs, "\n")

	b.ReportAllocs()
	b.SetBytes(int64(len(benchLogs)))
	b.RunParallel(func(pb *testing.PB) {
		var hashes []uint64
		for pb.Next() {
			hashes = tokenizeHashes(hashes[:0], a)
		}
	})
}

/*
go test -test.fullpath=true -benchmem -run=^$ -bench ^BenchmarkTokenizeHashesOld$ github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage

goos: darwin
goarch: arm64
pkg: github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage
cpu: Apple M2
=== RUN   BenchmarkTokenizeHashesOld
BenchmarkTokenizeHashesOld
BenchmarkTokenizeHashesOld-8      665464              1864 ns/op        1796.11 MB/s           0 B/op          0 allocs/op
PASS

2267.84 MB/s vs 1796.11 MB/s,  about 26% speedup
*/
func BenchmarkTokenizeHashesOld(b *testing.B) {
	a := strings.Split(benchLogs, "\n")

	b.ReportAllocs()
	b.SetBytes(int64(len(benchLogs)))
	b.RunParallel(func(pb *testing.PB) {
		var hashes []uint64
		for pb.Next() {
			hashes = tokenizeHashesOld(hashes[:0], a)
		}
	})
}
