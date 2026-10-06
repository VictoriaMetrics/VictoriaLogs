package logsql

import (
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type histogramBucket struct {
	VMRange string
	Hits    uint64
}

// newHistogramBucketLabels returns a copy of labels with the vmrange label for b.
//
// The vmrange value is interned, so the returned labels do not reference the histogram value b was parsed from.
// The number of distinct vmrange values is small, so interning doesn't allocate in the steady state.
func newHistogramBucketLabels(labels []logstorage.Field, b histogramBucket) []logstorage.Field {
	bucketLabels := make([]logstorage.Field, 0, len(labels)+1)
	bucketLabels = append(bucketLabels, labels...)
	return append(bucketLabels, logstorage.Field{
		Name:  "vmrange",
		Value: bytesutil.InternString(b.VMRange),
	})
}

// appendHistogramBuckets parses the output of histogram() stats function and appends the parsed buckets to dst.
//
// The format is produced by statsHistogramProcessor.finalizeStats: `[]` or `[{"vmrange":"...","hits":123},...]`.
// It is parsed by hand, since encoding/json is too slow for this hot path.
//
// The returned VMRange values reference s.
// ok=false is returned if s doesn't match the format. dst is returned unchanged in this case.
func appendHistogramBuckets(dst []histogramBucket, s string) (_ []histogramBucket, ok bool) {
	dstLen := len(dst)

	if s == "[]" {
		return dst, true
	}
	s, ok = strings.CutPrefix(s, "[")
	if !ok {
		return dst, false
	}
	for {
		var b histogramBucket
		s, b, ok = parseHistogramBucket(s)
		if !ok {
			return dst[:dstLen], false
		}
		dst = append(dst, b)

		if s == "]" {
			return dst, true
		}
		s, ok = strings.CutPrefix(s, ",")
		if !ok {
			return dst[:dstLen], false
		}
	}
}

func parseHistogramBucket(s string) (tail string, b histogramBucket, ok bool) {
	s, ok = strings.CutPrefix(s, `{"vmrange":"`)
	if !ok {
		return s, b, false
	}
	// The vmrange cannot contain escaped chars.
	n := strings.IndexByte(s, '"')
	if n < 0 || strings.IndexByte(s[:n], '\\') >= 0 {
		return s, b, false
	}
	b.VMRange = s[:n]
	s, ok = strings.CutPrefix(s[n:], `","hits":`)
	if !ok {
		return s, b, false
	}

	n = 0
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		hits := b.Hits*10 + uint64(s[n]-'0')
		if hits/10 != b.Hits {
			// Overflow.
			return s, b, false
		}
		b.Hits = hits
		n++
	}
	if n == 0 {
		return s, b, false
	}
	s, ok = strings.CutPrefix(s[n:], "}")
	return s, b, ok
}
