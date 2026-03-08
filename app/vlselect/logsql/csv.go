package logsql

import (
	"strings"
)

func appendCSVLine(dst []byte, fields []string) []byte {
	for i := range fields {
		dst = appendCSVField(dst, fields[i])
		if i != len(fields)-1 {
			dst = append(dst, ',')
		}
	}
	dst = append(dst, '\n')
	return dst
}

func appendCSVField(dst []byte, s string) []byte {
	n := strings.IndexAny(s, `",`+"\n")
	if n < 0 {
		// fast path - nothing to quote
		return append(dst, s...)
	}

	// slow path - the s must be quoted
	dst = append(dst, '"')

	for {
		dst = append(dst, s[:n]...)

		ch := s[n]
		if ch == '"' {
			dst = append(dst, `""`...)
		} else {
			dst = append(dst, ch)
		}

		s = s[n+1:]
		if len(s) == 0 {
			break
		}

		n := strings.IndexByte(s, '"')
		if n < 0 {
			dst = append(dst, s...)
			break
		}
	}

	dst = append(dst, '"')
	return dst
}
