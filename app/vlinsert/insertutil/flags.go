package insertutil

import (
	"flag"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
)

var (
	// MaxLineSizeBytes is the maximum length of a single line for /insert/* handlers
	MaxLineSizeBytes = flagutil.NewBytes("insert.maxLineSizeBytes", 256*1024, "The maximum size of a single line that can be read by /insert/* handlers. Regardless of this flag, entries above the 2 MB limit are ignored, "+
		"see https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have")

	// MaxFieldsPerLine is the maximum number of fields per line for /insert/* handlers
	MaxFieldsPerLine = flag.Int("insert.maxFieldsPerLine", 1000, "The maximum number of log fields per line, which can be read by /insert/* handlers; "+
		"see https://docs.victoriametrics.com/victorialogs/faq/#how-many-fields-a-single-log-entry-may-contain")
)
