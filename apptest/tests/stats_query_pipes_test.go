package tests

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// Verifies that:
// - range stats reject pipes before stats that modify/drop _time
// - instant stats allow such pipes
func TestVlsingleStatsQueryPipesTimeFieldConstraints(t *testing.T) {
	// Use a clean dir per test
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	// Ingest two simple records
	records := []string{
		`{"_msg":"a","_time":"2025-01-01T00:00:00Z","x":"1"}`,
		`{"_msg":"b","_time":"2025-01-01T00:00:01Z","x":"2"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	type opts struct {
		queryOpts   apptest.StatsQueryRangeOpts
		instantOpts apptest.StatsQueryOpts
		wantOK      bool
	}

	f := func(query string, o *opts, isRange, isOk bool) {
		t.Helper()
		var status int
		if isRange {
			_, status = sut.StatsQueryRangeRaw(t, query, o.queryOpts)
		} else {
			_, status = sut.StatsQueryRaw(t, query, o.instantOpts)
		}
		ok := status == http.StatusOK
		if ok != isOk {
			t.Fatalf("expected OK=%v; got status=%d", isOk, status)
		}
	}

	// Range stats MUST reject pipes that modify/remove _time (step > 0 requires _time)
	opt := &opts{
		queryOpts: apptest.StatsQueryRangeOpts{
			Start: "2025-01-01T00:00:00Z",
			End:   "2025-01-01T00:05:00Z",
			Step:  "1m",
		},
		wantOK: false,
	}

	// safe queries
	f(`* | stats count()`, opt, true, true)
	f(`* | fields _time, x | stats count()`, opt, true, true)
	f(`* | delete x | stats count()`, opt, true, true)

	// modifying/removing _time is NOT OK
	f(`* | fields x | stats count()`, opt, true, false)
	f(`* | delete _time | stats count()`, opt, true, false)

	// step must be > 0
	opt.queryOpts.Step = "0s"
	f(`* | stats count()`, opt, true, false)

	// Instant stats MUST allow time-modifying pipes
	opt.instantOpts = apptest.StatsQueryOpts{
		Time: "2025-01-01T00:05:00Z",
	}
	f(`* | delete _time | stats count()`, opt, false, true)
	f(`* | fields x | stats count()`, opt, false, true)
	f(`* | stats count()`, opt, false, true)
}
