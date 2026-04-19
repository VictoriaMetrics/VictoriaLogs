package tests

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestHits(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	// Test https://github.com/VictoriaMetrics/VictoriaLogs/issues/1278
	records := []string{
		`{"_time":"2026-03-27T11:54:59Z","hits":"foo"}`,
		`{"_time":"2026-03-27T11:55:59Z","hits":"bar"}`,
		`{"_time":"2026-03-27T11:56:59Z","hits":"foo"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	query := `*`
	responseExpected := `{"hits":[{"fields":{"hits":"bar"},"timestamps":["2026-03-27T11:50:00Z","2026-03-27T11:55:00Z"],"values":[0,1],"total":1},{"fields":{"hits":"foo"},"timestamps":["2026-03-27T11:50:00Z","2026-03-27T11:55:00Z"],"values":[1,1],"total":2}]}`

	queryOpts := apptest.HitsOpts{
		Start: "2026-03-27T11:50:00Z",
		End:   "2026-03-27T12:00:00Z",
		Step:  "5m",
		Field: "hits",
	}
	response := sut.Hits(t, query, queryOpts)
	if response != responseExpected {
		t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
	}
}

func TestVlclusterHits(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlcluster()

	// Test https://github.com/VictoriaMetrics/VictoriaLogs/issues/1278
	records := []string{
		`{"_time":"2026-03-27T11:54:59Z","hits":"foo"}`,
		`{"_time":"2026-03-27T11:55:59Z","hits":"bar"}`,
		`{"_time":"2026-03-27T11:56:59Z","hits":"foo"}`,
	}
	sut.JSONLineWrite(t, records, apptest.IngestOpts{})
	sut.ForceFlush(t)

	query := `*`
	responseExpected := `{"hits":[{"fields":{"hits":"bar"},"timestamps":["2026-03-27T11:50:00Z","2026-03-27T11:55:00Z"],"values":[0,1],"total":1},{"fields":{"hits":"foo"},"timestamps":["2026-03-27T11:50:00Z","2026-03-27T11:55:00Z"],"values":[1,1],"total":2}]}`

	queryOpts := apptest.HitsOpts{
		Start: "2026-03-27T11:50:00Z",
		End:   "2026-03-27T12:00:00Z",
		Step:  "5m",
		Field: "hits",
	}
	response := sut.Hits(t, query, queryOpts)
	if response != responseExpected {
		t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
	}
}
