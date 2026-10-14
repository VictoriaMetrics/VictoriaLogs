package tests

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestVlsingleLiveTailing_Success(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlsingle()

	// ingest returns the live tailing line for the ingested log
	ingest := func(msg string) string {
		t.Helper()

		ts := time.Now().UTC().Format(time.RFC3339Nano)
		sut.JSONLineWrite(t, []string{fmt.Sprintf(`{"_msg":%q,"_time":%q}`, msg, ts)}, apptest.IngestOpts{})
		sut.ForceFlush(t)
		return fmt.Sprintf(`{"_time":%q,"_msg":%q}`, ts, msg)
	}

	lineOld := ingest("foo")

	// zero offset returns the newly ingested logs without the default 5s delay
	ltr := sut.LiveTail(t, "* | fields _time, _msg", apptest.LiveTailOpts{
		Offset: "0s",
	})
	defer ltr.Close()
	if ltr.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d; want %d", ltr.StatusCode, http.StatusOK)
	}

	f := func(lineExpected string) {
		t.Helper()

		line := ltr.NextLine(t)
		if line != lineExpected {
			t.Fatalf("unexpected line\ngot\n%s\nwant\n%s", line, lineExpected)
		}
	}

	// live tailing starts with the recently ingested logs
	f(lineOld)

	// then it returns the newly ingested logs without repeating the already returned logs
	lineNew := ingest("bar")
	f(lineNew)
}

func TestVlclusterLiveTailing_Success(t *testing.T) {
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartDefaultVlcluster()

	// ingest returns the live tailing line for the ingested log
	ingest := func(msg string) string {
		t.Helper()

		ts := time.Now().UTC().Format(time.RFC3339Nano)
		sut.JSONLineWrite(t, []string{fmt.Sprintf(`{"_msg":%q,"_time":%q}`, msg, ts)}, apptest.IngestOpts{})
		sut.ForceFlush(t)
		return fmt.Sprintf(`{"_time":%q,"_msg":%q}`, ts, msg)
	}

	lineOld := ingest("foo")

	// zero offset returns the newly ingested logs without the default 5s delay
	ltr := sut.LiveTail(t, "* | fields _time, _msg", apptest.LiveTailOpts{
		Offset: "0s",
	})
	defer ltr.Close()
	if ltr.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d; want %d", ltr.StatusCode, http.StatusOK)
	}

	f := func(lineExpected string) {
		t.Helper()

		line := ltr.NextLine(t)
		if line != lineExpected {
			t.Fatalf("unexpected line\ngot\n%s\nwant\n%s", line, lineExpected)
		}
	}

	// live tailing starts with the recently ingested logs
	f(lineOld)

	// then it returns the newly ingested logs without repeating the already returned logs
	lineNew := ingest("bar")
	f(lineNew)
}
