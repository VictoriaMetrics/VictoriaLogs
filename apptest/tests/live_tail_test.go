package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestVlsingleLiveTailInitialResponse(t *testing.T) {
	fs.MustRemoveDir(t.Name())

	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ts := time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
	sut.JSONLineWrite(t, []string{
		fmt.Sprintf(`{"_time":%q,"ts":%q,"_msg":"test"}`, ts, ts),
	}, apptest.IngestOpts{})
	sut.ForceFlush(t)

	client := &http.Client{
		Transport: httputil.NewTransport(false, "apptest_client"),
		Timeout:   10 * time.Second,
	}
	defer client.CloseIdleConnections()

	f := func(query string, statusExpected int) {
		t.Helper()

		resp := startLiveTail(t, client, sut.HTTPAddr(), query)
		defer resp.Body.Close()
		if resp.StatusCode != statusExpected {
			t.Fatalf("unexpected status for %q; got %d; want %d", query, resp.StatusCode, statusExpected)
		}
		if statusExpected == http.StatusBadRequest {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("cannot read error response for %q: %s", query, err)
			}
			if !strings.Contains(string(body), "missing or invalid _time field") {
				t.Fatalf("unexpected error response for %q: %s", query, body)
			}
			return
		}

		var row map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&row); err != nil {
			t.Fatalf("cannot read live tail row for %q: %s", query, err)
		}
		if row["_time"] != ts || row["_msg"] != "test" {
			t.Fatalf("unexpected row for %q: %v", query, row)
		}
	}

	f("* | fields _msg", http.StatusBadRequest)
	f("* | fields _msg | filter _msg:*", http.StatusBadRequest)
	f("* | delete _time", http.StatusBadRequest)
	f("* | len(_time) as _time", http.StatusBadRequest)
	f("* | copy ts as _time", http.StatusOK)
	f("* | fields _time, _msg", http.StatusOK)
}

func startLiveTail(t *testing.T, client *http.Client, addr, query string) *http.Response {
	t.Helper()

	args := url.Values{
		"query":        {query},
		"offset":       {"0s"},
		"start_offset": {"1h"},
	}
	resp, err := client.PostForm("http://"+addr+"/select/logsql/tail", args)
	if err != nil {
		t.Fatalf("cannot start live tail for %q: %s", query, err)
	}
	return resp
}
