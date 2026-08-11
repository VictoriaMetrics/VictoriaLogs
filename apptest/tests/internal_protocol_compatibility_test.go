package tests

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netselect"
	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestVlsingleMultipleSelectProtocolVersions(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	sut.JSONLineWrite(t, []string{
		`{"_msg":"protocol compatibility","_time":"2025-06-05T14:30:19.088007Z"}`,
	}, apptest.IngestOpts{})
	sut.ForceFlush(t)

	for _, protocolVersion := range []string{
		"v4", // Used by vlselect v1.50.0.
		netselect.QueryProtocolVersion,
	} {
		t.Run(protocolVersion, func(t *testing.T) {
			values := url.Values{
				"version":                {protocolVersion},
				"tenant_ids":             {`[{"account_id":0,"project_id":0}]`},
				"query":                  {`* | fields _msg`},
				"timestamp":              {"0"},
				"disable_compression":    {"true"},
				"allow_partial_response": {"false"},
				"hidden_fields_filters":  {"[]"},
			}
			u := "http://" + sut.HTTPAddr() + "/internal/select/query"
			response, statusCode := tc.Client().PostForm(t, u, values)
			if statusCode != http.StatusOK {
				t.Fatalf("unexpected status code; got %d; want %d; response %q", statusCode, http.StatusOK, response)
			}
			if message := unmarshalFirstMessage(t, []byte(response)); message != "protocol compatibility" {
				t.Fatalf("unexpected message; got %q; want %q", message, "protocol compatibility")
			}
		})
	}
}

func unmarshalFirstMessage(t *testing.T, data []byte) string {
	t.Helper()

	if len(data) < 9 {
		t.Fatalf("unexpectedly short query response: %d bytes", len(data))
	}
	blockLen := encoding.UnmarshalUint64(data)
	if blockLen > uint64(len(data)-8) {
		t.Fatalf("unexpected block size: %d bytes; response contains %d bytes", blockLen, len(data)-8)
	}
	blockData := data[8 : 8+blockLen]
	if blockData[0] != 0 {
		t.Fatalf("unexpected first block type: %d", blockData[0])
	}

	var db logstorage.DataBlock
	if _, _, err := db.UnmarshalInplace(blockData[1:], nil); err != nil {
		t.Fatalf("cannot unmarshal query result: %s", err)
	}
	c := db.GetColumnByName("_msg")
	if c == nil || len(c.Values) != 1 {
		t.Fatalf("unexpected _msg column: %#v", c)
	}
	return c.Values[0]
}
