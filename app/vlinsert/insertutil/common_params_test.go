package insertutil

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestGetCommonParams_RemoveEmptyTokens(t *testing.T) {
	f := func(headers map[string]string, streamFieldsExpected, timeFieldsExpected []string, isTimeFieldSetExpected bool, extraFieldsExpected int) {
		t.Helper()

		r := httptest.NewRequest("POST", "http://example.com/insert", nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}

		cp, err := GetCommonParams(r)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if !slicesEqual(cp.StreamFields, streamFieldsExpected) {
			t.Fatalf("unexpected StreamFields; got %q; want %q", cp.StreamFields, streamFieldsExpected)
		}
		if !slicesEqual(cp.TimeFields, timeFieldsExpected) {
			t.Fatalf("unexpected TimeFields; got %q; want %q", cp.TimeFields, timeFieldsExpected)
		}
		if cp.IsTimeFieldSet != isTimeFieldSetExpected {
			t.Fatalf("unexpected IsTimeFieldSet; got %v; want %v", cp.IsTimeFieldSet, isTimeFieldSetExpected)
		}
		if len(cp.ExtraFields) != extraFieldsExpected {
			t.Fatalf("unexpected ExtraFields len; got %d; want %d", len(cp.ExtraFields), extraFieldsExpected)
		}
	}

	f(map[string]string{
		"VL-Stream-Fields": "collector,,service.name",
	}, []string{"collector", "service.name"}, []string{"_time"}, false, 0)

	f(map[string]string{
		"VL-Time-Field": ",  observedTimestamp, ",
	}, nil, []string{"observedTimestamp"}, true, 0)

	f(map[string]string{
		"VL-Time-Field": ",,",
	}, nil, []string{"_time"}, false, 0)

	f(map[string]string{
		"VL-Extra-Fields": "a=b,, c=d",
	}, nil, []string{"_time"}, false, 2)
}

func TestLogMessageProcessorDiscoverLogLevels(t *testing.T) {
	discoverLogLevelsOld := *discoverLogLevels
	logLevelFieldsOld := append([]string(nil), (*logLevelFields)...)
	defer func() {
		*discoverLogLevels = discoverLogLevelsOld
		*logLevelFields = append((*logLevelFields)[:0], logLevelFieldsOld...)
	}()

	tests := []struct {
		name           string
		discover       bool
		logLevelFields []string
		fields         []logstorage.Field
		resultExpected string
	}{
		{
			name:     "disabled by default",
			discover: false,
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "severity", Value: "ERROR"},
			},
			resultExpected: `{"_msg":"foo","severity":"ERROR"}`,
		},
		{
			name:     "discovers level from severity",
			discover: true,
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "severity", Value: "ERROR"},
			},
			resultExpected: `{"_msg":"foo","severity":"ERROR","level":"error"}`,
		},
		{
			name:     "preserves existing level",
			discover: true,
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "level", Value: "info"},
				{Name: "severity", Value: "ERROR"},
			},
			resultExpected: `{"_msg":"foo","level":"info","severity":"ERROR"}`,
		},
		{
			name:           "uses candidate field priority",
			discover:       true,
			logLevelFields: []string{"lvl", "severity"},
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "lvl", Value: "debug"},
				{Name: "severity", Value: "warn"},
			},
			resultExpected: `{"_msg":"foo","lvl":"debug","severity":"warn","level":"debug"}`,
		},
		{
			name:     "normalizes warning",
			discover: true,
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "severity", Value: "WARNING"},
			},
			resultExpected: `{"_msg":"foo","severity":"WARNING","level":"warn"}`,
		},
		{
			name:     "normalizes warn",
			discover: true,
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "severity", Value: "Warn"},
			},
			resultExpected: `{"_msg":"foo","severity":"Warn","level":"warn"}`,
		},
		{
			name:     "normalizes fatal",
			discover: true,
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "severity", Value: "FATAL"},
			},
			resultExpected: `{"_msg":"foo","severity":"FATAL","level":"fatal"}`,
		},
		{
			name:     "skips unrecognized value",
			discover: true,
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "severity", Value: "banana"},
			},
			resultExpected: `{"_msg":"foo","severity":"banana"}`,
		},
		{
			name:     "skips empty candidate value",
			discover: true,
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "severity", Value: ""},
			},
			resultExpected: `{"_msg":"foo"}`,
		},
		{
			name:     "skips row without candidate fields",
			discover: true,
			fields: []logstorage.Field{
				{Name: "_msg", Value: "foo"},
				{Name: "service", Value: "app"},
			},
			resultExpected: `{"_msg":"foo","service":"app"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			*discoverLogLevels = tt.discover
			*logLevelFields = append((*logLevelFields)[:0], tt.logLevelFields...)

			storage := &testLogRowsStorage{}
			SetLogRowsStorage(storage)

			cp := &CommonParams{}
			lmp := cp.NewLogMessageProcessor("test", false)
			lmp.AddRow(123, tt.fields, -1)
			lmp.MustClose()

			result := strings.Join(storage.rows, "\n")
			if result != tt.resultExpected {
				t.Fatalf("unexpected result;\ngot\n%s\nwant\n%s", result, tt.resultExpected)
			}
		})
	}
}

type testLogRowsStorage struct {
	rows []string
}

func (s *testLogRowsStorage) MustAddRows(lr *logstorage.LogRows) {
	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		row := logstorage.MarshalFieldsToJSON(nil, r.Fields)
		s.rows = append(s.rows, string(row))
	})
}

func (s *testLogRowsStorage) CanWriteData() error {
	return nil
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
