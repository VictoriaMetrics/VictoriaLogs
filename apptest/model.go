package apptest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"testing"
)

// QueryOpts contains params used for querying VictoriaLogs via /select/logsq/query
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-logs
type QueryOpts struct {
	Timeout      string
	Start        string
	End          string
	Limit        string
	ExtraFilters []string
}

func (qos *QueryOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "timeout", qos.Timeout)
	addNonEmpty(uv, "start", qos.Start)
	addNonEmpty(uv, "end", qos.End)
	addNonEmpty(uv, "limit", qos.Limit)
	addNonEmpty(uv, "extra_filters", qos.ExtraFilters...)
	return uv
}

// FacetsOpts contains params used for querying VictoriaLogs via /select/logsql/facets
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-facets
type FacetsOpts struct {
	Start             string
	End               string
	Limit             string
	MaxValuesPerField string
	MaxValueLen       string
	KeepConstFields   string
	ExtraFilters      []string
}

func (fos *FacetsOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "start", fos.Start)
	addNonEmpty(uv, "end", fos.End)
	addNonEmpty(uv, "limit", fos.Limit)
	addNonEmpty(uv, "max_values_per_field", fos.MaxValuesPerField)
	addNonEmpty(uv, "max_value_len", fos.MaxValueLen)
	addNonEmpty(uv, "keep_const_fields", fos.KeepConstFields)
	addNonEmpty(uv, "extra_filters", fos.ExtraFilters...)
	return uv
}

// StatsQueryOpts contains params used for querying VictoriaLogs via /select/logsq/stats_query
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-log-stats
type StatsQueryOpts struct {
	Timeout      string
	Time         string
	ExtraFilters []string
}

func (qos *StatsQueryOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "timeout", qos.Timeout)
	addNonEmpty(uv, "time", qos.Time)
	addNonEmpty(uv, "extra_filters", qos.ExtraFilters...)
	return uv
}

// StatsQueryRangeOpts contains params used for querying VictoriaLogs via /select/logsq/stats_query_range
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-log-range-stats
type StatsQueryRangeOpts struct {
	Timeout      string
	Start        string
	End          string
	Step         string
	ExtraFilters []string
}

func (qos *StatsQueryRangeOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "timeout", qos.Timeout)
	addNonEmpty(uv, "start", qos.Start)
	addNonEmpty(uv, "end", qos.End)
	addNonEmpty(uv, "step", qos.Step)
	addNonEmpty(uv, "extra_filters", qos.ExtraFilters...)
	return uv
}

// IngestOpts contains various params used for VictoriaLogs ingesting data
type IngestOpts struct {
	MessageField string
	StreamFields string
	TimeField    string
}

func (qos *IngestOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "_time_field", qos.TimeField)
	addNonEmpty(uv, "_stream_fields", qos.StreamFields)
	addNonEmpty(uv, "_msg_field", qos.MessageField)
	return uv
}

// LogsQLQueryResponse is an in-memory representation of the
// /select/logsql/query response.
type LogsQLQueryResponse struct {
	LogLines []string
}

// NewLogsQLQueryResponse is a test helper function that creates a new
// instance of LogsQLQueryResponse by unmarshalling a json string.
func NewLogsQLQueryResponse(t *testing.T, s string) *LogsQLQueryResponse {
	t.Helper()

	res := &LogsQLQueryResponse{}
	if len(s) == 0 {
		return res
	}
	bs := bytes.NewBufferString(s)
	for {
		logLine, err := bs.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(logLine) > 0 {
					t.Fatalf("BUG: unexpected non-empty line=%q with io.EOF", logLine)
				}
				break
			}
			t.Fatalf("BUG: cannot read logline from buffer: %s", err)
		}
		var lv map[string]any
		if err := json.Unmarshal([]byte(logLine), &lv); err != nil {
			t.Fatalf("cannot parse log line=%q: %s", logLine, err)
		}
		delete(lv, "_stream_id")
		normalizedLine, err := json.Marshal(lv)
		if err != nil {
			t.Fatalf("cannot marshal parsed logline=%q: %s", logLine, err)
		}
		res.LogLines = append(res.LogLines, string(normalizedLine))
	}

	return res
}

func addNonEmpty(uv url.Values, name string, values ...string) {
	for _, value := range values {
		if value != "" {
			uv.Add(name, value)
		}
	}
}
