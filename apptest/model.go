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
	AccountID string
	ProjectID string

	Timeout      string
	Start        string
	End          string
	Limit        string
	ExtraFilters []string
	Format       string
}

func (qos *QueryOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "timeout", qos.Timeout)
	addNonEmpty(uv, "start", qos.Start)
	addNonEmpty(uv, "end", qos.End)
	addNonEmpty(uv, "limit", qos.Limit)
	addNonEmpty(uv, "extra_filters", qos.ExtraFilters...)
	addNonEmpty(uv, "format", qos.Format)
	return uv
}

// FieldNamesOpts contains params used for query VictoriaLogs via /select/logsql/field_names
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-field-names
type FieldNamesOpts struct {
	Start       string
	End         string
	Filter      string
	IgnorePipes string
}

func (fos *FieldNamesOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "start", fos.Start)
	addNonEmpty(uv, "end", fos.End)
	addNonEmpty(uv, "filter", fos.Filter)
	addNonEmpty(uv, "ignore_pipes", fos.IgnorePipes)
	return uv
}

// FieldValuesOpts contains params used for query VictoriaLogs via /select/logsql/field_values
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-field-values
type FieldValuesOpts struct {
	Start       string
	End         string
	Field       string
	Filter      string
	Limit       string
	IgnorePipes string
}

func (fos *FieldValuesOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "start", fos.Start)
	addNonEmpty(uv, "end", fos.End)
	addNonEmpty(uv, "field", fos.Field)
	addNonEmpty(uv, "filter", fos.Filter)
	addNonEmpty(uv, "limit", fos.Limit)
	addNonEmpty(uv, "ignore_pipes", fos.IgnorePipes)
	return uv
}

// StreamFieldNamesOpts contains params used for query VictoriaLogs via /select/logsql/stream_field_names
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-stream-field-names
type StreamFieldNamesOpts struct {
	Start       string
	End         string
	Filter      string
	IgnorePipes string
}

func (fos *StreamFieldNamesOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "start", fos.Start)
	addNonEmpty(uv, "end", fos.End)
	addNonEmpty(uv, "filter", fos.Filter)
	addNonEmpty(uv, "ignore_pipes", fos.IgnorePipes)
	return uv
}

// StreamFieldValuesOpts contains params used for query VictoriaLogs via /select/logsql/field_values
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-stream-field-values
type StreamFieldValuesOpts struct {
	Start       string
	End         string
	Field       string
	Filter      string
	Limit       string
	IgnorePipes string
}

func (fos *StreamFieldValuesOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "start", fos.Start)
	addNonEmpty(uv, "end", fos.End)
	addNonEmpty(uv, "field", fos.Field)
	addNonEmpty(uv, "filter", fos.Filter)
	addNonEmpty(uv, "limit", fos.Limit)
	addNonEmpty(uv, "ignore_pipes", fos.IgnorePipes)
	return uv
}

// StreamsOpts contains params used for query VictoriaLogs via /select/logsql/streams
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-streams
type StreamsOpts struct {
	Start       string
	End         string
	Limit       string
	IgnorePipes string
}

func (fos *StreamsOpts) asURLValues() url.Values {
	uv := make(url.Values)
	addNonEmpty(uv, "start", fos.Start)
	addNonEmpty(uv, "end", fos.End)
	addNonEmpty(uv, "limit", fos.Limit)
	addNonEmpty(uv, "ignore_pipes", fos.IgnorePipes)
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
	IgnorePipes       string
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
	addNonEmpty(uv, "ignore_pipes", fos.IgnorePipes)
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
	AccountID string
	ProjectID string

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
