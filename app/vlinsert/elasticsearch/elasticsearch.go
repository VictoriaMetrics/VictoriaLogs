package elasticsearch

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bufferedwriter"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	elasticsearchVersion = flag.String("elasticsearch.version", "8.9.0", "Elasticsearch version to report to client")
)

// RequestHandler processes Elasticsearch insert requests
func RequestHandler(path string, w http.ResponseWriter, r *http.Request) bool {
	w.Header().Add("Content-Type", "application/json")
	// This header is needed for Logstash
	w.Header().Set("X-Elastic-Product", "Elasticsearch")

	if strings.HasPrefix(path, "/insert/elasticsearch/_ilm/policy") {
		// Return fake response for Elasticsearch ilm request.
		fmt.Fprintf(w, `{}`)
		return true
	}
	if strings.HasPrefix(path, "/insert/elasticsearch/_index_template") {
		// Return fake response for Elasticsearch index template request.
		fmt.Fprintf(w, `{}`)
		return true
	}
	if strings.HasPrefix(path, "/insert/elasticsearch/_ingest") {
		// Return fake response for Elasticsearch ingest pipeline request.
		// See: https://www.elastic.co/guide/en/elasticsearch/reference/8.8/put-pipeline-api.html
		fmt.Fprintf(w, `{}`)
		return true
	}
	if strings.HasPrefix(path, "/insert/elasticsearch/_nodes") {
		// Return fake response for Elasticsearch nodes discovery request.
		// See: https://www.elastic.co/guide/en/elasticsearch/reference/8.8/cluster.html
		fmt.Fprintf(w, `{}`)
		return true
	}
	if strings.HasPrefix(path, "/insert/elasticsearch/_rollup") {
		// Return fake response for Elasticsearch rollup apis
		// See: https://www.elastic.co/guide/en/elasticsearch/reference/8.8/rollup-apis.html
		fmt.Fprintf(w, `{}`)
		return true
	}
	if strings.HasPrefix(path, "/insert/elasticsearch/logstash") || strings.HasPrefix(path, "/insert/elasticsearch/_logstash") {
		// Return fake response for Logstash APIs requests.
		// See: https://www.elastic.co/guide/en/elasticsearch/reference/8.8/logstash-apis.html
		fmt.Fprintf(w, `{}`)
		return true
	}
	switch path {
	// some clients may omit trailing slash
	// see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8353
	case "/insert/elasticsearch/", "/insert/elasticsearch":
		switch r.Method {
		case http.MethodGet:
			// Return fake response for Elasticsearch ping request.
			// See the latest available version for Elasticsearch at https://github.com/elastic/elasticsearch/releases
			fmt.Fprintf(w, `{
			"version": {
				"number": %q
			}
		}`, *elasticsearchVersion)
		case http.MethodHead:
			// Return empty response for Logstash ping request.
		}

		return true
	case "/insert/elasticsearch/_license":
		// Return fake response for Elasticsearch license request.
		fmt.Fprintf(w, `{
			"license": {
				"uid": "cbff45e7-c553-41f7-ae4f-9205eabd80xx",
				"type": "oss",
				"status": "active",
				"expiry_date_in_millis" : 4000000000000
			}
		}`)
		return true
	case "/insert/elasticsearch/_bulk":
		startTime := time.Now()
		bulkRequestsTotal.Inc()

		cp, err := insertutil.GetCommonParams(r)
		if err != nil {
			httpserver.Errorf(w, r, "%s", err)
			return true
		}
		if err := insertutil.CanWriteData(); err != nil {
			httpserver.Errorf(w, r, "%s", err)
			return true
		}
		lmp := cp.NewLogMessageProcessor("elasticsearch_bulk", true)
		encoding := r.Header.Get("Content-Encoding")
		streamName := fmt.Sprintf("remoteAddr=%s, requestURI=%q", httpserver.GetQuotedRemoteAddr(r), r.RequestURI)
		results, err := readBulkRequest(streamName, r.Body, encoding, cp.TimeFields, cp.MsgFields, cp.PreserveJSONKeys, lmp)
		lmp.MustClose()
		if err != nil {
			httpserver.Errorf(w, r, "cannot decode log message #%d in /_bulk request: %s, stream fields: %s", len(results), err, cp.StreamFields)
			return true
		}

		tookMs := time.Since(startTime).Milliseconds()
		bw := bufferedwriter.Get(w)
		defer bufferedwriter.Put(bw)
		WriteBulkResponse(bw, results, tookMs)
		_ = bw.Flush()

		// update bulkRequestDuration only for successfully parsed requests
		// There is no need in updating bulkRequestDuration for request errors,
		// since their timings are usually much smaller than the timing for successful request parsing.
		bulkRequestDuration.UpdateDuration(startTime)

		return true
	default:
		return false
	}
}

var (
	bulkRequestsTotal   = metrics.NewCounter(`vl_http_requests_total{path="/insert/elasticsearch/_bulk"}`)
	bulkRequestDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/insert/elasticsearch/_bulk"}`)
)

type bulkItemResult []error

func hasBulkErrors(results bulkItemResult) bool {
	for _, err := range results {
		if err != nil {
			return true
		}
	}
	return false
}

func readBulkRequest(streamName string, r io.Reader, encoding string, timeFields, msgFields, preserveKeys []string, lmp insertutil.LogMessageProcessor) (bulkItemResult, error) {
	// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

	wcr, err := writeconcurrencylimiter.GetReader(r)
	if err != nil {
		return nil, err
	}
	defer writeconcurrencylimiter.PutReader(wcr)

	reader, err := protoparserutil.GetUncompressedReader(wcr, encoding)
	if err != nil {
		return nil, fmt.Errorf("cannot decode Elasticsearch protocol data: %w", err)
	}
	defer protoparserutil.PutUncompressedReader(reader)

	lr := insertutil.NewLineReader(streamName, reader)

	var results bulkItemResult
	for {
		ok, err := readBulkLine(lr, timeFields, msgFields, preserveKeys, lmp)
		if !ok {
			return results, err
		}
		results = append(results, err)
	}
}

func readBulkLine(lr *insertutil.LineReader, timeFields, msgFields, preserveKeys []string, lmp insertutil.LogMessageProcessor) (bool, error) {
	var line []byte

	// Read the command, must be "create" or "index"
	for len(line) == 0 {
		if !lr.NextLine() {
			err := lr.Err()
			return false, err
		}
		line = lr.Line
	}
	lineStr := bytesutil.ToUnsafeString(line)
	if !strings.Contains(lineStr, `"create"`) && !strings.Contains(lineStr, `"index"`) {
		return false, fmt.Errorf(`unexpected command %q; expecting "create" or "index"`, line)
	}

	// Decode log message
	ok := lr.NextLine()
	if lr.IsTooLongLine {
		return true, errTooLongLine
	}
	if !ok {
		if err := lr.Err(); err != nil {
			return false, err
		}
		return false, fmt.Errorf(`missing log message after the "create" or "index" command`)
	}
	line = lr.Line
	if len(line) == 0 {
		return false, fmt.Errorf(`missing log message after the "create" or "index" command`)
	}

	p := logstorage.GetJSONParser()
	defer logstorage.PutJSONParser(p)

	if err := p.ParseLogMessage(line, preserveKeys, ""); err != nil {
		const tailBytes = 128
		lineTail := line
		if len(lineTail) > tailBytes {
			lineTail = lineTail[len(lineTail)-tailBytes:]
		}
		return false, fmt.Errorf("cannot parse json-encoded log entry: %w; last %d bytes: %q", err, len(lineTail), lineTail)
	}

	ts, err := extractTimestampFromFields(timeFields, p.Fields)
	if err != nil {
		return false, fmt.Errorf("cannot parse timestamp: %w", err)
	}
	if ts == 0 {
		ts = time.Now().UnixNano()
	}
	logstorage.RenameField(p.Fields, msgFields, "_msg")
	lmp.AddRow(ts, p.Fields, -1)

	return true, nil
}

var errTooLongLine = errors.New("log line exceeds -insert.maxLineSizeBytes")

func extractTimestampFromFields(timeFields []string, fields []logstorage.Field) (int64, error) {
	for _, timeField := range timeFields {
		for i := range fields {
			f := &fields[i]
			if f.Name != timeField {
				continue
			}
			timestamp, err := parseElasticsearchTimestamp(f.Value)
			if err != nil {
				return 0, err
			}
			f.Value = ""
			return timestamp, nil
		}
	}
	return 0, nil
}

func parseElasticsearchTimestamp(s string) (int64, error) {
	if s == "0" || s == "" {
		// Special case - zero or empty timestamp must be substituted
		// with the current time by the caller.
		return 0, nil
	}
	if len(s) < len("YYYY-MM-DD") || s[len("YYYY")] != '-' {
		// Try parsing timestamp in seconds or milliseconds
		nsecs, ok := timeutil.TryParseUnixTimestamp(s)
		if !ok {
			return 0, fmt.Errorf("cannot parse unix timestamp %q", s)
		}
		return nsecs, nil
	}
	if len(s) == len("YYYY-MM-DD") {
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return 0, fmt.Errorf("cannot parse date %q: %w", s, err)
		}
		return t.UnixNano(), nil
	}
	nsecs, ok := logstorage.TryParseTimestampRFC3339Nano(s)
	if !ok {
		return 0, fmt.Errorf("cannot parse timestamp %q", s)
	}
	return nsecs, nil
}
