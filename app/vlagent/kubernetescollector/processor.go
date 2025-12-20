package kubernetescollector

import (
	"bytes"
	"cmp"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/valyala/fastjson"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	tenantID = flag.String("kubernetesCollector.tenantID", "0:0",
		"Default tenant ID to use for logs collected from Kubernetes pods in format: <accountID>:<projectID>. See https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy")
	ignoreFields     = flagutil.NewArrayString("kubernetesCollector.ignoreFields", "Fields to ignore across logs ingested from Kubernetes")
	decolorizeFields = flagutil.NewArrayString("kubernetesCollector.decolorizeFields", "Fields to remove ANSI color codes across logs ingested from Kubernetes")
	msgField         = flagutil.NewArrayString("kubernetesCollector.msgField", "Fields that may contain the _msg field. "+
		"Default: message,msg,log. See https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field")
	timeField = flagutil.NewArrayString("kubernetesCollector.timeField", "Fields that may contain the _time field. "+
		"Default: time,timestamp,ts. If none of the specified fields is found in the log line, then the write time will be used. "+
		"See https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field")
	extraFields = flag.String("kubernetesCollector.extraFields", "", "Extra fields to add to each log line collected from Kubernetes pods in JSON format. "+
		`For example: -kubernetesCollector.extraFields='{"cluster":"cluster-1","env":"production"}'`)

	includePodLabels = flag.Bool("kubernetesCollector.includePodLabels", true, "Include Pod labels as additional fields in the log entries. "+
		"Even this setting is disabled, Pod labels are available for filtering via -kubernetes.excludeFilter flag")
	includePodAnnotations = flag.Bool("kubernetesCollector.includePodAnnotations", false, "Include Pod annotations as additional fields in the log entries. "+
		"Even this setting is disabled, Pod annotations are available for filtering via -kubernetes.excludeFilter flag")
	includeNodeLabels = flag.Bool("kubernetesCollector.includeNodeLabels", false, "Include Node labels as additional fields in the log entries. "+
		"Even this setting is disabled, Node labels are available for filtering via -kubernetes.excludeFilter flag")
	includeNodeAnnotations = flag.Bool("kubernetesCollector.includeNodeAnnotations", false, "Include Node annotations as additional fields in the log entries. "+
		"Even this setting is disabled, Node annotations are available for filtering via -kubernetes.excludeFilter flag")
)

type logFileProcessor struct {
	storage  insertutil.LogRowsStorage
	lr       *logstorage.LogRows
	tenantID logstorage.TenantID

	// streamFieldsLen is the number of stream fields at the beginning of commonFields.
	streamFieldsLen int

	// commonFields are common fields for the given log file.
	commonFields []logstorage.Field

	// fieldsBuf is used for constructing log fields from commonFields and the actual log line fields before sending them to VictoriaLogs.
	fieldsBuf []logstorage.Field

	// partialCRIContent accumulates the content of partial CRI log lines.
	// Can be truncated if it exceeds maxLineSize.
	partialCRIContent *bytesutil.ByteBuffer
	// partialCRIContentSize tracks the actual size of the partialCRIContent.
	partialCRIContentSize int
}

// newLogFileProcessor returns a new logFileProcessor for the given storage.
// commonFields must not be modified as they can be accessed from multiple goroutines.
func newLogFileProcessor(storage insertutil.LogRowsStorage, commonFields []logstorage.Field) *logFileProcessor {
	// Exclude labels or annotations if they should not be included.
	if !*includePodLabels || !*includePodAnnotations || !*includeNodeLabels || !*includeNodeAnnotations {
		var fields []logstorage.Field
		for _, f := range commonFields {
			excludeField := !*includePodLabels && strings.HasPrefix(f.Name, "kubernetes.pod_labels.") ||
				!*includePodAnnotations && strings.HasPrefix(f.Name, "kubernetes.pod_annotations.") ||
				!*includeNodeLabels && strings.HasPrefix(f.Name, "kubernetes.node_labels.") ||
				!*includeNodeAnnotations && strings.HasPrefix(f.Name, "kubernetes.node_annotations.")

			if !excludeField {
				fields = append(fields, f)
			}
		}
		commonFields = fields
	}

	// move stream fields to the beginning of commonFields

	streamFields := make([]logstorage.Field, 0, len(commonFields))
	for _, f := range commonFields {
		if slices.Contains(streamFieldNames, f.Name) {
			streamFields = append(streamFields, f)
		}
	}
	streamFieldsLen := len(streamFields)

	fields := streamFields
	for _, f := range commonFields {
		if !slices.Contains(streamFieldNames, f.Name) {
			fields = append(fields, f)
		}
	}

	tenantID := getTenantID()
	extraFields := getExtraFields()

	const defaultMsgValue = "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"
	lr := logstorage.GetLogRows(nil, *ignoreFields, *decolorizeFields, extraFields, defaultMsgValue)

	return &logFileProcessor{
		storage:  storage,
		lr:       lr,
		tenantID: tenantID,

		streamFieldsLen: streamFieldsLen,
		commonFields:    fields,
	}
}

func (lfp *logFileProcessor) tryAddLine(logLine []byte) bool {
	if len(logLine) == 0 {
		return true
	}

	if logLine[0] == '{' {
		// Most likely, vlagent is running in Docker,
		// so fallback to the 'json-file' logging driver.
		parser := criJSONParserPool.Get()
		defer criJSONParserPool.Put(parser)

		criLine, err := parseCRILineJSON(parser, logLine)
		if err != nil {
			logger.Panicf("FATAL: cannot parse 'json-file' log content: %s; content: %q", err, logLine)
		}

		lfp.addLineInternal(criLine.timestamp, criLine.content)

		return true
	}

	criLine, err := parseCRILine(logLine)
	if err != nil {
		logger.Panicf("FATAL: cannot parse Container Runtime Interface log line: %s; content: %q", err, logLine)
	}

	timestamp, content, ok := lfp.joinPartialLines(criLine)
	if !ok {
		// The log content is not yet complete.
		return false
	}
	if len(content) == 0 {
		// The log content is truncated or empty.
		// Skip such lines.
		return true
	}

	lfp.addLineInternal(timestamp, content)

	if lfp.partialCRIContent != nil {
		partialCRIContentBufPool.Put(lfp.partialCRIContent)
		lfp.partialCRIContent = nil
	}

	return true
}

func (lfp *logFileProcessor) joinPartialLines(criLine criLine) (int64, []byte, bool) {
	if criLine.partial {
		// The log line is split into multiple lines.
		// Accumulate the content until the full line is received.

		if lfp.partialCRIContent == nil {
			lfp.partialCRIContent = partialCRIContentBufPool.Get()
		}

		lfp.partialCRIContentSize += len(criLine.content)
		if lfp.partialCRIContentSize <= maxLogLineSize {
			lfp.partialCRIContent.MustWrite(criLine.content)
		}
		return 0, nil, false
	}

	if lfp.partialCRIContent == nil || lfp.partialCRIContent.Len() == 0 {
		// The log line is complete and not split.
		return criLine.timestamp, criLine.content, true
	}

	// The final part of the split log line received.

	lfp.partialCRIContentSize += len(criLine.content)
	if lfp.partialCRIContentSize > maxLogLineSize {
		// Discard the too large log line.
		reportLogRowSizeExceeded(lfp.commonFields[:lfp.streamFieldsLen], lfp.partialCRIContentSize)

		lfp.partialCRIContent.Reset()
		lfp.partialCRIContentSize = 0

		return 0, nil, true
	}

	lfp.partialCRIContent.MustWrite(criLine.content)
	content := lfp.partialCRIContent.B

	lfp.partialCRIContent.Reset()
	lfp.partialCRIContentSize = 0

	return criLine.timestamp, content, true
}

func (lfp *logFileProcessor) addLineInternal(criTimestamp int64, line []byte) {
	parser := logstorage.GetJSONParser()
	defer logstorage.PutJSONParser(parser)

	timestamp, ok := parseLogRowContent(parser, line)
	if !ok {
		parser.Fields = append(parser.Fields, logstorage.Field{
			Name:  "_msg",
			Value: bytesutil.ToUnsafeString(line),
		})
	}

	if timestamp <= 0 {
		// Timestamp from the log line is missing or invalid, use the timestamp from Container Runtime Interface.
		timestamp = criTimestamp
	}

	if len(parser.Fields) > 1000 {
		line := logstorage.MarshalFieldsToJSON(nil, parser.Fields)
		logger.Warnf("dropping log line with %d fields; %s", len(parser.Fields), line)
		return
	}

	lfp.addRow(timestamp, parser.Fields)
}

func (lfp *logFileProcessor) addRow(timestamp int64, fields []logstorage.Field) {
	clear(lfp.fieldsBuf)
	lfp.fieldsBuf = append(lfp.fieldsBuf[:0], lfp.commonFields...)
	lfp.fieldsBuf = append(lfp.fieldsBuf, fields...)

	lfp.lr.MustAdd(lfp.tenantID, timestamp, lfp.fieldsBuf, lfp.streamFieldsLen)
	lfp.storage.MustAddRows(lfp.lr)
	lfp.lr.ResetKeepSettings()
}

func parseLogRowContent(p *logstorage.JSONParser, data []byte) (int64, bool) {
	if len(data) == 0 {
		return 0, false
	}

	switch data[0] {
	case '{':
		err := p.ParseLogMessage(data)
		if err != nil {
			return 0, false
		}

		// Try to parse timestamp from the time fields.
		var timestamp int64
		n := fieldIndex(p.Fields, getTimeFields())
		if n >= 0 {
			f := &p.Fields[n]
			v, ok := logstorage.TryParseTimestampRFC3339Nano(f.Value)
			if ok {
				timestamp = v
				// Set the time field to empty string to ignore it during data ingestion.
				f.Value = ""
			}
		}

		// Rename the message field to _msg.
		logstorage.RenameField(p.Fields, getMsgFields(), "_msg")

		return timestamp, true
	case 'I', 'W', 'E', 'F':
		timestamp, fields, ok := tryParseKlog(p.Fields, bytesutil.ToUnsafeString(data))
		if !ok {
			return 0, false
		}
		p.Fields = fields
		return timestamp, true
	}

	return 0, false
}

// tryParseKlog parses the given string in Kubernetes Log format and returns the parsed fields.
// See https://github.com/kubernetes/klog/
func tryParseKlog(dst []logstorage.Field, src string) (int64, []logstorage.Field, bool) {
	if len(src) < len("I0101 00:00:00.000000 1 p:1] m") {
		return 0, nil, false
	}

	// Parse level.
	level := getKlogLevel(src[0])
	src = src[1:]
	dst = append(dst, logstorage.Field{Name: "level", Value: level})

	// Parse timestamp.
	timestampStr := src[:len("0102 15:04:05.000000")]
	t, err := time.ParseInLocation("0102 15:04:05.000000", timestampStr, time.UTC)
	if err != nil {
		return 0, nil, false
	}
	src = src[len("0102 15:04:05.000000"):]
	t = t.AddDate(time.Now().Year(), 0, 0)
	timestamp := t.UnixNano()

	// Remove trailing spaces.
	if len(src) == 0 || src[0] != ' ' {
		return 0, nil, false
	}
	src = strings.TrimLeft(src, " ")

	// Parse thread ID.
	n := strings.IndexByte(src, ' ')
	if n <= 0 {
		return 0, nil, false
	}
	threadID := src[:n]
	src = src[n+1:]
	dst = append(dst, logstorage.Field{Name: "thread_id", Value: threadID})

	// Parse file:line.
	n = strings.IndexByte(src, ']')
	if n <= 0 {
		return 0, nil, false
	}
	sourceLine := src[:n]
	src = src[n+1:]
	if len(src) == 0 || src[0] != ' ' {
		return 0, nil, false
	}
	src = src[1:]
	dst = append(dst, logstorage.Field{Name: "source_line", Value: sourceLine})

	// Parse log content.
	var ok bool
	dst, ok = tryParseKlogContent(dst, src)
	if !ok {
		return 0, nil, false
	}

	return timestamp, dst, true
}

func tryParseKlogContent(dst []logstorage.Field, src string) ([]logstorage.Field, bool) {
	if len(src) == 0 {
		return dst, false
	}
	if src[0] != '"' {
		// Fast path: message is not quoted and does not contain additional key="value" fields.
		return append(dst, logstorage.Field{Name: "_msg", Value: src}), true
	}

	// Slow path: message is quoted and contains additional key="value" fields.
	prefix, err := strconv.QuotedPrefix(src)
	if err != nil {
		return nil, false
	}
	msg, err := strconv.Unquote(prefix)
	if err != nil {
		return nil, false
	}
	src = src[len(prefix):]
	dst = append(dst, logstorage.Field{Name: "_msg", Value: msg})

	// Parse key="value" pairs.
	for len(src) > 0 {
		if src[0] == ' ' {
			src = src[1:]
		}

		n := strings.IndexByte(src, '=')
		if n <= 0 {
			return nil, false
		}
		key := src[:n]
		src = src[n+1:]

		prefix, err := strconv.QuotedPrefix(src)
		if err != nil {
			return nil, false
		}
		value, err := strconv.Unquote(prefix)
		if err != nil {
			return nil, false
		}
		src = src[len(prefix):]

		dst = append(dst, logstorage.Field{Name: key, Value: value})
	}

	return dst, true
}

// getKlogLevel returns the string representation of the given klog level character.
// See https://github.com/kubernetes/klog/blob/main/internal/severity/severity.go#L41-L47
func getKlogLevel(l byte) string {
	switch l {
	case 'I':
		return "INFO"
	case 'W':
		return "WARNING"
	case 'E':
		return "ERROR"
	case 'F':
		return "FATAL"
	}
	return "UNKNOWN"
}

func fieldIndex(fields []logstorage.Field, names []string) int {
	for _, n := range names {
		for j := range fields {
			f := &fields[j]
			if f.Name == n && f.Value != "" {
				return j
			}
		}
	}
	return -1
}

func (lfp *logFileProcessor) mustClose() {
	logstorage.PutLogRows(lfp.lr)
	lfp.lr = nil
}

type criLine struct {
	// timestamp of the log entry, from the perspective of Container Runtime.
	timestamp int64
	// partial is true if the log line is split into multiple lines.
	partial bool
	// content of the log entry.
	content []byte
}

// parseCRILine parses a log line in CRI format.
func parseCRILine(b []byte) (criLine, error) {
	n := bytes.IndexByte(b, ' ')
	if n < 0 {
		return criLine{}, fmt.Errorf("unexpected end of timestamp")
	}
	v := b[:n]
	b = b[n+1:]
	timestamp, ok := logstorage.TryParseTimestampRFC3339Nano(bytesutil.ToUnsafeString(v))
	if !ok {
		return criLine{}, fmt.Errorf("invalid timestamp %q", v)
	}

	n = bytes.IndexByte(b, ' ')
	if n < 0 {
		return criLine{}, fmt.Errorf("unexpected end of stream")
	}
	// Skip stream value.
	b = b[n+1:]

	n = bytes.IndexByte(b, ' ')
	if n < 0 {
		return criLine{}, fmt.Errorf("unexpected end of follow flag")
	}
	v = b[:n]
	b = b[n+1:]
	if len(v) != 1 {
		return criLine{}, fmt.Errorf("invalid length of follow flag")
	}
	partial := v[0] == 'P'

	content := b

	return criLine{
		timestamp: timestamp,
		partial:   partial,
		content:   content,
	}, nil
}

// parseCRILineJSON parses a log line in JSON format used by Docker 'json-file' logging driver.
// See: https://docs.docker.com/engine/logging/drivers/json-file/
func parseCRILineJSON(parser *fastjson.Parser, b []byte) (criLine, error) {
	v, err := parser.ParseBytes(b)
	if err != nil {
		return criLine{}, err
	}

	obj, err := v.Object()
	if err != nil {
		return criLine{}, err
	}

	f := obj.Get("log")
	if f == nil {
		return criLine{}, fmt.Errorf("missing 'log' field")
	}

	logContent, err := f.StringBytes()
	if err != nil {
		return criLine{}, err
	}

	f = obj.Get("time")
	if f == nil {
		return criLine{}, fmt.Errorf("missing 'time' field")
	}

	timestampContent, err := f.StringBytes()
	if err != nil {
		return criLine{}, err
	}
	timestampStr := bytesutil.ToUnsafeString(timestampContent)
	timestamp, ok := logstorage.TryParseTimestampRFC3339Nano(timestampStr)
	if !ok {
		return criLine{}, fmt.Errorf("invalid timestamp %q", timestampStr)
	}

	return criLine{
		timestamp: timestamp,
		// Assume the entire log content is always completely written.
		partial: false,
		content: logContent,
	}, nil
}

var tenantIDOnce sync.Once
var parsedTenantID logstorage.TenantID

func getTenantID() logstorage.TenantID {
	tenantIDOnce.Do(initTenantID)
	return parsedTenantID
}

func initTenantID() {
	v, err := logstorage.ParseTenantID(*tenantID)
	if err != nil {
		logger.Fatalf("cannot parse -kubernetesCollector.tenantID=%q: %s", *tenantID, err)
	}
	parsedTenantID = v
}

var extraFieldsOnce sync.Once
var parsedExtraFields []logstorage.Field

func getExtraFields() []logstorage.Field {
	extraFieldsOnce.Do(initExtraFields)
	return parsedExtraFields
}

func initExtraFields() {
	if *extraFields == "" {
		return
	}

	p := logstorage.GetJSONParser()
	if err := p.ParseLogMessage([]byte(*extraFields)); err != nil {
		logger.Fatalf("cannot parse -kubernetesCollector.extraFields=%q: %s", *extraFields, err)
	}

	fields := p.Fields
	slices.SortFunc(fields, func(a, b logstorage.Field) int {
		return cmp.Compare(a.Name, b.Name)
	})

	parsedExtraFields = fields
}

var defaultMsgFields = []string{"message", "msg", "log"}

func getMsgFields() []string {
	if len(*msgField) == 0 {
		return defaultMsgFields
	}
	return *msgField
}

var defaultTimeFields = []string{"time", "timestamp", "ts"}

func getTimeFields() []string {
	if len(*timeField) == 0 {
		return defaultTimeFields
	}
	return *timeField
}

var partialCRIContentBufPool bytesutil.ByteBufferPool

var criJSONParserPool fastjson.ParserPool
