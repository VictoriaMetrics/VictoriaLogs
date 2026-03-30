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
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
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
	streamFields = flagutil.NewArrayString("kubernetesCollector.streamFields", "Comma-separated list of fields to use as log stream fields for logs ingested from Kubernetes Pods. "+
		"Default: kubernetes.container_name,kubernetes.pod_name,kubernetes.pod_namespace. "+
		"See: https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields")

	includePodLabels = flag.Bool("kubernetesCollector.includePodLabels", true, "Include Pod labels as additional fields in the log entries. "+
		"Even this setting is disabled, Pod labels are available for filtering via -kubernetesCollector.excludeFilter flag")
	includePodAnnotations = flag.Bool("kubernetesCollector.includePodAnnotations", false, "Include Pod annotations as additional fields in the log entries. "+
		"Even this setting is disabled, Pod annotations are available for filtering via -kubernetesCollector.excludeFilter flag")
	includeNodeLabels = flag.Bool("kubernetesCollector.includeNodeLabels", false, "Include Node labels as additional fields in the log entries. "+
		"Even this setting is disabled, Node labels are available for filtering via -kubernetesCollector.excludeFilter flag")
	includeNodeAnnotations = flag.Bool("kubernetesCollector.includeNodeAnnotations", false, "Include Node annotations as additional fields in the log entries. "+
		"Even this setting is disabled, Node annotations are available for filtering via -kubernetesCollector.excludeFilter flag")
	includeNamespaceLabels = flag.Bool("kubernetesCollector.includeNamespaceLabels", false, "Include Namespace labels as additional fields in the log entries. "+
		"Even this setting is disabled, Namespace labels are available for filtering via -kubernetesCollector.excludeFilter flag")
	includeNamespaceAnnotations = flag.Bool("kubernetesCollector.includeNamespaceAnnotations", false, "Include Namespace annotations as additional fields in the log entries. "+
		"Even this setting is disabled, Namespace annotations are available for filtering via -kubernetesCollector.excludeFilter flag")
)

type logFileProcessor struct {
	storage  insertutil.LogRowsStorage
	lr       *logstorage.LogRows
	tenantID logstorage.TenantID

	// commonFields are common fields for the given log file.
	commonFields []logstorage.Field

	// fieldsBuf is used for constructing log fields from commonFields and the actual log line fields before sending them to VictoriaLogs.
	fieldsBuf []logstorage.Field

	partialCRIStdout partialCRILineState
	partialCRIStderr partialCRILineState
}

// newLogFileProcessor returns a new logFileProcessor for the given storage.
// commonFields must not be modified as they can be accessed from multiple goroutines.
func newLogFileProcessor(storage insertutil.LogRowsStorage, commonFields []logstorage.Field) *logFileProcessor {
	var fs []logstorage.Field
	for _, f := range commonFields {
		if shouldIncludeMetadataField(f.Name) {
			fs = append(fs, f)
		}
	}
	commonFields = fs

	sfs := getStreamFields()
	efs := getExtraFields()
	const defaultMsgValue = "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"
	lr := logstorage.GetLogRows(sfs, *ignoreFields, *decolorizeFields, efs, defaultMsgValue)

	return &logFileProcessor{
		storage:      storage,
		lr:           lr,
		tenantID:     getTenantID(),
		commonFields: commonFields,
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
			pod := mustGetFieldValByName(lfp.commonFields, "kubernetes.pod_name")
			namespace := mustGetFieldValByName(lfp.commonFields, "kubernetes.pod_namespace")
			invalidCRILineLogger.Errorf("skipping invalid json-file log line %q from Pod %q in Namespace %q: %s; "+
				"see https://docs.victoriametrics.com/victorialogs/vlagent/#troubleshooting for more details", logLine, pod, namespace, err)
			return true
		}

		lfp.addLineInternal(criLine.timestamp, criLine.content)

		return true
	}

	criLine, err := parseCRILine(logLine)
	if err != nil {
		pod := mustGetFieldValByName(lfp.commonFields, "kubernetes.pod_name")
		namespace := mustGetFieldValByName(lfp.commonFields, "kubernetes.pod_namespace")
		lfp.partialCRIStdout.reset()
		lfp.partialCRIStderr.reset()
		invalidCRILineLogger.Errorf("skipping invalid CRI log line %q from Pod %q in Namespace %q: %s; "+
			"see https://docs.victoriametrics.com/victorialogs/vlagent/#troubleshooting for more details", logLine, pod, namespace, err)
		return true
	}

	prevState := &lfp.partialCRIStderr
	if criLine.stream == streamStdout {
		prevState = &lfp.partialCRIStdout
	}
	timestamp, content, ok := lfp.joinPartialLines(prevState, criLine)
	if !ok {
		// The log content is not yet complete.
		return false
	}
	defer prevState.reset()

	if len(content) == 0 {
		// The log content is truncated or empty.
		// Skip such lines.
		return true
	}

	lfp.addLineInternal(timestamp, content)
	return true
}

var invalidCRILineLogger = logger.WithThrottler("invalid_cri_log_line", 5*time.Second)

type partialCRILineState struct {
	// content accumulates the content of partial CRI log lines.
	// Can be truncated if it exceeds maxLineSize.
	content *bytesutil.ByteBuffer
	// size tracks the actual size of the content.
	size int
}

func (pcs *partialCRILineState) reset() {
	if pcs.content != nil {
		partialCRIContentBufPool.Put(pcs.content)
		pcs.content = nil
	}
	pcs.size = 0
}

func (lfp *logFileProcessor) joinPartialLines(state *partialCRILineState, criLine criLine) (int64, []byte, bool) {
	if !criLine.partial && (state.content == nil || state.content.Len() == 0) {
		// Fast path: the log line is complete and not split.
		return criLine.timestamp, criLine.content, true
	}
	// Slow path: line is split into multiple lines.
	return lfp.joinPartialLinesSlow(state, criLine)
}

func (lfp *logFileProcessor) joinPartialLinesSlow(state *partialCRILineState, criLine criLine) (int64, []byte, bool) {
	if criLine.partial {
		// The log line is split into multiple lines.
		// Accumulate the content until the full line is received.

		if state.content == nil {
			state.content = partialCRIContentBufPool.Get()
		}

		state.size += len(criLine.content)
		if state.size <= maxLogLineSize {
			state.content.MustWrite(criLine.content)
		}
		return 0, nil, false
	}

	// The final part of the split log line received.

	state.size += len(criLine.content)
	if state.size > maxLogLineSize {
		// Discard the too large log line.
		pod := mustGetFieldValByName(lfp.commonFields, "kubernetes.pod_name")
		namespace := mustGetFieldValByName(lfp.commonFields, "kubernetes.pod_namespace")
		logLineExceedsMaxLineSizeLogger.Warnf("skipping log entry from Pod %q in namespace %q: entry size of %.2f MiB exceeds the maximum allowed size of %d MiB",
			pod, namespace, float64(state.size)/1024/1024, maxLogLineSize/1024/1024)
		return 0, nil, true
	}

	state.content.MustWrite(criLine.content)
	content := state.content.B
	return criLine.timestamp, content, true
}

var logLineExceedsMaxLineSizeLogger = logger.WithThrottler("log_line_exceeds_max_line_size", 5*time.Second)

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

	lfp.lr.MustAdd(lfp.tenantID, timestamp, lfp.fieldsBuf, -1)
	lfp.storage.MustAddRows(lfp.lr)
	lfp.lr.ResetKeepSettings()
}

func parseLogRowContent(p *logstorage.JSONParser, data []byte) (int64, bool) {
	if len(data) == 0 {
		return 0, false
	}

	switch data[0] {
	case '{':
		err := p.ParseLogMessage(data, nil)
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
		ts := fasttime.UnixTimestamp()
		current := time.Unix(int64(ts), 0).UTC()
		timestamp, fields, ok := tryParseKlog(p.Fields, bytesutil.ToUnsafeString(data), current)
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
func tryParseKlog(dst []logstorage.Field, src string, current time.Time) (int64, []logstorage.Field, bool) {
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
	t = t.AddDate(current.Year(), 0, 0)
	if t.Add(-time.Hour * 24).After(current) {
		// Adjust time to the previous year.
		t = t.AddDate(-1, 0, 0)
	}
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
	lfp.partialCRIStdout.reset()
	lfp.partialCRIStderr.reset()
	logstorage.PutLogRows(lfp.lr)
	lfp.lr = nil
}

type stream byte

const (
	streamStdout stream = 0
	streamStderr stream = 1
)

type criLine struct {
	// timestamp of the log entry, from the perspective of Container Runtime.
	timestamp int64
	// stream contains the output stream such as stdout or stderr.
	stream stream
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
	stream := streamStderr
	if string(b[:n]) == "stdout" {
		stream = streamStdout
	}
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
		stream:    stream,
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
	if err := p.ParseLogMessage([]byte(*extraFields), nil); err != nil {
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

// defaultStreamFields is a list of default _stream fields.
// Must be synced with getCommonFields.
var defaultStreamFields = []string{"kubernetes.container_name", "kubernetes.pod_name", "kubernetes.pod_namespace"}

func getStreamFields() []string {
	if len(*streamFields) == 0 {
		return defaultStreamFields
	}
	return *streamFields
}

var metadataIncludeFlags map[string]bool
var initMetadataIncludeFlagsOnce sync.Once

func initMetadataIncludeFlags() {
	metadataIncludeFlags = map[string]bool{
		"kubernetes.pod_labels.":            *includePodLabels,
		"kubernetes.pod_annotations.":       *includePodAnnotations,
		"kubernetes.node_labels.":           *includeNodeLabels,
		"kubernetes.node_annotations.":      *includeNodeAnnotations,
		"kubernetes.namespace_labels.":      *includeNamespaceLabels,
		"kubernetes.namespace_annotations.": *includeNamespaceAnnotations,
	}
}

func shouldIncludeMetadataField(field string) bool {
	initMetadataIncludeFlagsOnce.Do(initMetadataIncludeFlags)

	for prefix, include := range metadataIncludeFlags {
		if strings.HasPrefix(field, prefix) {
			return include
		}
	}
	// Not a metadata field.
	return true
}

var partialCRIContentBufPool bytesutil.ByteBufferPool

var criJSONParserPool fastjson.ParserPool

func mustGetFieldValByName(commonFields []logstorage.Field, fieldName string) string {
	n := slices.IndexFunc(commonFields, func(f logstorage.Field) bool {
		return f.Name == fieldName
	})
	if n < 0 {
		panic(fmt.Errorf("BUG: cannot find field %q in commonFields", fieldName))
	}
	return commonFields[n].Value
}
