package kubernetescollector

import (
	"bytes"
	"cmp"
	"flag"
	"fmt"
	"slices"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/valyala/fastjson"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	tenantID = flag.String("kubernetesCollector.tenantID", "0:0",
		"Default tenant ID to use for logs collected from Kubernetes pods in format: <accountID>:<projectID>")
	ignoreFields     = flagutil.NewArrayString("kubernetesCollector.ignoreFields", "Fields to ignore across logs ingested from Kubernetes")
	decolorizeFields = flagutil.NewArrayString("kubernetesCollector.decolorizeFields", "Fields to remove ANSI color codes across logs ingested from Kubernetes")
	msgField         = flagutil.NewArrayString("kubernetesCollector.msgField", "Fields that may contain the _msg field. "+
		"Default: message,msg,log. See https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field")
	timeField = flagutil.NewArrayString("kubernetesCollector.timeField", "Fields that may contain the _time field. "+
		"Default: time,timestamp,ts. If none of the specified fields is found in the log line, then the write time will be used. "+
		"See https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field")
	extraFields = flag.String("kubernetesCollector.extraFields", "", "Extra fields to add to each log line collected from Kubernetes pods in JSON format. "+
		`For example: -kubernetes.extraFields='{"cluster":"cluster-1","env":"production"}'`)
)

type logFileProcessor struct {
	storage  insertutil.LogRowsStorage
	lr       *logstorage.LogRows
	tenantID logstorage.TenantID

	commonFields []logstorage.Field
	streamFields []logstorage.Field

	// partialCRIContent accumulates the content of partial CRI log lines.
	// Can be truncated if it exceeds maxLineSize.
	partialCRIContent *bytesutil.ByteBuffer
	// partialCRIContentSize tracks the actual size of the partialCRIContent.
	partialCRIContentSize int
}

func newLogFileProcessor(storage insertutil.LogRowsStorage, commonFields []logstorage.Field) *logFileProcessor {
	var streamFields []logstorage.Field
	for _, f := range commonFields {
		for _, fieldName := range streamFieldNames {
			if f.Name == fieldName {
				streamFields = append(streamFields, f)
				break
			}
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

		commonFields: commonFields,
		streamFields: streamFields,
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
		reportLogRowSizeExceeded(lfp.streamFields, lfp.partialCRIContentSize)

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

func (lfp *logFileProcessor) addLineInternal(timestamp int64, line []byte) {
	parser := logstorage.GetJSONParser()
	defer logstorage.PutJSONParser(parser)

	ok := parseLogRowContent(parser, line)
	if !ok {
		parser.Fields = append(parser.Fields, logstorage.Field{
			Name:  "_msg",
			Value: bytesutil.ToUnsafeString(line),
		})
	} else {
		// vlagent should override the timestamp from CRI to the timestamp parsed from the log line.
		n := fieldIndex(parser.Fields, *timeField)
		if n >= 0 {
			f := &parser.Fields[n]
			v, ok := logstorage.TryParseTimestampRFC3339Nano(f.Value)
			if ok {
				timestamp = v
				// Set the time field to empty string to ignore it during data ingestion.
				f.Value = ""
			}
		}

		logstorage.RenameField(parser.Fields, *msgField, "_msg")
	}

	parser.Fields = append(parser.Fields, lfp.commonFields...)

	if len(parser.Fields) > 1000 {
		line := logstorage.MarshalFieldsToJSON(nil, parser.Fields)
		logger.Warnf("dropping log line with %d fields; %s", parser.Fields, line)
		return
	}

	lfp.addRow(timestamp, parser.Fields)
}

func (lfp *logFileProcessor) addRow(timestamp int64, fields []logstorage.Field) {
	lfp.lr.MustAdd(lfp.tenantID, timestamp, fields, lfp.streamFields)
	lfp.storage.MustAddRows(lfp.lr)
	lfp.lr.ResetKeepSettings()
}

func parseLogRowContent(dst *logstorage.JSONParser, data []byte) bool {
	if len(data) == 0 {
		return false
	}

	switch data[0] {
	case '{':
		err := dst.ParseLogMessage(data)
		if err != nil {
			return false
		}
		return true
	case 'I', 'W', 'E', 'F':
		// todo: parse klog native format
		// Template of this format: Lmmdd hh:mm:ss.uuuuuu threadid file:line] "<_msg>" key1="value" key2="value"
		// See: https://kubernetes.io/docs/concepts/cluster-administration/system-logs/
	case '1', '2':
		// todo: parse nginx error log format
		// Template of this format: yyyy/mm/dd hh:mm:ss [level] pid#tid: *cid message
	}
	return false
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
		logger.Fatalf("cannot parse -kubernetes.tenantID=%q: %s", *tenantID, err)
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
		logger.Fatalf("cannot parse -kubernetes.extraFields=%q: %s", *extraFields, err)
	}

	fields := p.Fields
	slices.SortFunc(fields, func(a, b logstorage.Field) int {
		return cmp.Compare(a.Name, b.Name)
	})

	parsedExtraFields = fields
}

var partialCRIContentBufPool bytesutil.ByteBufferPool

var criJSONParserPool fastjson.ParserPool
