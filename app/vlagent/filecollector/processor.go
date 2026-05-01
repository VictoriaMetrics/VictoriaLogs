package filecollector

import (
	"cmp"
	"flag"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	tenantIDs = flagutil.NewArrayString("fileCollector.tenantID", "Default tenant ID to use for logs collected from files in format: <accountID>:<projectID>. "+
		"See https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy")
	ignoreFields     = flagutil.NewArrayString("fileCollector.ignoreFields", "Fields to ignore across logs ingested from files")
	decolorizeFields = flagutil.NewArrayString("fileCollector.decolorizeFields", "Fields to remove ANSI color codes across logs ingested from files")
	msgField         = flagutil.NewArrayString("fileCollector.msgField", "Fields that may contain the _msg field. "+
		"Default: message, msg, log. See https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field")
	timeField = flagutil.NewArrayString("fileCollector.timeField", "Fields that may contain the _time field. "+
		"Default: time, timestamp, ts. If none of the specified fields is found in the log line, then the read time will be used. "+
		"See https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field")
	extraFields = flagutil.NewArrayString("fileCollector.extraFields", "Extra fields in JSON format to add to each log line collected from files. "+
		`For example, -fileCollector.extraFields='{"app":"nginx", "hostname":"%{HOST}"}'. `+
		`The "hostname" and "file" fields are injected automatically; `+
		"see -fileCollector.hostnameField and -fileCollector.fileField for details")
	fileField     = flag.String("fileCollector.fileField", "file", "Field name used to store the source file path in collected log entries. Set to empty string to disable")
	hostnameField = flag.String("fileCollector.hostnameField", "hostname", "Field name used to store the hostname in collected log entries. Set to empty string to disable")
	streamFields  = flagutil.NewArrayString("fileCollector.streamFields", "Comma-separated list of fields to use as log stream fields for logs ingested from files. "+
		"Default: -fileCollector.fileField and -fileCollector.hostnameField. See: https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields")
)

type processor struct {
	storage            insertutil.LogRowsStorage
	extraFieldsJSONLen int
	tenantID           logstorage.TenantID

	logRows *logstorage.LogRows

	rowsIngestedLocal  int
	bytesIngestedLocal int
}

func newProcessor(argIdx int, filePath string, storage insertutil.LogRowsStorage) *processor {
	efs := getExtraFields(argIdx)
	var defaultStreamFields []string

	if *fileField != "" {
		efs = append(efs, logstorage.Field{
			Name:  *fileField,
			Value: filePath,
		})
		defaultStreamFields = append(defaultStreamFields, *fileField)
	}

	if *hostnameField != "" {
		efs = append(efs, logstorage.Field{
			Name:  *hostnameField,
			Value: hostname,
		})
		defaultStreamFields = append(defaultStreamFields, *hostnameField)
	}

	sfs := *streamFields
	if len(sfs) == 0 {
		sfs = defaultStreamFields
	}

	logRows := logstorage.GetLogRows(sfs, *ignoreFields, *decolorizeFields, efs, *insertutil.DefaultMsgValue)

	return &processor{
		storage:            storage,
		extraFieldsJSONLen: logstorage.EstimatedJSONRowLen(efs),
		tenantID:           getTenantID(argIdx),
		logRows:            logRows,
	}
}

func (p *processor) TryAddLine(line []byte) bool {
	if len(line) == 0 {
		// Skip empty lines to avoid zero-value logs with the content "missing _msg field".
		return true
	}

	parser := logstorage.GetJSONParser()
	defer logstorage.PutJSONParser(parser)

	ok := false
	if line[0] == '{' {
		// Automatically parse JSON, since there's no sense to use an unstructured log format.
		err := parser.ParseLogMessage(line, nil, "")
		ok = err == nil
		// Rename the message field to _msg.
		logstorage.RenameField(parser.Fields, getMsgFields(), "_msg")
	}
	if !ok {
		parser.Fields = append(parser.Fields, logstorage.Field{
			Name:  "_msg",
			Value: bytesutil.ToUnsafeString(line),
		})
	}

	// Try to parse timestamp from the time fields.
	timestamp, err := insertutil.ExtractTimestampFromFields(getTimeFields(), parser.Fields)
	if err != nil {
		timestamp = time.Now().UnixNano()
	}

	if len(parser.Fields) > 1000 {
		line := logstorage.MarshalFieldsToJSON(nil, parser.Fields)
		logger.Warnf("dropping log line with %d fields; %s", len(parser.Fields), line)
		rowsDroppedTotalTooManyFields.Inc()
		return true
	}

	p.logRows.MustAdd(p.tenantID, timestamp, parser.Fields, -1)
	p.storage.MustAddRows(p.logRows)
	p.logRows.ResetKeepSettings()

	p.rowsIngestedLocal++
	p.bytesIngestedLocal += p.extraFieldsJSONLen + len(line)
	if p.rowsIngestedLocal > 128 {
		p.flushMetrics()
	}

	return true
}

var rowsDroppedTotalTooManyFields = metrics.GetOrCreateCounter(`vl_rows_dropped_total{reason="too_many_fields"}`)

func (p *processor) Flush() {
	p.flushMetrics()
}

func (p *processor) flushMetrics() {
	if p.rowsIngestedLocal == 0 {
		return
	}
	rowsIngestedTotal.Add(p.rowsIngestedLocal)
	bytesIngestedTotal.Add(p.bytesIngestedLocal)
	p.rowsIngestedLocal = 0
	p.bytesIngestedLocal = 0
}

var rowsIngestedTotal = metrics.GetOrCreateCounter(fmt.Sprintf("vl_rows_ingested_total{type=%q}", "file_logs"))
var bytesIngestedTotal = metrics.GetOrCreateCounter(fmt.Sprintf("vl_bytes_ingested_total{type=%q}", "file_logs"))

func (p *processor) MustClose() {
	p.Flush()
	logstorage.PutLogRows(p.logRows)
	p.logRows = nil
}

var parsedTenantIDs []logstorage.TenantID

func getTenantID(argIdx int) logstorage.TenantID {
	if argIdx >= len(parsedTenantIDs) {
		return logstorage.TenantID{}
	}
	return parsedTenantIDs[argIdx]
}

func initTenantIDs() {
	parsedTenantIDs = make([]logstorage.TenantID, 0, len(*tenantIDs))
	for i := range len(*glob) {
		s := tenantIDs.GetOptionalArg(i)
		if s == "" {
			parsedTenantIDs = append(parsedTenantIDs, logstorage.TenantID{})
			continue
		}
		v, err := logstorage.ParseTenantID(s)
		if err != nil {
			logger.Fatalf("cannot parse -fileCollector.tenantID=%q: %s", s, err)
		}
		parsedTenantIDs = append(parsedTenantIDs, v)
	}
}

var parsedExtraFields [][]logstorage.Field

func getExtraFields(argIdx int) []logstorage.Field {
	if argIdx >= len(parsedExtraFields) {
		return nil
	}
	return parsedExtraFields[argIdx]
}

func initExtraFields() {
	for i := range *glob {
		s := extraFields.GetOptionalArg(i)
		if s == "" {
			parsedExtraFields = append(parsedExtraFields, nil)
			continue
		}

		p := logstorage.GetJSONParser()
		if err := p.ParseLogMessage([]byte(s), nil, ""); err != nil {
			logger.Fatalf("cannot parse -fileCollector.extraFields=%q: %s", *extraFields, err)
		}

		slices.SortFunc(p.Fields, func(a, b logstorage.Field) int {
			return cmp.Compare(a.Name, b.Name)
		})

		parsedExtraFields = append(parsedExtraFields, p.Fields)
	}
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

var hostname string

func initHostname() {
	s, err := os.Hostname()
	if err != nil {
		logger.Fatalf("cannot get hostname: %s", err)
	}
	hostname = s
}
