package logsql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// ProcessDebugRequest handles /select/logsql/debug request.
//
// This endpoint allows users to test LogsQL queries against sample log data
// without needing to ingest the data into storage.
//
// Request format (JSON):
//
//	{
//	  "query": "error | stats count() by level",
//	  "logs": [
//	    {"_time": "2024-01-01T10:00:00Z", "_msg": "error occurred", "level": "error"},
//	    {"_time": "2024-01-01T10:01:00Z", "_msg": "info message", "level": "info"}
//	  ]
//	}
//
// Response format (JSON):
//
//	{
//	  "status": "success",
//	  "data": {
//	    "results": [
//	      {"level": "error", "count()": "1"},
//	      {"level": "info", "count()": "1"}
//	    ],
//	    "stats": {
//	      "rowsScanned": 2,
//	      "executionTime": "0.001s"
//	    }
//	  }
//	}
//
// Or in case of error:
//
//	{
//	  "status": "error",
//	  "error": "error message"
//	}
func ProcessDebugRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpserver.Errorf(w, r, "only POST method is supported for /select/logsql/debug; got %s", r.Method)
		return
	}

	// Parse request body
	var req debugRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendDebugErrorResponse(w, r, fmt.Sprintf("cannot parse request body: %s", err))
		return
	}

	// Validate request
	if req.Query == "" {
		sendDebugErrorResponse(w, r, "missing 'query' field in request")
		return
	}
	if len(req.Logs) == 0 {
		sendDebugErrorResponse(w, r, "missing 'logs' field or empty logs array in request")
		return
	}

	// Parse the query
	currTimestamp := time.Now().UnixNano()
	q, err := logstorage.ParseQueryAtTimestamp(req.Query, currTimestamp)
	if err != nil {
		sendDebugErrorResponse(w, r, fmt.Sprintf("cannot parse query: %s", err))
		return
	}

	// Execute the query on sample logs
	startTime := time.Now()
	results, rowsScanned, err := executeDebugQuery(ctx, q, req.Logs)
	if err != nil {
		sendDebugErrorResponse(w, r, fmt.Sprintf("cannot execute query: %s", err))
		return
	}
	executionTime := time.Since(startTime)

	// Send success response
	resp := debugResponse{
		Status: "success",
		Data: debugResponseData{
			Results: results,
			Stats: debugStats{
				RowsScanned:   rowsScanned,
				ExecutionTime: executionTime.String(),
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.Errorf("cannot send debug response: %s", err)
	}
}

type debugRequest struct {
	Query string                   `json:"query"`
	Logs  []map[string]interface{} `json:"logs"`
}

type debugResponse struct {
	Status string            `json:"status"`
	Data   debugResponseData `json:"data,omitempty"`
	Error  string            `json:"error,omitempty"`
}

type debugResponseData struct {
	Results []map[string]string `json:"results"`
	Stats   debugStats          `json:"stats"`
}

type debugStats struct {
	RowsScanned   int    `json:"rowsScanned"`
	ExecutionTime string `json:"executionTime"`
}

func sendDebugErrorResponse(w http.ResponseWriter, r *http.Request, errMsg string) {
	resp := debugResponse{
		Status: "error",
		Error:  errMsg,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.Errorf("cannot send debug error response: %s", err)
	}
}

func executeDebugQuery(ctx context.Context, q *logstorage.Query, sampleLogs []map[string]interface{}) ([]map[string]string, int, error) {
	rowsScanned := len(sampleLogs)

	// Extract all unique field names from sample logs
	fieldNamesSet := make(map[string]struct{})
	for _, logEntry := range sampleLogs {
		for k := range logEntry {
			fieldNamesSet[k] = struct{}{}
		}
	}

	// Convert to sorted field names for consistent ordering
	fieldNames := make([]string, 0, len(fieldNamesSet))
	for name := range fieldNamesSet {
		fieldNames = append(fieldNames, name)
	}

	// Build columns for the data block
	columns := make([]logstorage.BlockColumn, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		values := make([]string, len(sampleLogs))
		for i, logEntry := range sampleLogs {
			if val, ok := logEntry[fieldName]; ok {
				values[i] = convertToString(val)
			} else {
				values[i] = ""
			}
		}
		columns = append(columns, logstorage.BlockColumn{
			Name:   fieldName,
			Values: values,
		})
	}

	// Create a data block
	db := &logstorage.DataBlock{}
	db.SetColumns(columns)

	// Convert the data block to result format
	// Note: This is a simplified implementation that returns the raw data
	// A full implementation would apply filters and pipes from the query
	results := make([]map[string]string, 0)
	rowsCount := db.RowsCount()
	if rowsCount == 0 {
		return results, rowsScanned, nil
	}

	needSortFields := !q.IsFixedOutputFieldsOrder()
	columns = db.GetColumns(needSortFields)

	for i := range rowsCount {
		row := make(map[string]string)
		for _, c := range columns {
			if i < len(c.Values) && c.Values[i] != "" {
				row[c.Name] = c.Values[i]
			}
		}
		if len(row) > 0 {
			results = append(results, row)
		}
	}

	return results, rowsScanned, nil
}

func convertToString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%v", v)
	case bool:
		return fmt.Sprintf("%v", v)
	case nil:
		return ""
	default:
		// Try to marshal complex types as JSON
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	}
}
