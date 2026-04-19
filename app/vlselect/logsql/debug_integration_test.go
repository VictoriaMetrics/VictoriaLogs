//go:build integration
// +build integration

package logsql

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestDebugAPIIntegration tests the complete flow of the debug API
func TestDebugAPIIntegration(t *testing.T) {
	testCases := []struct {
		name        string
		query       string
		logs        []map[string]interface{}
		expectError bool
		validate    func(t *testing.T, resp debugResponse)
	}{
		{
			name:  "wildcard query returns all logs",
			query: "*",
			logs: []map[string]interface{}{
				{"_time": "2024-01-01T10:00:00Z", "_msg": "log 1", "level": "info"},
				{"_time": "2024-01-01T10:01:00Z", "_msg": "log 2", "level": "error"},
				{"_time": "2024-01-01T10:02:00Z", "_msg": "log 3", "level": "warning"},
			},
			expectError: false,
			validate: func(t *testing.T, resp debugResponse) {
				if resp.Status != "success" {
					t.Errorf("expected success status, got %s", resp.Status)
				}
				if resp.Data.Stats.RowsScanned != 3 {
					t.Errorf("expected 3 rows scanned, got %d", resp.Data.Stats.RowsScanned)
				}
				if len(resp.Data.Results) != 3 {
					t.Errorf("expected 3 results, got %d", len(resp.Data.Results))
				}
			},
		},
		{
			name:  "query with time field",
			query: "*",
			logs: []map[string]interface{}{
				{"_time": "2024-01-01T10:00:00Z", "_msg": "test 1"},
				{"_time": "2024-01-01T11:00:00Z", "_msg": "test 2"},
			},
			expectError: false,
			validate: func(t *testing.T, resp debugResponse) {
				if resp.Status != "success" {
					t.Errorf("expected success status, got %s", resp.Status)
				}
				// Verify _time field is present in results
				for _, result := range resp.Data.Results {
					if _, ok := result["_time"]; !ok {
						t.Errorf("expected _time field in result, got %v", result)
					}
				}
			},
		},
		{
			name:  "query with custom fields",
			query: "*",
			logs: []map[string]interface{}{
				{
					"_time":   "2024-01-01T10:00:00Z",
					"_msg":    "user login",
					"user":    "alice",
					"ip":      "192.168.1.1",
					"success": true,
				},
				{
					"_time":   "2024-01-01T10:01:00Z",
					"_msg":    "user login",
					"user":    "bob",
					"ip":      "192.168.1.2",
					"success": false,
				},
			},
			expectError: false,
			validate: func(t *testing.T, resp debugResponse) {
				if resp.Status != "success" {
					t.Errorf("expected success status, got %s", resp.Status)
				}
				// Verify custom fields are present
				for _, result := range resp.Data.Results {
					if _, ok := result["user"]; !ok {
						t.Errorf("expected user field in result, got %v", result)
					}
					if _, ok := result["ip"]; !ok {
						t.Errorf("expected ip field in result, got %v", result)
					}
				}
			},
		},
		{
			name:  "query with numeric values",
			query: "*",
			logs: []map[string]interface{}{
				{"_time": "2024-01-01T10:00:00Z", "_msg": "metric", "value": 123.45, "count": 10.0},
				{"_time": "2024-01-01T10:01:00Z", "_msg": "metric", "value": 678.90, "count": 20.0},
			},
			expectError: false,
			validate: func(t *testing.T, resp debugResponse) {
				if resp.Status != "success" {
					t.Errorf("expected success status, got %s", resp.Status)
				}
				// Verify numeric values are converted to strings
				for _, result := range resp.Data.Results {
					if val, ok := result["value"]; ok {
						if val == "" {
							t.Errorf("expected non-empty value field, got empty string")
						}
					}
				}
			},
		},
		{
			name:        "empty logs array",
			query:       "*",
			logs:        []map[string]interface{}{},
			expectError: true,
			validate: func(t *testing.T, resp debugResponse) {
				if resp.Status != "error" {
					t.Errorf("expected error status, got %s", resp.Status)
				}
				if resp.Error == "" {
					t.Errorf("expected error message, got empty string")
				}
			},
		},
		{
			name:  "invalid query with multiple pipe separators",
			query: "* | | stats",
			logs: []map[string]interface{}{
				{"_time": "2024-01-01T10:00:00Z", "_msg": "test"},
			},
			expectError: true,
			validate: func(t *testing.T, resp debugResponse) {
				if resp.Status != "error" {
					t.Errorf("expected error status, got %s", resp.Status)
				}
				if resp.Error == "" {
					t.Errorf("expected error message, got empty string")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create request
			reqBody := debugRequest{
				Query: tc.query,
				Logs:  tc.logs,
			}
			body, err := json.Marshal(reqBody)
			if err != nil {
				t.Fatalf("failed to marshal request: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/select/logsql/debug", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			// Create response recorder
			w := httptest.NewRecorder()

			// Call handler
			ProcessDebugRequest(context.Background(), w, req)

			// Parse response
			var resp debugResponse
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}

			// Validate response
			tc.validate(t, resp)
		})
	}
}

// TestDebugAPIHTTPMethods tests that only POST is allowed
func TestDebugAPIHTTPMethods(t *testing.T) {
	methods := []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/select/logsql/debug", nil)
			w := httptest.NewRecorder()

			ProcessDebugRequest(context.Background(), w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("expected status %d for method %s, got %d", http.StatusBadRequest, method, w.Code)
			}
		})
	}
}

// TestDebugAPIContentType tests various content types
func TestDebugAPIContentType(t *testing.T) {
	reqBody := debugRequest{
		Query: "*",
		Logs: []map[string]interface{}{
			{"_time": "2024-01-01T10:00:00Z", "_msg": "test"},
		},
	}
	body, _ := json.Marshal(reqBody)

	testCases := []struct {
		name        string
		contentType string
		expectError bool
	}{
		{
			name:        "valid json content type",
			contentType: "application/json",
			expectError: false,
		},
		{
			name:        "json with charset",
			contentType: "application/json; charset=utf-8",
			expectError: false,
		},
		{
			name:        "no content type",
			contentType: "",
			expectError: false, // JSON decoder should still work
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/select/logsql/debug", bytes.NewReader(body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}

			w := httptest.NewRecorder()
			ProcessDebugRequest(context.Background(), w, req)

			var resp debugResponse
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}

			if tc.expectError && resp.Status != "error" {
				t.Errorf("expected error status, got %s", resp.Status)
			}
			if !tc.expectError && resp.Status != "success" {
				t.Errorf("expected success status, got %s: %s", resp.Status, resp.Error)
			}
		})
	}
}
