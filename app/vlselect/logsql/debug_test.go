package logsql

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProcessDebugRequest(t *testing.T) {
	tests := []struct {
		name         string
		requestBody  debugRequest
		wantStatus   int
		wantError    bool
		checkResults bool
		expectedRows int
	}{
		{
			name: "simple query with sample logs",
			requestBody: debugRequest{
				Query: "*",
				Logs: []map[string]interface{}{
					{"_time": "2024-01-01T10:00:00Z", "_msg": "error occurred", "level": "error"},
					{"_time": "2024-01-01T10:01:00Z", "_msg": "info message", "level": "info"},
				},
			},
			wantStatus:   http.StatusOK,
			wantError:    false,
			checkResults: true,
			expectedRows: 2,
		},
		{
			name: "missing query field",
			requestBody: debugRequest{
				Query: "",
				Logs: []map[string]interface{}{
					{"_time": "2024-01-01T10:00:00Z", "_msg": "test"},
				},
			},
			wantStatus: http.StatusBadRequest,
			wantError:  true,
		},
		{
			name: "missing logs field",
			requestBody: debugRequest{
				Query: "*",
				Logs:  []map[string]interface{}{},
			},
			wantStatus: http.StatusBadRequest,
			wantError:  true,
		},
		{
			name: "invalid query syntax",
			requestBody: debugRequest{
				Query: "invalid | | syntax",
				Logs: []map[string]interface{}{
					{"_time": "2024-01-01T10:00:00Z", "_msg": "test"},
				},
			},
			wantStatus: http.StatusBadRequest,
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			body, err := json.Marshal(tt.requestBody)
			if err != nil {
				t.Fatalf("failed to marshal request body: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/select/logsql/debug", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			// Create response recorder
			w := httptest.NewRecorder()

			// Call the handler
			ProcessDebugRequest(context.Background(), w, req)

			// Check status code
			if w.Code != tt.wantStatus {
				t.Errorf("ProcessDebugRequest() status = %v, want %v", w.Code, tt.wantStatus)
			}

			// Parse response
			var resp debugResponse
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}

			// Check error status
			if tt.wantError {
				if resp.Status != "error" {
					t.Errorf("expected error status, got %s", resp.Status)
				}
				if resp.Error == "" {
					t.Errorf("expected error message, got empty string")
				}
			} else {
				if resp.Status != "success" {
					t.Errorf("expected success status, got %s: %s", resp.Status, resp.Error)
				}
			}

			// Check results if needed
			if tt.checkResults && !tt.wantError {
				if resp.Data.Stats.RowsScanned != tt.expectedRows {
					t.Errorf("expected %d rows scanned, got %d", tt.expectedRows, resp.Data.Stats.RowsScanned)
				}
				if len(resp.Data.Results) != tt.expectedRows {
					t.Errorf("expected %d results, got %d", tt.expectedRows, len(resp.Data.Results))
				}
			}
		})
	}
}

func TestConvertToString(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  string
	}{
		{
			name:  "string value",
			input: "test",
			want:  "test",
		},
		{
			name:  "float64 value",
			input: 123.45,
			want:  "123.45",
		},
		{
			name:  "bool value true",
			input: true,
			want:  "true",
		},
		{
			name:  "bool value false",
			input: false,
			want:  "false",
		},
		{
			name:  "nil value",
			input: nil,
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertToString(tt.input)
			if got != tt.want {
				t.Errorf("convertToString() = %v, want %v", got, tt.want)
			}
		})
	}
}
