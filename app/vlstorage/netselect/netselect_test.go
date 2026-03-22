package netselect

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestGetFirstError_AllSuccess_NoPartialResponse(t *testing.T) {
	errs := []error{nil, nil, nil}
	qs := &logstorage.QueryStats{}

	err := getFirstError(errs, false, qs)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 0 {
		t.Fatalf("expected IsPartial=0 (false) when all backends succeed, got %d", isPartial)
	}
}

func TestGetFirstError_AllSuccess_WithPartialResponse(t *testing.T) {
	errs := []error{nil, nil, nil}
	qs := &logstorage.QueryStats{}

	err := getFirstError(errs, true, qs)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 0 {
		t.Fatalf("expected IsPartial=0 (false) when all backends succeed, got %d", isPartial)
	}
}

func TestGetFirstError_OneFailure_NoPartialResponse(t *testing.T) {
	errs := []error{nil, errors.New("backend error"), nil}
	qs := &logstorage.QueryStats{}

	err := getFirstError(errs, false, qs)
	if err == nil {
		t.Fatalf("expected error when allowPartialResponse=false and one backend fails")
	}

	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 0 {
		t.Fatalf("expected IsPartial=0 (false) when returning error in non-partial mode, got %d", isPartial)
	}
}

func TestGetFirstError_SomeUnavailable_WithPartialResponse(t *testing.T) {
	// Create unavailable backend errors (wrapped in httpserver.ErrorWithStatusCode)
	unavailableErr := &httpserver.ErrorWithStatusCode{
		Err:        errors.New("connection refused"),
		StatusCode: 503,
	}

	errs := []error{nil, unavailableErr, nil}
	qs := &logstorage.QueryStats{}

	err := getFirstError(errs, true, qs)
	if err != nil {
		t.Fatalf("expected no error when some backends succeed with allowPartialResponse=true, got: %v", err)
	}

	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 1 {
		t.Fatalf("expected IsPartial=1 (true) when some backends unavailable but others succeed, got %d", isPartial)
	}
}

func TestGetFirstError_AllUnavailable_WithPartialResponse(t *testing.T) {
	// Create unavailable backend errors
	unavailableErr1 := &httpserver.ErrorWithStatusCode{
		Err:        errors.New("connection refused"),
		StatusCode: 503,
	}
	unavailableErr2 := &httpserver.ErrorWithStatusCode{
		Err:        errors.New("timeout"),
		StatusCode: 504,
	}

	errs := []error{unavailableErr1, unavailableErr2}
	qs := &logstorage.QueryStats{}

	err := getFirstError(errs, true, qs)
	if err == nil {
		t.Fatalf("expected error when all backends are unavailable")
	}

	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 1 {
		t.Fatalf("expected IsPartial=1 (true) when all backends unavailable, got %d", isPartial)
	}
}

func TestGetFirstError_ConfigError_WithPartialResponse(t *testing.T) {
	// Configuration error (not an unavailable backend error)
	configErr := errors.New("invalid query syntax")

	errs := []error{nil, configErr, nil}
	qs := &logstorage.QueryStats{}

	err := getFirstError(errs, true, qs)
	if err == nil {
		t.Fatalf("expected error when backend returns configuration error")
	}
	if !errors.Is(err, configErr) {
		t.Fatalf("expected error to wrap configuration error")
	}

	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 0 {
		t.Fatalf("expected IsPartial=0 (false) when returning configuration error, got %d", isPartial)
	}
}

func TestGetFirstError_NilQueryStats(t *testing.T) {
	// Test that passing nil QueryStats doesn't panic
	errs := []error{nil, nil}

	err := getFirstError(errs, false, nil)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	err = getFirstError(errs, true, nil)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestGetFirstError_MixedErrors_WithPartialResponse(t *testing.T) {
	unavailableErr := &httpserver.ErrorWithStatusCode{
		Err:        errors.New("connection refused"),
		StatusCode: 503,
	}
	configErr := errors.New("invalid configuration")

	// Test: config error takes precedence over unavailable errors
	errs := []error{nil, configErr, unavailableErr}
	qs := &logstorage.QueryStats{}

	err := getFirstError(errs, true, qs)
	if err == nil {
		t.Fatalf("expected error when backend returns configuration error")
	}

	isPartial := atomic.LoadUint32(&qs.IsPartial)
	if isPartial != 0 {
		t.Fatalf("expected IsPartial=0 (false) when returning configuration error, got %d", isPartial)
	}
}

func TestGetFirstError_EmptyErrors(t *testing.T) {
	// This should panic according to the implementation
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when errs is empty")
		}
	}()

	errs := []error{}
	qs := &logstorage.QueryStats{}
	_ = getFirstError(errs, false, qs)
}

func TestIsUnavailableBackendError(t *testing.T) {
	// Test unavailable backend error
	unavailableErr := &httpserver.ErrorWithStatusCode{
		Err:        errors.New("connection refused"),
		StatusCode: 503,
	}
	if !isUnavailableBackendError(unavailableErr) {
		t.Fatalf("expected isUnavailableBackendError to return true for ErrorWithStatusCode")
	}

	// Test wrapped unavailable backend error
	wrappedErr := fmt.Errorf("wrapped: %w", unavailableErr)
	if !isUnavailableBackendError(wrappedErr) {
		t.Fatalf("expected isUnavailableBackendError to return true for wrapped ErrorWithStatusCode")
	}

	// Test regular error
	regularErr := errors.New("some error")
	if isUnavailableBackendError(regularErr) {
		t.Fatalf("expected isUnavailableBackendError to return false for regular error")
	}

	// Test nil error
	if isUnavailableBackendError(nil) {
		t.Fatalf("expected isUnavailableBackendError to return false for nil error")
	}
}

func TestGetFirstError_PartialResponseScenarios(t *testing.T) {
	tests := []struct {
		name                 string
		errs                 []error
		allowPartialResponse bool
		expectError          bool
		expectedIsPartial    uint32
	}{
		{
			name:                 "3 nodes all success, no partial allowed",
			errs:                 []error{nil, nil, nil},
			allowPartialResponse: false,
			expectError:          false,
			expectedIsPartial:    0, // false
		},
		{
			name:                 "3 nodes all success, partial allowed",
			errs:                 []error{nil, nil, nil},
			allowPartialResponse: true,
			expectError:          false,
			expectedIsPartial:    0, // false
		},
		{
			name: "3 nodes, 2 success 1 unavailable, no partial allowed",
			errs: []error{
				nil,
				&httpserver.ErrorWithStatusCode{Err: errors.New("unavailable"), StatusCode: 503},
				nil,
			},
			allowPartialResponse: false,
			expectError:          true,
			expectedIsPartial:    0, // false (error returned)
		},
		{
			name: "3 nodes, 2 success 1 unavailable, partial allowed",
			errs: []error{
				nil,
				&httpserver.ErrorWithStatusCode{Err: errors.New("unavailable"), StatusCode: 503},
				nil,
			},
			allowPartialResponse: true,
			expectError:          false,
			expectedIsPartial:    1, // true (partial response)
		},
		{
			name: "3 nodes, 1 success 2 unavailable, partial allowed",
			errs: []error{
				&httpserver.ErrorWithStatusCode{Err: errors.New("unavailable"), StatusCode: 503},
				nil,
				&httpserver.ErrorWithStatusCode{Err: errors.New("unavailable"), StatusCode: 503},
			},
			allowPartialResponse: true,
			expectError:          false,
			expectedIsPartial:    1, // true (partial response)
		},
		{
			name: "3 nodes, all unavailable, partial allowed",
			errs: []error{
				&httpserver.ErrorWithStatusCode{Err: errors.New("unavailable"), StatusCode: 503},
				&httpserver.ErrorWithStatusCode{Err: errors.New("unavailable"), StatusCode: 503},
				&httpserver.ErrorWithStatusCode{Err: errors.New("unavailable"), StatusCode: 503},
			},
			allowPartialResponse: true,
			expectError:          true,
			expectedIsPartial:    1, // true (all unavailable)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qs := &logstorage.QueryStats{}
			err := getFirstError(tt.errs, tt.allowPartialResponse, qs)

			if tt.expectError && err == nil {
				t.Fatalf("expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Fatalf("expected no error but got: %v", err)
			}

			isPartial := atomic.LoadUint32(&qs.IsPartial)
			if isPartial != tt.expectedIsPartial {
				t.Fatalf("expected IsPartial=%d, got %d", tt.expectedIsPartial, isPartial)
			}
		})
	}
}
