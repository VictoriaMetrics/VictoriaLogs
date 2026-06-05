package internalselect

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequestHandlerSizeLimit(t *testing.T) {
	f := func(body []byte, readLimit string, sizeError bool) {
		t.Helper()
		testResponseWriter := httptest.NewRecorder()
		testRequest := httptest.NewRequest(http.MethodPost, "/internal/select/query", bytes.NewReader(body))
		testRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		if err := maxReadBodySize.Set(readLimit); err != nil {
			t.Fatalf("maxReadBodySize.Set(%s) got error: %s", readLimit, err)
		}

		requestHandler(t.Context(), testResponseWriter, testRequest, time.Now())
		// in any case it should be a status error. because the request is missing necessary form params.
		if testResponseWriter.Code != http.StatusBadRequest {
			t.Fatalf("unexpected response code; got %d; want %d", testResponseWriter.Code, http.StatusBadRequest)
		}

		// verify if it's a size limit error
		if sizeError {
			errGot := testResponseWriter.Body.String()
			errWant := "http: request body too large"
			if !strings.Contains(errGot, errWant) {
				t.Fatalf("unexpected response content; got %q; should contain %q", errGot, errWant)
			}
		}
	}

	// exceed limit
	f([]byte("0000000000"), "9", true)

	// fits limit
	f([]byte("0000000000"), "10", false)
}
