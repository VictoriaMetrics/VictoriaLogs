package internalselect

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
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

		requestHandler(context.TODO(), testResponseWriter, testRequest, time.Now())
		// in any case it should be a status error. because the request is missing necessary form params.
		if testResponseWriter.Code != http.StatusBadRequest {
			t.Fatalf("unexpected response code; got %d; want %d", testResponseWriter.Code, http.StatusBadRequest)
		}

		// verify if it's a size limit error
		if sizeError != (testResponseWriter.Body.String() == "cannot parse form: http: request body too large\n") {
			t.Fatalf(`unexpected response body; got %q; should be size limit error: %t\n"`, testResponseWriter.Body.String(), sizeError)
		}
	}

	// fits limit
	f([]byte("0000000000"), "9", true)

	// exceed limit
	f([]byte("0000000000"), "10", false)
}
