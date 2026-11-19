package netselect

import (
	"errors"
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
)

func TestGetFirstError(t *testing.T) {
	errUnavailable := &errUnavailableBackend{
		err: errors.New("cannot connect to storage node"),
	}
	errResponse := errors.New("unexpected response status code")

	f := func(errs []error, allowPartialResponse bool, errExpected error) {
		t.Helper()

		err := getFirstError(errs, allowPartialResponse)
		if errExpected == nil {
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			return
		}

		if !errors.Is(err, errExpected) {
			t.Fatalf("unexpected error\ngot\n%v\nwant the error wrapping\n%v", err, errExpected)
		}
		esc, ok := errors.AsType[*httpserver.ErrorWithStatusCode](err)
		if !ok || esc.StatusCode != http.StatusBadGateway {
			t.Fatalf("expecting %d status code for the error %v", http.StatusBadGateway, err)
		}
	}

	// partial responses are disallowed
	f([]error{nil, nil}, false, nil)
	f([]error{nil, errUnavailable}, false, errUnavailable)

	// partial responses are allowed
	f([]error{nil, nil}, true, nil)
	f([]error{nil, errUnavailable}, true, nil)
	f([]error{errUnavailable, errUnavailable}, true, errUnavailable)
	f([]error{errUnavailable, errResponse}, true, errResponse)
	f([]error{errResponse, nil}, true, errResponse)

	// handleError() returns nil for the query canceled because of an error at another backend.
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1935
	f([]error{nil, errResponse}, true, errResponse)
}
