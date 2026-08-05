package internalselect

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newTestMultipartBody builds a minimal valid multipart/form-data request body
// (with zero or more simple fields) and returns the body reader together with
// the Content-Type header value to use for it.
//
// parseRequest() only cares that the Content-Type/body are well-formed enough
// for r.ParseMultipartForm() to succeed - it doesn't require any specific
// fields to be present.
func newTestMultipartBody() (*bytes.Buffer, string) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	// A single throwaway field is enough to produce a valid multipart body.
	_ = w.WriteField("dummy", "value")
	_ = w.Close()
	return &buf, w.FormDataContentType()
}

// fillConcurrencyLimitCh fills concurrencyLimitCh (initialized via Init())
// up to its full capacity, forcing any subsequent RequestHandler call to
// wait in the ctx.Done() branch of the concurrency limiter select statement.
// It registers a cleanup which drains whatever slots it filled.
func fillConcurrencyLimitCh(t *testing.T) {
	t.Helper()

	n := cap(concurrencyLimitCh)
	for i := 0; i < n; i++ {
		concurrencyLimitCh <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			select {
			case <-concurrencyLimitCh:
			default:
			}
		}
	})
}

// TestRequestHandlerCancelQueuedRequestOnClientDisconnect verifies that a
// request queued behind a full concurrency limiter gets its ctx cancelled
// once the client gives up and closes its connection, instead of sitting in
// the queue forever and then executing once a slot frees up.
func TestRequestHandlerCancelQueuedRequestOnClientDisconnect(t *testing.T) {
	Init()
	t.Cleanup(Stop)

	// Fill the concurrency limiter to capacity, so that any incoming request
	// is forced onto the ctx.Done() branch of the select statement.
	fillConcurrencyLimitCh(t)

	done := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		RequestHandler(r.Context(), w, r, "/internal/select/tenant_ids")
		close(done)
	}))
	defer ts.Close()

	body, contentType := newTestMultipartBody()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/internal/select/tenant_ids", body)
	if err != nil {
		t.Fatalf("cannot create request: %s", err)
	}
	req.Header.Set("Content-Type", contentType)

	client := &http.Client{
		Timeout: 150 * time.Millisecond,
	}

	startTime := time.Now()
	_, err = client.Do(req)
	if err == nil {
		t.Fatalf("expected the client request to time out, but it succeeded")
	}

	select {
	case <-done:
		// Good - the server-side handler noticed the client went away and returned.
	case <-time.After(2 * time.Second):
		t.Fatalf("RequestHandler did not return within 2s after the client disconnected at %s ago", time.Since(startTime))
	}
}

// TestRequestHandlerParseErrorWithoutFreeConcurrencySlot verifies that a request
// which fails to parse is rejected without waiting for a free slot in the
// concurrency limiter, and that it doesn't occupy such a slot.
//
// All the slots in the concurrency limiter are occupied on purpose - this is what
// makes the test verify the ordering between parseRequest() and the concurrency
// limiter. If the request were parsed only after obtaining a free slot, then
// RequestHandler() would wait in the queue instead of returning the parse error.
func TestRequestHandlerParseErrorWithoutFreeConcurrencySlot(t *testing.T) {
	Init()
	t.Cleanup(Stop)

	fillConcurrencyLimitCh(t)

	// Malformed multipart body: Content-Type declares multipart/form-data, but
	// the body itself is garbage, so r.ParseMultipartForm() must fail.
	req := httptest.NewRequest(http.MethodPost, "/internal/select/tenant_ids", bytes.NewBufferString("not a valid multipart body"))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=whatever")

	rr := httptest.NewRecorder()

	doneCh := make(chan struct{})
	go func() {
		RequestHandler(context.Background(), rr, req, "/internal/select/tenant_ids")
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// Good - the parse error has been returned without waiting for a free slot.
	case <-time.After(5 * time.Second):
		t.Fatalf("RequestHandler didn't return the parse error while all the slots in the concurrency limiter are occupied")
	}

	if rr.Code == http.StatusOK {
		t.Fatalf("expected a non-200 status code for a malformed request, got %d", rr.Code)
	}
	if rr.Body.Len() == 0 {
		t.Fatalf("expected an error message to be written to the response, got empty body")
	}

	if n := len(concurrencyLimitCh); n != cap(concurrencyLimitCh) {
		t.Fatalf("unexpected number of occupied slots in the concurrency limiter after a parse error; got %d; want %d", n, cap(concurrencyLimitCh))
	}
}

// TestRequestHandlerCancelledContext verifies that RequestHandler returns
// promptly, without writing an error response, when ctx is already cancelled
// and the concurrency limiter is full.
func TestRequestHandlerCancelledContext(t *testing.T) {
	Init()
	t.Cleanup(Stop)

	fillConcurrencyLimitCh(t)

	body, contentType := newTestMultipartBody()
	req := httptest.NewRequest(http.MethodPost, "/internal/select/tenant_ids", body)
	req.Header.Set("Content-Type", contentType)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rr := httptest.NewRecorder()

	resultCh := make(chan struct{})
	go func() {
		RequestHandler(ctx, rr, req, "/internal/select/tenant_ids")
		close(resultCh)
	}()

	select {
	case <-resultCh:
		// Good.
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("RequestHandler did not return within 500ms for an already-cancelled ctx")
	}

	if rr.Body.Len() != 0 {
		t.Fatalf("expected no response body to be written for an already-cancelled ctx, got %q", rr.Body.String())
	}
}
