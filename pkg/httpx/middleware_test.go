package httpx

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestIDGeneratedWhenAbsent(t *testing.T) {
	var seen string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFrom(r.Context())
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if seen == "" {
		t.Fatal("no request id in context")
	}
	if len(seen) != 26 {
		t.Errorf("generated id %q is not a ULID", seen)
	}
	if rec.Header().Get(HeaderRequestID) != seen {
		t.Error("request id not echoed on response")
	}
}

func TestRequestIDEchoed(t *testing.T) {
	var seen string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFrom(r.Context())
	}))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(HeaderRequestID, "client-supplied")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if seen != "client-supplied" {
		t.Errorf("context id = %q", seen)
	}
	if rec.Header().Get(HeaderRequestID) != "client-supplied" {
		t.Error("incoming request id not echoed")
	}
}
