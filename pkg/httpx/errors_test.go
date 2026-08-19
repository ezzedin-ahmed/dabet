package httpx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusForEveryCode(t *testing.T) {
	want := map[string]int{
		CodeValidationFailed: 400,
		CodeUnauthenticated:  401,
		CodeNotFound:         404,
		CodeConflict:         409,
		CodeStateConflict:    409,
		CodeUnprocessable:    422,
		CodeTooManyRequests:  429,
		CodeInternalError:    500,
		CodeUpstreamError:    502,
		CodeUnavailable:      503,
	}
	for code, status := range want {
		if got := StatusFor(code); got != status {
			t.Errorf("StatusFor(%q) = %d, want %d", code, got, status)
		}
	}
	if got := StatusFor("no_such_code"); got != 500 {
		t.Errorf("StatusFor(unknown) = %d, want 500", got)
	}
}

func TestWriteErrorEnvelope(t *testing.T) {
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, CodeValidationFailed, "restricted_words exceeds maximum of 500 entries",
			map[string]any{"field": "restricted_words", "limit": 500})
	}))

	req := httptest.NewRequest("POST", "/v1/policies", nil)
	req.Header.Set(HeaderRequestID, "01J8XQTEST")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != 400 {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("content type = %q", ct)
	}
	var env ErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Error.Code != CodeValidationFailed {
		t.Errorf("code = %q", env.Error.Code)
	}
	if env.Error.Message != "restricted_words exceeds maximum of 500 entries" {
		t.Errorf("message = %q", env.Error.Message)
	}
	if env.Error.RequestID != "01J8XQTEST" {
		t.Errorf("request_id = %q", env.Error.RequestID)
	}
	if env.Error.Details["field"] != "restricted_words" {
		t.Errorf("details = %v", env.Error.Details)
	}

	// The envelope must be exactly {"error":{...}} at the top level.
	var top map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &top); err != nil {
		t.Fatal(err)
	}
	if len(top) != 1 {
		t.Errorf("top-level keys = %d, want only \"error\"", len(top))
	}
	if _, ok := top["error"]; !ok {
		t.Error("missing top-level \"error\" key")
	}
}

func TestWriteErrorOmitsEmptyDetails(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	WriteError(rec, req, CodeNotFound, "not found", nil)

	var body map[string]map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["error"]["details"]; ok {
		t.Error("details should be omitted when nil")
	}
}
