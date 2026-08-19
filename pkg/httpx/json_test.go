package httpx

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeRejectsUnknownFields(t *testing.T) {
	type body struct {
		Email string `json:"email"`
	}
	req := httptest.NewRequest("POST", "/v1/auth/register",
		strings.NewReader(`{"email":"a@b.c","surprise":true}`))
	rec := httptest.NewRecorder()

	var dst body
	if ok := Decode(rec, req, &dst); ok {
		t.Fatal("Decode accepted a body with an unknown field")
	}
	if rec.Code != 400 {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	var env ErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != CodeValidationFailed {
		t.Errorf("code = %q, want validation_failed", env.Error.Code)
	}
}

func TestDecodeAcceptsKnownFields(t *testing.T) {
	type body struct {
		Email string `json:"email"`
	}
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"email":"a@b.c"}`))
	rec := httptest.NewRecorder()
	var dst body
	if ok := Decode(rec, req, &dst); !ok {
		t.Fatalf("Decode rejected a valid body: %s", rec.Body.String())
	}
	if dst.Email != "a@b.c" {
		t.Errorf("email = %q", dst.Email)
	}
}

func TestDecodeRejectsMalformedJSON(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"email":`))
	rec := httptest.NewRecorder()
	var dst struct{}
	if ok := Decode(rec, req, &dst); ok {
		t.Fatal("Decode accepted malformed JSON")
	}
	if rec.Code != 400 {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}
