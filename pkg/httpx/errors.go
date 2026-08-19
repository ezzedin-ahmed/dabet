// Package httpx implements the shared HTTP conventions of docs §4.1:
// the error envelope, strict JSON decoding, request IDs, cursor
// pagination, JWT auth, and idempotency-key extraction.
package httpx

import (
	"encoding/json"
	"net/http"
)

// Error codes from the docs §4.1 table.
const (
	CodeValidationFailed = "validation_failed" // 400
	CodeUnauthenticated  = "unauthenticated"   // 401
	CodeNotFound         = "not_found"         // 404
	CodeConflict         = "conflict"          // 409
	CodeStateConflict    = "state_conflict"    // 409
	CodeUnprocessable    = "unprocessable"     // 422
	CodeTooManyRequests  = "too_many_requests" // 429, reserved in v1
	CodeInternalError    = "internal_error"    // 500
	CodeUpstreamError    = "upstream_error"    // 502
	CodeUnavailable      = "unavailable"       // 503
)

var statusByCode = map[string]int{
	CodeValidationFailed: http.StatusBadRequest,
	CodeUnauthenticated:  http.StatusUnauthorized,
	CodeNotFound:         http.StatusNotFound,
	CodeConflict:         http.StatusConflict,
	CodeStateConflict:    http.StatusConflict,
	CodeUnprocessable:    http.StatusUnprocessableEntity,
	CodeTooManyRequests:  http.StatusTooManyRequests,
	CodeInternalError:    http.StatusInternalServerError,
	CodeUpstreamError:    http.StatusBadGateway,
	CodeUnavailable:      http.StatusServiceUnavailable,
}

// StatusFor maps an error code to its HTTP status; unknown codes are 500.
func StatusFor(code string) int {
	if s, ok := statusByCode[code]; ok {
		return s
	}
	return http.StatusInternalServerError
}

// ErrorBody is the inner object of the error envelope.
type ErrorBody struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Details   map[string]any `json:"details,omitempty"`
	RequestID string         `json:"request_id"`
}

// ErrorEnvelope is the single error shape: {"error":{...}}.
type ErrorEnvelope struct {
	Error ErrorBody `json:"error"`
}

// WriteError renders the envelope with the status implied by code and the
// request ID taken from the request context. Per P4, message must never
// echo chat message content.
func WriteError(w http.ResponseWriter, r *http.Request, code, message string, details map[string]any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(StatusFor(code))
	_ = json.NewEncoder(w).Encode(ErrorEnvelope{Error: ErrorBody{
		Code:      code,
		Message:   message,
		Details:   details,
		RequestID: RequestIDFrom(r.Context()),
	}})
}
