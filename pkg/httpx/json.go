package httpx

import (
	"encoding/json"
	"net/http"
)

// DecodeJSON decodes the request body into dst, rejecting unknown fields
// (docs §4.1: client bugs surface early). A non-nil error should be
// rendered as validation_failed.
func DecodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// Decode is DecodeJSON plus error rendering: on failure it writes the
// validation_failed envelope and returns false.
func Decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := DecodeJSON(r, dst); err != nil {
		WriteError(w, r, CodeValidationFailed, "invalid request body: "+err.Error(), nil)
		return false
	}
	return true
}

// WriteJSON renders v with the standard content type.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
