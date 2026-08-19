package httpx

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// Pagination limits per docs §4.1.
const (
	DefaultLimit = 50
	MaxLimit     = 200
)

// ParseLimit reads the "limit" query parameter: default 50, values above
// 200 are clamped to 200. Non-numeric or non-positive values error and
// should be rendered as validation_failed.
func ParseLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return DefaultLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("limit must be a positive integer")
	}
	if n > MaxLimit {
		return MaxLimit, nil
	}
	return n, nil
}

// EncodeCursor serialises a cursor payload to an opaque base64 blob.
// Clients must never decode it.
func EncodeCursor(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// DecodeCursor deserialises an opaque cursor into dst.
func DecodeCursor(cursor string, dst any) error {
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return fmt.Errorf("invalid cursor")
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("invalid cursor")
	}
	return nil
}
