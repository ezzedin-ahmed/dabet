package httpx

import "net/http"

// HeaderIdempotencyKey is the idempotency header of docs §4.1.
const HeaderIdempotencyKey = "Idempotency-Key"

// IdempotencyKey returns the Idempotency-Key header value, or "".
// Servers store (creator_id, key) -> response for 24h and replay on repeat.
func IdempotencyKey(r *http.Request) string {
	return r.Header.Get(HeaderIdempotencyKey)
}
