// Package stripe is a minimal Stripe REST client and webhook-signature
// verifier. It covers exactly what §5.7 needs — creating a PaymentIntent
// and verifying Stripe-Signature — without pulling the full stripe-go SDK.
//
// P4/security: the API key is sent in the Authorization header and is
// never logged; error values carry Stripe's error type/message only,
// never the key or full response bodies.
package stripe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"dabet/pkg/tracing"
)

// PaymentIntent is the subset of Stripe's payment_intent object the topup
// flow needs.
type PaymentIntent struct {
	ID           string `json:"id"`
	ClientSecret string `json:"client_secret"`
}

// PaymentIntents is the interface the topup handler depends on; the e2e
// stub and unit-test fakes implement it too.
type PaymentIntents interface {
	// CreatePaymentIntent creates an intent for amountCents with the
	// given metadata. idempotencyKey is sent as Stripe's Idempotency-Key
	// header, so retries return the same intent.
	CreatePaymentIntent(ctx context.Context, amountCents int64, metadata map[string]string, idempotencyKey string) (*PaymentIntent, error)
}

// Client talks to the Stripe REST API.
type Client struct {
	base  string
	key   string
	httpc *http.Client
}

// NewClient builds a client for the Stripe API at base (normally
// https://api.stripe.com, overridable via STRIPE_API_BASE for the e2e
// stub) authenticated with the secret key.
func NewClient(base, secretKey string) *Client {
	return &Client{
		base:  strings.TrimSuffix(base, "/"),
		key:   secretKey,
		httpc: tracing.HTTPClient(10 * time.Second),
	}
}

// apiError is Stripe's error envelope, reduced to what we surface.
type apiError struct {
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// CreatePaymentIntent implements PaymentIntents via
// POST /v1/payment_intents (form-encoded, per Stripe's API).
func (c *Client) CreatePaymentIntent(ctx context.Context, amountCents int64, metadata map[string]string, idempotencyKey string) (*PaymentIntent, error) {
	form := url.Values{}
	form.Set("amount", strconv.FormatInt(amountCents, 10))
	form.Set("currency", "usd")
	for k, v := range metadata {
		form.Set("metadata["+k+"]", v)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.base+"/v1/payment_intents", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("stripe: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("stripe: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var e apiError
		_ = json.Unmarshal(body, &e)
		if e.Error.Type != "" || e.Error.Message != "" {
			return nil, fmt.Errorf("stripe: status %d: %s: %s", resp.StatusCode, e.Error.Type, e.Error.Message)
		}
		return nil, fmt.Errorf("stripe: status %d", resp.StatusCode)
	}

	var pi PaymentIntent
	if err := json.Unmarshal(body, &pi); err != nil {
		return nil, fmt.Errorf("stripe: decode payment_intent: %w", err)
	}
	if pi.ID == "" || pi.ClientSecret == "" {
		return nil, fmt.Errorf("stripe: payment_intent missing id or client_secret")
	}
	return &pi, nil
}
