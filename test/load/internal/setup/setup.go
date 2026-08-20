// Package setup provisions the account state a load run needs before a
// single message can be moderated.
//
// This is not incidental plumbing: moderation-service short-circuits at
// stage 2 when no policy resolves (§7.3 — the message is billed and
// counted clean, and no detector ever runs) and again at stage 3 when
// the creator has no credits (§5.8 — counted as
// fail_open_total{reason="no_credits"}). A run against an unprovisioned
// creator therefore measures a JSON decoder and two cache lookups, and
// reports beautiful numbers that mean nothing.
package setup

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Endpoints are the host-side base URLs of the services setup talks to.
type Endpoints struct {
	User    string `json:"user"`
	Credits string `json:"credits"`
	Policy  string `json:"policy"`
}

// DefaultEndpoints are the host port mappings of
// deploy/compose/docker-compose.yml.
func DefaultEndpoints() Endpoints {
	return Endpoints{
		User:    "http://localhost:8081",
		Credits: "http://localhost:8082",
		Policy:  "http://localhost:8083",
	}
}

// PolicyShape is the policy the run provisions. Each field maps onto
// one stage of the §7.3 cascade, so a scenario chooses which stages it
// wants live by choosing this.
type PolicyShape struct {
	RateLimitMessages int      `json:"rate_limit_messages"`
	RateLimitSeconds  int      `json:"rate_limit_seconds"`
	Spam              string   `json:"spam"` // none | identical | semantic
	RestrictedWords   []string `json:"restricted_words"`
	// RestrictedContentAction is the policy API's spelling: "auto" or
	// "review". Note the divergence from the wire contract — flagged.v1
	// carries action="auto_delete" (§4.2) while the policy document
	// carries restricted_content_action="auto" (§6.4). Both are as
	// implemented; the harness has to speak each in its own place.
	RestrictedContentAction string `json:"restricted_content_action"`
	// LLMRuleTitle/Description/Examples become the numbered rubric of
	// §7.9. Keeping them short keeps the prompt small, which matters
	// because the batcher's whole point is amortising them.
	LLMRuleTitle       string   `json:"llm_rule_title"`
	LLMRuleDescription string   `json:"llm_rule_description"`
	LLMRuleExamples    []string `json:"llm_rule_examples"`
}

// DefaultPolicy leaves every cheap stage live and the LLM stage
// reachable.
//
// The rate limit is deliberately loose (10 messages per 10 s per
// sender-content pair): with a realistic author population even the
// hottest content gives one sender well under a message a second, so
// only the generator's deliberate bursts trip stage 4 — a tight limit
// would flag the whole hot content at stage 4 and nothing would ever
// reach the sampler or the LLM, which is the part the run exists to
// stress.
func DefaultPolicy() PolicyShape {
	return PolicyShape{
		RateLimitMessages:       10,
		RateLimitSeconds:        10,
		Spam:                    "identical",
		RestrictedWords:         []string{"bannedword"},
		RestrictedContentAction: "auto",
		LLMRuleTitle:            "Ticket scalping",
		LLMRuleDescription:      "Offers to resell event tickets, or requests to buy them.",
		LLMRuleExamples:         []string{"selling 2 tickets for tonight DM me", "anyone got a spare ticket"},
	}
}

// Account is what a provisioned run works with.
type Account struct {
	CreatorID string `json:"creator_id"`
	Email     string `json:"email"`
	PolicyID  string `json:"policy_id"`
	Token     string `json:"-"`
}

// Client provisions accounts.
type Client struct {
	ep           Endpoints
	hc           *http.Client
	stripeSecret string
	topupCents   int64
}

// New builds a setup client. stripeSecret must match the stack's
// STRIPE_WEBHOOK_SECRET: credits are granted only on a
// signature-verified webhook (§5.7), never on the client confirmation.
func New(ep Endpoints, stripeSecret string, topupCents int64) *Client {
	if topupCents <= 0 {
		topupCents = 50_000_000
	}
	return &Client{
		ep:           ep,
		hc:           &http.Client{Timeout: 30 * time.Second},
		stripeSecret: stripeSecret,
		topupCents:   topupCents,
	}
}

// Provision registers a creator, verifies the email, buys credits
// through a signed Stripe webhook, and creates the policy.
func (c *Client) Provision(ctx context.Context, shape PolicyShape) (*Account, error) {
	acct := &Account{Email: fmt.Sprintf("load+%d@dabet.test", time.Now().UnixNano())}
	const password = "correct-horse-battery-staple"

	var reg struct {
		CreatorID         string `json:"creator_id"`
		VerificationToken string `json:"verification_token"`
	}
	if err := c.call(ctx, http.MethodPost, c.ep.User+"/v1/auth/register", "", map[string]string{
		"email": acct.Email, "fullname": "Load Generator", "password": password,
	}, http.StatusCreated, &reg, nil); err != nil {
		return nil, err
	}
	if reg.VerificationToken == "" {
		return nil, fmt.Errorf("register returned no verification_token: the stack must run " +
			"user-service with DEV_EXPOSE_VERIFICATION_TOKEN=1 (there is no mailer in v1)")
	}
	acct.CreatorID = reg.CreatorID

	if err := c.call(ctx, http.MethodPost, c.ep.User+"/v1/auth/verify", "",
		map[string]string{"token": reg.VerificationToken}, http.StatusNoContent, nil, nil); err != nil {
		return nil, err
	}

	var login struct {
		AccessToken string `json:"access_token"`
	}
	if err := c.call(ctx, http.MethodPost, c.ep.User+"/v1/auth/login", "",
		map[string]string{"email": acct.Email, "password": password}, http.StatusOK, &login, nil); err != nil {
		return nil, err
	}
	acct.Token = login.AccessToken

	if err := c.topup(ctx, acct); err != nil {
		return nil, err
	}
	if err := c.policy(ctx, acct, shape); err != nil {
		return nil, err
	}
	return acct, nil
}

// topup buys a large credit balance. A load run burns credits at the
// message rate (§7.10 bills per message processed), so the balance has
// to outlast the run: hitting zero mid-run turns every later message
// into fail_open_total{reason="no_credits"} and silently ends the
// measurement.
func (c *Client) topup(ctx context.Context, acct *Account) error {
	var intent struct {
		PaymentIntentID string `json:"payment_intent_id"`
	}
	if err := c.call(ctx, http.MethodPost, c.ep.Credits+"/v1/credits/topup", acct.Token,
		map[string]int64{"amount_cents": c.topupCents}, http.StatusOK, &intent,
		map[string]string{"Idempotency-Key": "load-topup-" + acct.CreatorID}); err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{
		"id":   "evt_load_" + acct.CreatorID,
		"type": "payment_intent.succeeded",
		"data": map[string]any{"object": map[string]any{
			"id":              intent.PaymentIntentID,
			"object":          "payment_intent",
			"amount_received": c.topupCents,
			"metadata":        map[string]string{"creator_id": acct.CreatorID},
		}},
	})
	if err != nil {
		return err
	}
	sig := signStripe(payload, c.stripeSecret, time.Now())
	return c.call(ctx, http.MethodPost, c.ep.Credits+"/v1/webhooks/stripe", "",
		json.RawMessage(payload), http.StatusNoContent, nil,
		map[string]string{"Stripe-Signature": sig})
}

// policy creates the creator-scoped policy. Creator scope, not content
// scope: adapter events carry no platform and the harness's content ids
// are minted per run, so creator scope is the only one that resolves
// for every generated message (§6.2, and the keying deviation noted in
// moderation-service's policy cache).
func (c *Client) policy(ctx context.Context, acct *Account, shape PolicyShape) error {
	doc := map[string]any{
		"scope":    "creator",
		"scope_id": acct.CreatorID,
		"spam":     shape.Spam,
	}
	if shape.RateLimitMessages > 0 && shape.RateLimitSeconds > 0 {
		doc["rate_limit_messages"] = shape.RateLimitMessages
		doc["rate_limit_seconds"] = shape.RateLimitSeconds
	}
	if len(shape.RestrictedWords) > 0 {
		doc["restricted_words"] = shape.RestrictedWords
	}
	if shape.LLMRuleTitle != "" {
		doc["restricted_content"] = []map[string]any{{
			"title":       shape.LLMRuleTitle,
			"description": shape.LLMRuleDescription,
			"examples":    shape.LLMRuleExamples,
		}}
		doc["restricted_content_action"] = shape.RestrictedContentAction
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := c.call(ctx, http.MethodPost, c.ep.Policy+"/v1/policies", acct.Token,
		doc, http.StatusCreated, &created, nil); err != nil {
		return err
	}
	acct.PolicyID = created.ID
	return nil
}

// WaitHealthy blocks until every provisioning endpoint answers.
func (c *Client) WaitHealthy(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, base := range []string{c.ep.User, c.ep.Credits, c.ep.Policy} {
		for {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/healthz", nil)
			if err != nil {
				return err
			}
			resp, err := c.hc.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("%s/healthz not ready within %s", base, timeout)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	return nil
}

func (c *Client) call(ctx context.Context, method, url, token string, body any, want int, out any, headers map[string]string) error {
	var rdr io.Reader
	if body != nil {
		enc, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(enc)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != want {
		return fmt.Errorf("%s %s: status %d, want %d: %s", method, url, resp.StatusCode, want,
			strings.TrimSpace(string(raw)))
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("%s %s: decode: %w", method, url, err)
		}
	}
	return nil
}

// signStripe produces the Stripe-Signature header credits-service
// verifies: t=<unix>,v1=hex(HMAC-SHA256(secret, "<t>.<payload>")).
func signStripe(payload []byte, secret string, at time.Time) string {
	ts := strconv.FormatInt(at.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(payload)
	return "t=" + ts + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}
