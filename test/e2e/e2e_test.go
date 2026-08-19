//go:build e2e

// The test is a single ordered scenario rather than independent cases:
// every step depends on the creator, connection and policy the previous
// steps established, and Kafka makes the whole thing one causal chain.
// Each assertion polls to a deadline instead of sleeping — verdicts cross
// three services and two topics, and the usage ledger only moves on a
// minute boundary.
package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Timeouts. Generous, because the pipeline is Kafka-bound and the usage
// path waits for a wall-clock minute boundary (§7.10).
const (
	healthTimeout   = 90 * time.Second
	verdictTimeout  = 90 * time.Second
	insightsTimeout = 120 * time.Second
	usageTimeout    = 180 * time.Second
)

// Fixtures. restrictedWord must be a token the normaliser keeps whole
// (§7.4 matches whole tokens, not substrings).
const (
	restrictedWord = "bannedword"
	// mockllm flags any listed message containing FLAGME as a rule-1
	// violation, which is how the LLM stage is exercised deterministically.
	flagmeText  = "FLAGME anyone selling tickets for tonight"
	cleanText   = "great stream today, thanks for having us"
	dupText     = "same message sent twice in a row"
	rateLimit   = 5
	rateWindowS = 60
)

// scenario carries the state each step hands to the next.
type scenario struct {
	email     string
	password  string
	creatorID string
	token     string

	channel  string // native mock channel id
	policyID string

	// nativeIDs maps a human label to the native message id the adapter
	// handed back on injection, which is how deletions are correlated.
	nativeIDs map[string]string
}

func TestDabetEndToEnd(t *testing.T) {
	s := &scenario{
		email:     fmt.Sprintf("e2e+%d@dabet.test", time.Now().UnixNano()),
		password:  "correct-horse-battery-staple",
		channel:   fmt.Sprintf("e2e-channel-%d", time.Now().UnixNano()),
		nativeIDs: map[string]string{},
	}

	waitHealthy(t, healthTimeout)

	// The steps are strictly ordered; a failure in one makes the rest
	// meaningless, so each aborts the run.
	steps := []struct {
		name string
		fn   func(*testing.T, *scenario)
	}{
		{"a_register_verify_login", stepAuth},
		// Credits must exist before any message is injected: a creator at
		// zero balance is passed through unmoderated and counted as
		// fail_open_total{reason="no_credits"} (§5.8), which would both
		// defeat the moderation assertions and break step i.
		{"g1_topup_via_signed_stripe_webhook", stepTopup},
		{"b_connect_mock_platform_over_oauth", stepConnect},
		{"c_create_policy", stepPolicy},
		{"d_inject_messages", stepInject},
		{"e_auto_deletions_reached_the_adapter", stepDeletions},
		{"f_review_queue_upholds_and_advances", stepReview},
		{"g2_usage_debited_the_credit_ledger", stepUsage},
		{"h_embeddings_landed_in_minio_as_parquet", stepInsights},
		{"i_moderation_metrics_are_healthy", stepMetrics},
	}
	for _, step := range steps {
		if !t.Run(step.name, func(t *testing.T) { step.fn(t, s) }) {
			t.Fatalf("step %s failed; the remaining steps depend on it", step.name)
		}
	}
}

// ---------------------------------------------------------------------
// (a) register -> verify -> login -> JWT works on /v1/me
// ---------------------------------------------------------------------

func stepAuth(t *testing.T, s *scenario) {
	var reg struct {
		CreatorID         string `json:"creator_id"`
		VerificationToken string `json:"verification_token"`
	}
	mustStatus(t, do(t, client, http.MethodPost, userURL+"/v1/auth/register", "", map[string]string{
		"email":    s.email,
		"fullname": "E2E Creator",
		"password": s.password,
	}), http.StatusCreated, "register").json(t, &reg)

	if reg.CreatorID == "" {
		t.Fatal("register returned no creator_id")
	}
	s.creatorID = reg.CreatorID
	if reg.VerificationToken == "" {
		t.Fatal("register returned no verification_token; the stack must run user-service " +
			"with DEV_EXPOSE_VERIFICATION_TOKEN=1 (there is no mailer in v1)")
	}

	// Unverified, connecting a platform must be refused (§5.4/A3). Proving
	// this before verifying keeps the affordance honest: the token really
	// is what unlocks connections.
	pre := do(t, client, http.MethodPost, userURL+"/v1/auth/login", "", map[string]string{
		"email": s.email, "password": s.password,
	})
	mustStatus(t, pre, http.StatusOK, "login before verification")
	var preTok struct {
		AccessToken string `json:"access_token"`
	}
	pre.json(t, &preTok)
	unverified := do(t, client, http.MethodPost, userURL+"/v1/connections/mock", preTok.AccessToken, nil)
	if unverified.status != http.StatusUnprocessableEntity {
		t.Fatalf("connect while unverified: status %d, want 422\nbody: %s",
			unverified.status, truncate(unverified.body))
	}

	mustStatus(t, do(t, client, http.MethodPost, userURL+"/v1/auth/verify", "",
		map[string]string{"token": reg.VerificationToken}), http.StatusNoContent, "verify")

	var login struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
	}
	mustStatus(t, do(t, client, http.MethodPost, userURL+"/v1/auth/login", "", map[string]string{
		"email": s.email, "password": s.password,
	}), http.StatusOK, "login").json(t, &login)
	if login.AccessToken == "" || login.RefreshToken == "" || login.ExpiresIn <= 0 {
		t.Fatalf("login response incomplete: %+v", login)
	}
	s.token = login.AccessToken

	var me struct {
		ID            string `json:"id"`
		Email         string `json:"email"`
		Fullname      string `json:"fullname"`
		EmailVerified bool   `json:"email_verified"`
	}
	mustStatus(t, do(t, client, http.MethodGet, userURL+"/v1/me", s.token, nil),
		http.StatusOK, "GET /v1/me").json(t, &me)
	if me.ID != s.creatorID {
		t.Fatalf("/v1/me id = %q, want %q", me.ID, s.creatorID)
	}
	if !strings.EqualFold(me.Email, s.email) {
		t.Fatalf("/v1/me email = %q, want %q", me.Email, s.email)
	}
	if !me.EmailVerified {
		t.Fatal("/v1/me reports email_verified=false after a successful verify")
	}

	// A JWT is genuinely required, not merely accepted.
	if r := do(t, client, http.MethodGet, userURL+"/v1/me", "", nil); r.status != http.StatusUnauthorized {
		t.Fatalf("GET /v1/me without a token: status %d, want 401", r.status)
	}
}

// ---------------------------------------------------------------------
// (g, first half) top up through a signed Stripe webhook
// ---------------------------------------------------------------------

func stepTopup(t *testing.T, s *scenario) {
	// The topup endpoint must reach Stripe (the stub) and hand back a
	// client secret — the half of §5.7 that does not move money.
	var topup struct {
		ClientSecret    string `json:"client_secret"`
		PaymentIntentID string `json:"payment_intent_id"`
	}
	mustStatus(t, do(t, client, http.MethodPost, creditsURL+"/v1/credits/topup", s.token,
		map[string]int64{"amount_cents": 50_000},
		[2]string{"Idempotency-Key", "e2e-topup-" + s.creatorID},
	), http.StatusOK, "topup").json(t, &topup)
	if topup.PaymentIntentID == "" || topup.ClientSecret == "" {
		t.Fatalf("topup response incomplete: %+v", topup)
	}

	// Credits are granted only on the signature-verified webhook (§5.7),
	// never on the client-side confirmation. Deliver it as Stripe would.
	payload, err := json.Marshal(map[string]any{
		"id":   "evt_e2e_" + s.creatorID,
		"type": "payment_intent.succeeded",
		"data": map[string]any{"object": map[string]any{
			"id":              topup.PaymentIntentID,
			"object":          "payment_intent",
			"amount_received": 50_000,
			"metadata":        map[string]string{"creator_id": s.creatorID},
		}},
	})
	if err != nil {
		t.Fatalf("encode webhook payload: %v", err)
	}
	postWebhook := func(sig string) response {
		return do(t, client, http.MethodPost, creditsURL+"/v1/webhooks/stripe", "",
			json.RawMessage(payload), [2]string{"Stripe-Signature", sig})
	}

	// An unsigned webhook is the authentication boundary: it must fail.
	if r := postWebhook("t=1,v1=deadbeef"); r.status != http.StatusBadRequest {
		t.Fatalf("webhook with a bad signature: status %d, want 400\nbody: %s", r.status, truncate(r.body))
	}

	sig := signStripe(payload, stripeWebhookSecret, time.Now())
	mustStatus(t, postWebhook(sig), http.StatusNoContent, "signed stripe webhook")

	var bal struct {
		Balance int64 `json:"balance"`
	}
	mustStatus(t, do(t, client, http.MethodGet, creditsURL+"/v1/credits", s.token, nil),
		http.StatusOK, "GET /v1/credits").json(t, &bal)
	if bal.Balance <= 0 {
		t.Fatalf("balance after topup = %d, want a positive balance", bal.Balance)
	}

	// The payment_intent_id is the idempotency key, so a redelivery is a
	// no-op rather than a second grant (§5.7).
	mustStatus(t, postWebhook(sig), http.StatusNoContent, "redelivered stripe webhook")
	var bal2 struct {
		Balance int64 `json:"balance"`
	}
	mustStatus(t, do(t, client, http.MethodGet, creditsURL+"/v1/credits", s.token, nil),
		http.StatusOK, "GET /v1/credits after redelivery").json(t, &bal2)
	if bal2.Balance != bal.Balance {
		t.Fatalf("redelivered webhook changed the balance: %d -> %d", bal.Balance, bal2.Balance)
	}
}

// ---------------------------------------------------------------------
// (b) connect the mock platform through the full OAuth round trip
// ---------------------------------------------------------------------

func stepConnect(t *testing.T, s *scenario) {
	var start struct {
		AuthorizeURL string `json:"authorize_url"`
		State        string `json:"state"`
	}
	mustStatus(t, do(t, client, http.MethodPost, userURL+"/v1/connections/mock", s.token, nil),
		http.StatusOK, "POST /v1/connections/mock").json(t, &start)
	if start.State == "" || start.AuthorizeURL == "" {
		t.Fatalf("connect response incomplete: %+v", start)
	}
	// PKCE must be requested (§5.5): without a challenge the mock provider
	// would happily skip verification and the test would prove nothing.
	authURL, err := url.Parse(start.AuthorizeURL)
	if err != nil {
		t.Fatalf("parse authorize_url: %v", err)
	}
	if authURL.Query().Get("code_challenge") == "" ||
		authURL.Query().Get("code_challenge_method") != "S256" {
		t.Fatalf("authorize_url carries no S256 PKCE challenge: %s", start.AuthorizeURL)
	}

	// Act as the browser: consent at the provider, then follow the
	// redirect back into user-service's callback.
	auth := do(t, noRedirect, http.MethodGet, start.AuthorizeURL, "", nil)
	if auth.status != http.StatusFound {
		t.Fatalf("authorize: status %d, want 302\nbody: %s", auth.status, truncate(auth.body))
	}
	callback := auth.location(t, start.AuthorizeURL)
	if got := callback.Query().Get("state"); got != start.State {
		t.Fatalf("callback state = %q, want %q", got, start.State)
	}

	cb := do(t, noRedirect, http.MethodGet, callback.String(), "", nil)
	if cb.status != http.StatusFound {
		t.Fatalf("callback: status %d, want 302\nbody: %s", cb.status, truncate(cb.body))
	}

	// Replaying the same state must fail — the state is single-use and is
	// the CSRF defence (§5.5).
	replay := do(t, noRedirect, http.MethodGet, callback.String(), "", nil)
	if replay.status != http.StatusBadRequest {
		t.Fatalf("replayed callback: status %d, want 400", replay.status)
	}

	var list struct {
		Items []map[string]any `json:"items"`
	}
	listResp := mustStatus(t, do(t, client, http.MethodGet, userURL+"/v1/connections", s.token, nil),
		http.StatusOK, "GET /v1/connections")
	listResp.json(t, &list)
	if len(list.Items) != 1 {
		t.Fatalf("connections = %d, want exactly 1\nbody: %s", len(list.Items), truncate(listResp.body))
	}
	conn := list.Items[0]
	if conn["platform"] != "mock" {
		t.Fatalf("connection platform = %v, want mock", conn["platform"])
	}
	if conn["status"] != "active" {
		t.Fatalf("connection status = %v, want active", conn["status"])
	}
	if name, _ := conn["display_name"].(string); name == "" {
		t.Fatalf("connection display_name is empty; userinfo was not applied: %v", conn)
	}
	// §5.5: Connection never exposes tokens. Check both the parsed object
	// and the raw bytes, so a token cannot hide in an unexpected field.
	for _, forbidden := range []string{"access_token", "refresh_token", "code_verifier", "client_secret"} {
		if _, ok := conn[forbidden]; ok {
			t.Fatalf("connection object leaks %q", forbidden)
		}
		if strings.Contains(string(listResp.body), forbidden) {
			t.Fatalf("GET /v1/connections body mentions %q", forbidden)
		}
	}
}

// ---------------------------------------------------------------------
// (c) create a policy exercising words, LLM rules and a rate limit
// ---------------------------------------------------------------------

func stepPolicy(t *testing.T, s *scenario) {
	rl, rw := rateLimit, rateWindowS
	doc := map[string]any{
		// Resolution is driven by GetPolicy(creator_id, platform,
		// content_id) and adapter events carry no platform (§1.4), so a
		// creator-scoped policy is the one that can actually be resolved
		// for an opaque content id the test never sees.
		"scope":               "creator",
		"scope_id":            s.creatorID,
		"rate_limit_messages": rl,
		"rate_limit_seconds":  rw,
		"spam":                "identical",
		"restricted_words":    []string{restrictedWord},
		"restricted_content": []map[string]any{{
			"title":       "Ticket scalping",
			"description": "Offers to resell event tickets, or requests to buy them.",
			"examples":    []string{"selling 2 tickets for tonight DM me", "anyone got a spare ticket"},
		}},
		// review, so step (f) has a queue to work.
		"restricted_content_action": "review",
	}
	var created map[string]any
	mustStatus(t, do(t, client, http.MethodPost, policyURL+"/v1/policies", s.token, doc),
		http.StatusCreated, "create policy").json(t, &created)
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("policy response has no id: %v", created)
	}
	s.policyID = id

	// Exactly one policy per (scope, scope_id) — a second is 409 (§6.1).
	if r := do(t, client, http.MethodPost, policyURL+"/v1/policies", s.token, doc); r.status != http.StatusConflict {
		t.Fatalf("duplicate policy: status %d, want 409\nbody: %s", r.status, truncate(r.body))
	}

	// Read it back so the test is asserting on stored state, not on the
	// echo of its own request.
	var fetched map[string]any
	mustStatus(t, do(t, client, http.MethodGet, policyURL+"/v1/policies/"+id, s.token, nil),
		http.StatusOK, "get policy").json(t, &fetched)
	words, _ := fetched["restricted_words"].([]any)
	if len(words) != 1 || words[0] != restrictedWord {
		t.Fatalf("restricted_words = %v, want [%s]", fetched["restricted_words"], restrictedWord)
	}
	if fetched["restricted_content_action"] != "review" {
		t.Fatalf("restricted_content_action = %v, want review", fetched["restricted_content_action"])
	}
	if rc, _ := fetched["restricted_content"].([]any); len(rc) != 1 {
		t.Fatalf("restricted_content = %v, want one rule", fetched["restricted_content"])
	}
}

// ---------------------------------------------------------------------
// (d) inject chat through provider-adapter's mock platform
// ---------------------------------------------------------------------

// inject posts one message and records the native id under label.
func (s *scenario) inject(t *testing.T, label, author, text string) string {
	t.Helper()
	var out struct {
		ConnectionID    string `json:"connection_id"`
		NativeMessageID string `json:"native_message_id"`
	}
	mustStatus(t, do(t, client, http.MethodPost, adapterURL+"/mock/messages", "", map[string]string{
		"creator_id": s.creatorID,
		"channel":    s.channel,
		"author":     author,
		"text":       text,
	}), http.StatusAccepted, "inject "+label).json(t, &out)
	if out.NativeMessageID == "" {
		t.Fatalf("inject %s returned no native_message_id", label)
	}
	s.nativeIDs[label] = out.NativeMessageID
	return out.NativeMessageID
}

func stepInject(t *testing.T, s *scenario) {
	// The policy write must be visible to moderation-service before any
	// message for this content arrives: a miss is cached as "no policy"
	// for the local TTL (§6.8), which would silently skip every detector.
	// The content id is opaque and unknown here, so the only sound
	// sequencing is to inject after the policy exists — which it does —
	// and to keep every message on one freshly-minted channel.

	// (i) clean — must survive every stage and produce no deletion.
	s.inject(t, "clean", "viewer-clean", cleanText)

	// (ii) restricted word — auto_delete, no review.
	s.inject(t, "word", "viewer-word", "hey everyone "+restrictedWord+" right now")

	// (iii) rate limit — capacity is rateLimit, so the tail trips it.
	// Each text differs so the duplicate detector (an earlier stage)
	// cannot claim them first.
	for i := 1; i <= rateLimit+3; i++ {
		label := fmt.Sprintf("rate-%d", i)
		s.inject(t, label, "viewer-rate", fmt.Sprintf("rate probe number %d for the stream", i))
	}

	// (iv) identical duplicates — the policy sets spam=identical.
	s.inject(t, "dup-1", "viewer-dup", dupText)
	s.inject(t, "dup-2", "viewer-dup", dupText)

	// (v) FLAGME — the mock LLM calls this a rule-1 violation, and the
	// policy routes restricted_content to review rather than deletion.
	s.inject(t, "flagme", "viewer-llm", flagmeText)
}

// ---------------------------------------------------------------------
// (e) deletions.v1 effects reached the adapter
// ---------------------------------------------------------------------

type deletionRecord struct {
	ConnectionID    string `json:"connection_id"`
	ContentID       string `json:"content_id"`
	MessageID       string `json:"message_id"`
	NativeMessageID string `json:"native_message_id"`
}

func fetchDeletions(t *testing.T) []deletionRecord {
	t.Helper()
	var out struct {
		Deletions []deletionRecord `json:"deletions"`
	}
	mustStatus(t, do(t, client, http.MethodGet, adapterURL+"/mock/deletions", "", nil),
		http.StatusOK, "GET /mock/deletions").json(t, &out)
	return out.Deletions
}

// deletedNatives indexes the current deletion set by native message id.
func deletedNatives(t *testing.T) map[string]bool {
	t.Helper()
	set := map[string]bool{}
	for _, d := range fetchDeletions(t) {
		if d.NativeMessageID != "" {
			set[d.NativeMessageID] = true
		}
	}
	return set
}

func stepDeletions(t *testing.T, s *scenario) {
	// Every auto_delete verdict must land: the restricted word, the
	// rate-limited tail, and the second identical message.
	want := []string{"word", "dup-2"}
	for i := rateLimit + 1; i <= rateLimit+3; i++ {
		want = append(want, fmt.Sprintf("rate-%d", i))
	}

	poll(t, verdictTimeout, "auto_delete verdicts to reach the mock platform", func() error {
		deleted := deletedNatives(t)
		var missing []string
		for _, label := range want {
			if !deleted[s.nativeIDs[label]] {
				missing = append(missing, label)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("not deleted yet: %s", strings.Join(missing, ", "))
		}
		return nil
	})

	deleted := deletedNatives(t)

	// The clean message must NOT have been deleted. Checked after the
	// wanted set has fully arrived, so this is a real negative and not a
	// race with a verdict still in flight.
	if deleted[s.nativeIDs["clean"]] {
		t.Fatal("the clean message was deleted")
	}
	// Neither must the messages that were inside the rate-limit budget.
	for i := 1; i <= rateLimit; i++ {
		label := fmt.Sprintf("rate-%d", i)
		if deleted[s.nativeIDs[label]] {
			t.Fatalf("%s was deleted, but it is inside the rate-limit budget of %d", label, rateLimit)
		}
	}
	// The first of the identical pair is the original, not the duplicate.
	if deleted[s.nativeIDs["dup-1"]] {
		t.Fatal("the first of the identical pair was deleted; only the repeat is a duplicate")
	}
	// restricted_content_action=review means FLAGME is queued, not deleted.
	if deleted[s.nativeIDs["flagme"]] {
		t.Fatal("the FLAGME message was auto-deleted, but the policy routes restricted_content to review")
	}

	// deletions.v1 carries no text (§4.2) and the adapter records only
	// identifiers — assert nothing leaked into the mock platform's log.
	for _, d := range fetchDeletions(t) {
		if d.ContentID == "" || d.MessageID == "" {
			t.Fatalf("deletion record missing identifiers: %+v", d)
		}
	}
	raw := do(t, client, http.MethodGet, adapterURL+"/mock/deletions", "", nil)
	for _, text := range []string{cleanText, dupText, flagmeText, restrictedWord} {
		if strings.Contains(string(raw.body), text) {
			t.Fatalf("/mock/deletions echoes message text (%q); deletions.v1 carries none", text)
		}
	}
}

// ---------------------------------------------------------------------
// (f) review queue: read, uphold, cursor advances, replay is a no-op
// ---------------------------------------------------------------------

type pendingReview struct {
	MessageID string `json:"message_id"`
	ContentID string `json:"content_id"`
	Text      string `json:"text"`
	Detector  string `json:"detector"`
	PolicyID  string `json:"policy_id"`
}

func listReviews(t *testing.T, token string) ([]pendingReview, string) {
	t.Helper()
	var out struct {
		Items      []pendingReview `json:"items"`
		NextCursor string          `json:"next_cursor"`
	}
	mustStatus(t, do(t, client, http.MethodGet, reviewURL+"/v1/reviews?limit=50", token, nil),
		http.StatusOK, "GET /v1/reviews").json(t, &out)
	return out.Items, out.NextCursor
}

func stepReview(t *testing.T, s *scenario) {
	var target pendingReview
	poll(t, verdictTimeout, "the FLAGME message to appear in the review queue", func() error {
		items, _ := listReviews(t, s.token)
		for _, it := range items {
			if it.Text == flagmeText {
				target = it
				return nil
			}
		}
		return fmt.Errorf("queue has %d item(s), none matching", len(items))
	})

	if target.Detector != "restricted_content" {
		t.Fatalf("review detector = %q, want restricted_content", target.Detector)
	}
	if target.PolicyID != s.policyID {
		t.Fatalf("review policy_id = %q, want the policy this test created (%q)", target.PolicyID, s.policyID)
	}
	if target.MessageID == "" || target.ContentID == "" {
		t.Fatalf("review item missing identifiers: %+v", target)
	}

	// Reading must not advance the cursor: the read is idempotent (§7.6).
	again, _ := listReviews(t, s.token)
	if !containsMessage(again, target.MessageID) {
		t.Fatal("a second read lost the pending review; the cursor advanced on read")
	}

	// Uphold it. The upheld decision converges on deletions.v1 exactly
	// like an auto_delete verdict (§7.1).
	var decided struct {
		Applied int      `json:"applied"`
		Deleted int      `json:"deleted"`
		Ignored []string `json:"ignored"`
	}
	mustStatus(t, do(t, client, http.MethodPost, reviewURL+"/v1/reviews", s.token, map[string]any{
		"decisions": []map[string]any{{"message_id": target.MessageID, "flagged": true}},
	}, [2]string{"Idempotency-Key", "e2e-review-" + s.creatorID}),
		http.StatusOK, "POST /v1/reviews").json(t, &decided)
	if decided.Deleted != 1 {
		t.Fatalf("uphold produced %d deletion(s), want 1 (applied=%d, ignored=%v)",
			decided.Deleted, decided.Applied, decided.Ignored)
	}

	poll(t, verdictTimeout, "the upheld review to be deleted on the platform", func() error {
		for _, d := range fetchDeletions(t) {
			if d.MessageID == target.MessageID {
				return nil
			}
		}
		return fmt.Errorf("no deletion for the upheld message yet")
	})

	// The cursor advanced past the window, so the item is gone from the
	// queue and replaying the same batch is a no-op (§7.6, §7.8).
	poll(t, 30*time.Second, "the review queue to advance past the upheld item", func() error {
		items, _ := listReviews(t, s.token)
		if containsMessage(items, target.MessageID) {
			return fmt.Errorf("the upheld item is still pending")
		}
		return nil
	})

	replay := mustStatus(t, do(t, client, http.MethodPost, reviewURL+"/v1/reviews", s.token, map[string]any{
		"decisions": []map[string]any{{"message_id": target.MessageID, "flagged": true}},
	}), http.StatusOK, "replayed POST /v1/reviews")
	var replayed struct {
		Applied int      `json:"applied"`
		Deleted int      `json:"deleted"`
		Ignored []string `json:"ignored"`
	}
	replay.json(t, &replayed)
	if replayed.Deleted != 0 {
		t.Fatalf("replaying a decided batch deleted %d message(s); it must be a no-op", replayed.Deleted)
	}
}

func containsMessage(items []pendingReview, messageID string) bool {
	for _, it := range items {
		if it.MessageID == messageID {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------
// (g, second half) usage flowed into the credit ledger
// ---------------------------------------------------------------------

type creditEntry struct {
	ID       int64          `json:"id"`
	Delta    int64          `json:"delta"`
	Reason   string         `json:"reason"`
	Metadata map[string]any `json:"metadata"`
}

func stepUsage(t *testing.T, s *scenario) {
	// moderation-service aggregates per creator per minute and flushes on
	// the boundary (§7.10), so this is the slowest assertion in the run.
	poll(t, usageTimeout, "a messages_processed debit to reach the ledger", func() error {
		var out struct {
			Items []creditEntry `json:"items"`
		}
		mustStatus(t, do(t, client, http.MethodGet, creditsURL+"/v1/credits/entries?limit=100", s.token, nil),
			http.StatusOK, "GET /v1/credits/entries").json(t, &out)

		var sawTopup bool
		for _, e := range out.Items {
			switch e.Reason {
			case "topup":
				sawTopup = true
			case "messages_processed":
				if e.Delta >= 0 {
					return fmt.Errorf("messages_processed entry has delta %d, want a debit", e.Delta)
				}
				if q, ok := e.Metadata["quantity"].(float64); !ok || q <= 0 {
					return fmt.Errorf("messages_processed entry carries no positive quantity: %v", e.Metadata)
				}
				return nil
			}
		}
		if !sawTopup {
			return fmt.Errorf("the ledger has no topup entry either (%d entries)", len(out.Items))
		}
		return fmt.Errorf("no messages_processed entry among %d ledger entries", len(out.Items))
	})
}

// ---------------------------------------------------------------------
// (i) moderation-service metrics
// ---------------------------------------------------------------------

func stepMetrics(t *testing.T, s *scenario) {
	samples, err := scrapeMetrics()
	if err != nil {
		t.Fatalf("scrape moderation-service metrics: %v", err)
	}

	// §4.5: fail_open_total is the count of work that went undone because
	// something was broken, and "it must be zero in steady state". Every
	// component of the default profile was up for this run, so every one
	// of them must report zero — including moderation's
	// reason="no_credits", which is why the topup runs before any
	// injection. Checking all of them, not just moderation-service, is
	// what makes this a health signal rather than a spot check.
	for name, base := range metricsPorts {
		svcSamples, err := scrape(base)
		if err != nil {
			t.Fatalf("scrape %s metrics: %v", name, err)
		}
		if n := metricSum(svcSamples, "fail_open_total", nil); n != 0 {
			var detail []string
			for _, smp := range svcSamples {
				if smp.name == "fail_open_total" && smp.value > 0 {
					detail = append(detail, fmt.Sprintf("%v=%g", smp.labels, smp.value))
				}
			}
			t.Errorf("%s: fail_open_total = %g with every dependency up; offending series: %s",
				name, n, strings.Join(detail, ", "))
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// Detector hits were actually counted, per detector (§7.11).
	for _, detector := range []string{"rate_limit", "duplicate", "restricted_word", "restricted_content"} {
		if n := metricSum(samples, "moderation_detector_hits_total", map[string]string{"detector": detector}); n <= 0 {
			t.Fatalf("moderation_detector_hits_total{detector=%q} = %g, want a positive count", detector, n)
		}
	}
	// restricted_content must have been actioned as review, not deletion.
	if n := metricSum(samples, "moderation_detector_hits_total",
		map[string]string{"detector": "restricted_content", "action": "review"}); n <= 0 {
		t.Fatal("no restricted_content hit was actioned as review")
	}

	// Messages were both classified clean and flagged.
	for _, outcome := range []string{"clean", "flagged"} {
		if n := metricSum(samples, "moderation_messages_total", map[string]string{"outcome": outcome}); n <= 0 {
			t.Fatalf("moderation_messages_total{outcome=%q} = %g, want a positive count", outcome, n)
		}
	}
	// The LLM stage really ran and really succeeded.
	if n := metricSum(samples, "llm_requests_total", map[string]string{"outcome": "ok"}); n <= 0 {
		t.Fatalf("llm_requests_total{outcome=ok} = %g, want a positive count", n)
	}

	// §4.5 cardinality rule: no metric may be labelled with a message,
	// author or content id. Assert on the raw exposition.
	resp := do(t, client, http.MethodGet, modMetricsURL+"/metrics", "", nil)
	for _, forbidden := range []string{"message_id=", "author_id=", "content_id="} {
		if strings.Contains(string(resp.body), forbidden) {
			t.Fatalf("moderation-service /metrics carries a %s label, breaking the §4.5 cardinality rule", forbidden)
		}
	}
}
