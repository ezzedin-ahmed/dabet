//go:build e2e

package e2e

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
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// env returns the variable's value or def, so a run can be pointed at a
// stack published on different ports.
func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// Service base URLs. Defaults are the host port mappings in
// deploy/compose/docker-compose.yml.
var (
	userURL       = env("E2E_USER_URL", "http://localhost:8081")
	creditsURL    = env("E2E_CREDITS_URL", "http://localhost:8082")
	policyURL     = env("E2E_POLICY_URL", "http://localhost:8083")
	adapterURL    = env("E2E_ADAPTER_URL", "http://localhost:8084")
	reviewURL     = env("E2E_REVIEW_URL", "http://localhost:8086")
	insightsURL   = env("E2E_INSIGHTS_URL", "http://localhost:8087")
	modMetricsURL = env("E2E_MODERATION_METRICS_URL", "http://localhost:9085")

	// Metrics ports of every service the default compose profile runs.
	// clustering-service and clusters-job are deliberately absent: they
	// are opt-in (`make up-full`) and the smoke test does not require
	// them, so asserting on them would be asserting on a service that may
	// not be up.
	metricsPorts = map[string]string{
		"user-service":       env("E2E_USER_METRICS_URL", "http://localhost:9081"),
		"credits-service":    env("E2E_CREDITS_METRICS_URL", "http://localhost:9082"),
		"policy-service":     env("E2E_POLICY_METRICS_URL", "http://localhost:9083"),
		"provider-adapter":   env("E2E_ADAPTER_METRICS_URL", "http://localhost:9084"),
		"moderation-service": modMetricsURL,
		"review-service":     env("E2E_REVIEW_METRICS_URL", "http://localhost:9086"),
		"insights-service":   env("E2E_INSIGHTS_METRICS_URL", "http://localhost:9087"),
	}

	// Stand-ins the tests drive directly rather than only through a service:
	// mockembed's ledger answers "how many times was this text embedded"
	// (§8.4) and mockoauth's admin surface breaks a token on demand (§5.6).
	embedURL = env("E2E_EMBED_URL", "http://localhost:8091")
	oauthURL = env("E2E_OAUTH_URL", "http://localhost:9099")

	// The clustering profile (`make up-full`, §8.5/§8.6). Only the
	// e2e_full-tagged tests touch these.
	clusteringMetricsURL = env("E2E_CLUSTERING_METRICS_URL", "http://localhost:9088")
	clustersJobURL       = env("E2E_CLUSTERS_JOB_URL", "http://localhost:8090")
	clustersJobMetricsCL = env("E2E_CLUSTERS_JOB_METRICS_URL", "http://localhost:9089")
	clickhouseURL        = env("E2E_CLICKHOUSE_URL", "http://localhost:8123")
	clickhouseUser       = env("E2E_CLICKHOUSE_USER", "dabet")
	clickhousePassword   = env("E2E_CLICKHOUSE_PASSWORD", "dabet")
	clickhouseDB         = env("E2E_CLICKHOUSE_DB", "dabet")

	s3Endpoint  = env("E2E_S3_ENDPOINT", "localhost:9000")
	s3AccessKey = env("E2E_S3_ACCESS_KEY", "minioadmin")
	s3SecretKey = env("E2E_S3_SECRET_KEY", "minioadmin")
	s3Bucket    = env("E2E_S3_BUCKET", "embeddings")

	stripeWebhookSecret = env("E2E_STRIPE_WEBHOOK_SECRET", "whsec_local_dev")
)

// noRedirect is used wherever the test must inspect a 302 rather than
// follow it — the OAuth authorize hop and the connection callback.
var noRedirect = &http.Client{
	Timeout: 20 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

var client = &http.Client{Timeout: 20 * time.Second}

// response is one decoded HTTP exchange. Headers are kept because the
// OAuth hops are asserted on their Location, not their body.
type response struct {
	status  int
	body    []byte
	headers http.Header
}

// location resolves the response's Location header against the request
// URL, so a relative redirect works the same as an absolute one.
func (r response) location(t *testing.T, requestURL string) *url.URL {
	t.Helper()
	raw := r.headers.Get("Location")
	if raw == "" {
		t.Fatalf("response has no Location header (status %d)", r.status)
	}
	base, err := url.Parse(requestURL)
	if err != nil {
		t.Fatalf("parse request url %q: %v", requestURL, err)
	}
	loc, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse Location %q: %v", raw, err)
	}
	return base.ResolveReference(loc)
}

// json decodes the body into v, failing the test with the raw body on
// error — a JSON contract drift should say what actually arrived.
func (r response) json(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode response: %v\nbody: %s", err, truncate(r.body))
	}
}

func truncate(b []byte) string {
	const max = 2000
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}

// do performs one request. body is JSON-encoded when non-nil; token, when
// set, becomes the bearer credential.
func do(t *testing.T, hc *http.Client, method, url, token string, body any, headers ...[2]string) response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		enc, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode request: %v", err)
		}
		rdr = bytes.NewReader(enc)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("build request %s %s: %v", method, url, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for _, h := range headers {
		req.Header.Set(h[0], h[1])
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	return readResponse(t, resp)
}

// readResponse drains an *http.Response into the decoded form the assertions
// use. Split out of do so a caller that has to build its own request — an
// OAuth form post, say — still gets the same failure reporting.
func readResponse(t *testing.T, resp *http.Response) response {
	t.Helper()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		t.Fatalf("%s %s: read body: %v", resp.Request.Method, resp.Request.URL, err)
	}
	return response{status: resp.StatusCode, body: raw, headers: resp.Header}
}

// mustStatus fails unless the response carries want, printing the error
// envelope so a §4.1 `code` is visible in the failure.
func mustStatus(t *testing.T, r response, want int, what string) response {
	t.Helper()
	if r.status != want {
		t.Fatalf("%s: status %d, want %d\nbody: %s", what, r.status, want, truncate(r.body))
	}
	return r
}

// poll runs fn until it returns nil or the deadline passes. It is used for
// every cross-service assertion: Kafka hops, the insights roll timer and
// the usage minute boundary are all eventually-consistent, and a fixed
// sleep would be both slower and flakier than a deadline.
func poll(t *testing.T, timeout time.Duration, what string, fn func() error) {
	t.Helper()
	const interval = 500 * time.Millisecond
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		lastErr = fn()
		if lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s: %v", timeout, what, lastErr)
		}
		time.Sleep(interval)
	}
}

// waitHealthy blocks until every service answers /healthz, so a failure
// later in the run is a real bug and not a stack that had not finished
// starting.
func waitHealthy(t *testing.T, timeout time.Duration) {
	t.Helper()
	targets := map[string]string{
		"user-service":     userURL,
		"credits-service":  creditsURL,
		"policy-service":   policyURL,
		"provider-adapter": adapterURL,
		"review-service":   reviewURL,
		"insights-service": insightsURL,
	}
	for name, base := range targets {
		poll(t, timeout, name+" /healthz", func() error {
			resp, err := client.Get(base + "/healthz")
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("status %d", resp.StatusCode)
			}
			return nil
		})
	}
	// moderation-service exposes no /v1 API; its metrics port is the
	// liveness signal the test cares about anyway.
	poll(t, timeout, "moderation-service /metrics", func() error {
		_, err := scrapeMetrics()
		return err
	})
}

// ---------------------------------------------------------------------
// Prometheus scraping
// ---------------------------------------------------------------------

// sample is one Prometheus time series: a metric name, its labels, value.
type sample struct {
	name   string
	labels map[string]string
	value  float64
}

// scrapeMetrics parses moderation-service's /metrics into samples.
func scrapeMetrics() ([]sample, error) { return scrape(modMetricsURL) }

// scrape parses one service's /metrics into samples. Only counters and
// gauges are needed, so the parser deliberately ignores histogram bucket
// lines beyond treating them as ordinary series.
func scrape(base string) ([]sample, error) {
	resp, err := client.Get(base + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics status %d", resp.StatusCode)
	}
	var out []sample
	for line := range strings.Lines(string(raw)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		series, valStr, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
		if err != nil {
			continue
		}
		s := sample{value: value, labels: map[string]string{}}
		if name, rest, hasLabels := strings.Cut(series, "{"); hasLabels {
			s.name = name
			for _, pair := range splitLabels(strings.TrimSuffix(rest, "}")) {
				k, v, ok := strings.Cut(pair, "=")
				if !ok {
					continue
				}
				s.labels[k] = strings.Trim(v, `"`)
			}
		} else {
			s.name = series
		}
		out = append(out, s)
	}
	return out, nil
}

// splitLabels splits a Prometheus label set on commas that are not inside
// a quoted value.
func splitLabels(s string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			inQuote = !inQuote
			cur.WriteByte(c)
		case c == ',' && !inQuote:
			out = append(out, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, strings.TrimSpace(cur.String()))
	}
	return out
}

// metricSum totals every series of name whose labels match the (possibly
// partial) selector.
func metricSum(samples []sample, name string, selector map[string]string) float64 {
	total := 0.0
	for _, s := range samples {
		if s.name != name {
			continue
		}
		match := true
		for k, v := range selector {
			if s.labels[k] != v {
				match = false
				break
			}
		}
		if match {
			total += s.value
		}
	}
	return total
}

// metricDelta is metricSum over after minus metricSum over before. Every
// service in the stack is long-lived and shared between tests, so a counter's
// absolute value says nothing — only its movement across one action does.
func metricDelta(before, after []sample, name string, selector map[string]string) float64 {
	return metricSum(after, name, selector) - metricSum(before, name, selector)
}

// mustScrape scrapes one service's metrics or fails the test.
func mustScrape(t *testing.T, name, base string) []sample {
	t.Helper()
	s, err := scrape(base)
	if err != nil {
		t.Fatalf("scrape %s metrics: %v", name, err)
	}
	return s
}

// ---------------------------------------------------------------------
// mockembed's ledger (§8.4 — "a message is embedded at most once")
// ---------------------------------------------------------------------

// embedCount asks mockembed how many times it was handed exactly this text.
func embedCount(t *testing.T, text string) int {
	t.Helper()
	var out struct {
		Count int `json:"count"`
	}
	mustStatus(t, do(t, client, http.MethodGet,
		embedURL+"/admin/embeds?text="+url.QueryEscape(text), "", nil),
		http.StatusOK, "GET /admin/embeds").json(t, &out)
	return out.Count
}

// ---------------------------------------------------------------------
// Stripe webhook signing
// ---------------------------------------------------------------------

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

// testContext bounds anything that takes a context (the MinIO client).
func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}
