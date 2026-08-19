package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"dabet/services/clusters-job/internal/job"

	"dabet/pkg/httpx"
)

var secret = []byte("test-secret")

type fakeEnqueuer struct {
	decisions []job.Decision
	full      bool
}

func (f *fakeEnqueuer) Enqueue(d job.Decision) bool {
	if f.full {
		return false
	}
	f.decisions = append(f.decisions, d)
	return true
}

func newMux(enq Enqueuer) *http.ServeMux {
	mux := http.NewServeMux()
	Register(mux, httpx.HMACVerifier(secret), enq, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return mux
}

func token(t *testing.T, sub string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   sub,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
	})
	s, err := tok.SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func post(t *testing.T, mux *http.ServeMux, auth, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/recluster", strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestReclusterAccepted(t *testing.T) {
	enq := &fakeEnqueuer{}
	mux := newMux(enq)
	// A window well older than 7 days is explicitly allowed (§8.6).
	body := `{"from": "2026-07-01T00:00:00Z", "to": "2026-07-08T00:00:00Z"}`
	w := post(t, mux, token(t, "cr-1"), body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", w.Code, w.Body.String())
	}
	var resp struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)
	if want := job.ReclusterJobID("cr-1", from, to); resp.JobID != want {
		t.Errorf("job_id = %q, want deterministic %q", resp.JobID, want)
	}
	if len(enq.decisions) != 1 {
		t.Fatalf("enqueued %d decisions", len(enq.decisions))
	}
	d := enq.decisions[0]
	if d.CreatorID != "cr-1" || d.Trigger != job.TriggerOnDemand || d.JobID != resp.JobID {
		t.Errorf("decision = %+v", d)
	}
	if !d.From.Equal(from) || !d.To.Equal(to) {
		t.Errorf("window = %v..%v", d.From, d.To)
	}

	// Retrying the same request converges on the same job id.
	w2 := post(t, mux, token(t, "cr-1"), body)
	var resp2 struct {
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal(w2.Body.Bytes(), &resp2)
	if resp2.JobID != resp.JobID {
		t.Errorf("retry produced a different job id: %q vs %q", resp2.JobID, resp.JobID)
	}
}

func TestReclusterOwnership(t *testing.T) {
	// The creator id comes only from the JWT: a body cannot name another
	// creator (unknown fields are rejected), and the queued decision
	// carries the token subject.
	enq := &fakeEnqueuer{}
	mux := newMux(enq)
	w := post(t, mux, token(t, "cr-2"),
		`{"from": "2026-07-01T00:00:00Z", "to": "2026-07-02T00:00:00Z", "creator_id": "cr-1"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("smuggled creator_id: status = %d, want 400", w.Code)
	}
	w = post(t, mux, token(t, "cr-2"), `{"from": "2026-07-01T00:00:00Z", "to": "2026-07-02T00:00:00Z"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d", w.Code)
	}
	if len(enq.decisions) != 1 || enq.decisions[0].CreatorID != "cr-2" {
		t.Errorf("decision creator = %+v, want the JWT subject cr-2", enq.decisions)
	}
}

func TestReclusterUnauthenticated(t *testing.T) {
	w := post(t, newMux(&fakeEnqueuer{}), "", `{"from": "2026-07-01T00:00:00Z", "to": "2026-07-02T00:00:00Z"}`)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestReclusterValidation(t *testing.T) {
	enq := &fakeEnqueuer{}
	mux := newMux(enq)
	cases := map[string]string{
		"bad json":      `{`,
		"bad from":      `{"from": "yesterday", "to": "2026-07-02T00:00:00Z"}`,
		"bad to":        `{"from": "2026-07-01T00:00:00Z", "to": "later"}`,
		"from after to": `{"from": "2026-07-02T00:00:00Z", "to": "2026-07-01T00:00:00Z"}`,
		"equal":         `{"from": "2026-07-01T00:00:00Z", "to": "2026-07-01T00:00:00Z"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			w := post(t, mux, token(t, "cr-1"), body)
			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400: %s", w.Code, w.Body.String())
			}
		})
	}
	if len(enq.decisions) != 0 {
		t.Errorf("invalid requests were enqueued: %+v", enq.decisions)
	}
}

func TestReclusterQueueFull(t *testing.T) {
	w := post(t, newMux(&fakeEnqueuer{full: true}), token(t, "cr-1"),
		`{"from": "2026-07-01T00:00:00Z", "to": "2026-07-02T00:00:00Z"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}
