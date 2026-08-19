package grpcapi

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"dabet/pkg/obs"
	"dabet/pkg/policyapi"

	"dabet/services/policy-service/internal/cache/cachetest"
	"dabet/services/policy-service/internal/metrics"
	"dabet/services/policy-service/internal/policy"
	"dabet/services/policy-service/internal/resolver"
	"dabet/services/policy-service/internal/store"
	"dabet/services/policy-service/internal/store/memstore"
)

const creatorA = "9d4ecafe-0000-0000-0000-00000000000a"

func newClient(t *testing.T, repo *memstore.Mem, fake *cachetest.Fake) policyapi.PolicyServiceClient {
	t.Helper()
	reg := prometheus.NewRegistry()
	res := resolver.New(repo, fake, metrics.New(reg), obs.NewMetrics(reg), resolver.DefaultTTL)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	policyapi.RegisterPolicyServiceServer(srv, New(res, log))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return policyapi.NewPolicyServiceClient(conn)
}

func TestGetPolicyFound(t *testing.T) {
	repo := memstore.New()
	now := store.Now()
	msgs, secs := 5, 10
	p := &policy.Policy{
		ID:        policy.NewID(),
		CreatorID: creatorA,
		Scope:     policy.ScopeCreator,
		ScopeID:   creatorA,
		Document: policy.Document{
			RateLimitMessages: &msgs,
			RateLimitSeconds:  &secs,
			Spam:              policy.SpamSemantic,
			RestrictedWords:   []string{"foo", "bar"},
			RestrictedContent: []policy.RestrictedContentEntry{{
				Title:       "Ticket scalping",
				Description: "Offers to resell event tickets.",
				Examples:    []string{"selling 2 tickets"},
			}},
			RestrictedContentAction: policy.RCActionReview,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := repo.Create(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	client := newClient(t, repo, cachetest.New())

	resp, err := client.GetPolicy(context.Background(), &policyapi.GetPolicyRequest{
		CreatorId: creatorA, Platform: "twitch", ContentId: "ct_1",
	})
	if err != nil {
		t.Fatalf("GetPolicy: %v", err)
	}
	if !resp.GetFound() {
		t.Fatal("found = false, want true")
	}
	got := resp.GetPolicy()
	if got.GetPolicyId() != p.ID {
		t.Errorf("policy_id = %q, want %q", got.GetPolicyId(), p.ID)
	}
	if got.GetResolvedAt() == nil {
		t.Error("resolved_at missing")
	}
	if got.GetRateLimitMessages() != 5 || got.GetRateLimitSeconds() != 10 {
		t.Errorf("rate limit = %d/%d, want 5/10", got.GetRateLimitMessages(), got.GetRateLimitSeconds())
	}
	if got.GetSpam() != policyapi.SpamMode_SPAM_MODE_SEMANTIC {
		t.Errorf("spam = %v", got.GetSpam())
	}
	if got.GetRestrictedContentAction() != policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_REVIEW {
		t.Errorf("action = %v", got.GetRestrictedContentAction())
	}
	if len(got.GetRestrictedWords()) != 2 {
		t.Errorf("restricted_words = %v", got.GetRestrictedWords())
	}
	rc := got.GetRestrictedContent()
	if len(rc) != 1 || rc[0].GetTitle() != "Ticket scalping" || len(rc[0].GetExamples()) != 1 {
		t.Errorf("restricted_content = %v", rc)
	}
}

func TestGetPolicyNoRateLimitFieldsAbsent(t *testing.T) {
	repo := memstore.New()
	now := store.Now()
	p := &policy.Policy{
		ID: policy.NewID(), CreatorID: creatorA,
		Scope: policy.ScopeCreator, ScopeID: creatorA,
		Document:  policy.Document{Spam: policy.SpamNone, RestrictedContentAction: policy.RCActionAuto},
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repo.Create(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	client := newClient(t, repo, cachetest.New())

	resp, err := client.GetPolicy(context.Background(), &policyapi.GetPolicyRequest{CreatorId: creatorA})
	if err != nil {
		t.Fatal(err)
	}
	got := resp.GetPolicy()
	if got.RateLimitMessages != nil || got.RateLimitSeconds != nil {
		t.Error("absent rate limit must stay absent, not zero")
	}
	if got.GetSpam() != policyapi.SpamMode_SPAM_MODE_NONE {
		t.Errorf("spam = %v, want NONE", got.GetSpam())
	}
}

// A negative result is a first-class answer, not an error (docs §6.7).
func TestGetPolicyNotFound(t *testing.T) {
	client := newClient(t, memstore.New(), cachetest.New())

	resp, err := client.GetPolicy(context.Background(), &policyapi.GetPolicyRequest{
		CreatorId: creatorA, Platform: "youtube", ContentId: "ct_none",
	})
	if err != nil {
		t.Fatalf("GetPolicy: %v", err)
	}
	if resp.GetFound() {
		t.Error("found = true, want false")
	}
	if resp.GetPolicy() != nil {
		t.Error("policy set on a negative result")
	}
}

func TestGetPolicyRequiresCreatorID(t *testing.T) {
	client := newClient(t, memstore.New(), cachetest.New())

	_, err := client.GetPolicy(context.Background(), &policyapi.GetPolicyRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

// The gRPC path is served through the same cache: with Memcached down it
// still answers from Postgres (docs §6.8).
func TestGetPolicyMemcachedDown(t *testing.T) {
	repo := memstore.New()
	now := store.Now()
	p := &policy.Policy{
		ID: policy.NewID(), CreatorID: creatorA,
		Scope: policy.ScopeCreator, ScopeID: creatorA,
		Document:  policy.Document{Spam: policy.SpamNone, RestrictedContentAction: policy.RCActionAuto},
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repo.Create(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	fake := cachetest.New()
	fake.SetDown(true)
	client := newClient(t, repo, fake)

	resp, err := client.GetPolicy(context.Background(), &policyapi.GetPolicyRequest{CreatorId: creatorA})
	if err != nil {
		t.Fatalf("GetPolicy with memcached down: %v", err)
	}
	if !resp.GetFound() || resp.GetPolicy().GetPolicyId() != p.ID {
		t.Errorf("resp = %v, want the creator policy", resp)
	}
}
