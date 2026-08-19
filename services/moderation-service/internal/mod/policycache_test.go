package mod

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"dabet/pkg/policyapi"
)

// fakePolicyServer is a scriptable GetPolicy backend served over bufconn.
type fakePolicyServer struct {
	policyapi.UnimplementedPolicyServiceServer
	mu    sync.Mutex
	calls int
	fail  bool
	resp  map[string]*policyapi.GetPolicyResponse // by creator_id
}

func (s *fakePolicyServer) GetPolicy(_ context.Context, req *policyapi.GetPolicyRequest) (*policyapi.GetPolicyResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.fail {
		return nil, status.Error(codes.Unavailable, "policy-service down")
	}
	if req.GetPlatform() != "" {
		return nil, status.Error(codes.InvalidArgument, "moderation must pass empty platform")
	}
	if r, ok := s.resp[req.GetCreatorId()]; ok {
		return r, nil
	}
	return &policyapi.GetPolicyResponse{Found: false}, nil
}

func (s *fakePolicyServer) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newPolicyClient(t *testing.T, srv *fakePolicyServer) policyapi.PolicyServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	policyapi.RegisterPolicyServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

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

func foundResp(p *policyapi.ResolvedPolicy) *policyapi.GetPolicyResponse {
	return &policyapi.GetPolicyResponse{Found: true, Policy: p}
}

func TestPolicyCachePositiveCaching(t *testing.T) {
	srv := &fakePolicyServer{resp: map[string]*policyapi.GetPolicyResponse{
		"cr1": foundResp(testPolicy(func(p *policyapi.ResolvedPolicy) {
			p.RestrictedWords = []string{"badword"}
		})),
	}}
	clock := newFakeClock(t0)
	c := NewPolicyCache(newPolicyClient(t, srv), 60*time.Second, 100, time.Second, clock.Now)

	got, err := c.Get(context.Background(), "cr1", "ct1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Policy.GetPolicyId() != "pol_7a13" {
		t.Fatalf("policy id = %q", got.Policy.GetPolicyId())
	}
	if got.Matcher == nil || !got.Matcher.Match("badword") {
		t.Fatal("matcher must be compiled and cached alongside the policy")
	}
	for i := 0; i < 5; i++ {
		if _, err := c.Get(context.Background(), "cr1", "ct1"); err != nil {
			t.Fatal(err)
		}
	}
	if n := srv.callCount(); n != 1 {
		t.Fatalf("server called %d times, want 1 (cached)", n)
	}
}

func TestPolicyCacheNegativeCaching(t *testing.T) {
	srv := &fakePolicyServer{resp: map[string]*policyapi.GetPolicyResponse{}}
	clock := newFakeClock(t0)
	c := NewPolicyCache(newPolicyClient(t, srv), 60*time.Second, 100, time.Second, clock.Now)

	for i := 0; i < 5; i++ {
		got, err := c.Get(context.Background(), "nobody", "ct1")
		if err != nil {
			t.Fatal(err)
		}
		if got.Policy != nil {
			t.Fatal("negative result must have nil policy")
		}
	}
	if n := srv.callCount(); n != 1 {
		t.Fatalf("server called %d times, want 1: negative results are cached too (§6.7)", n)
	}
}

func TestPolicyCacheTTLExpiry(t *testing.T) {
	srv := &fakePolicyServer{resp: map[string]*policyapi.GetPolicyResponse{
		"cr1": foundResp(testPolicy()),
	}}
	clock := newFakeClock(t0)
	c := NewPolicyCache(newPolicyClient(t, srv), 60*time.Second, 100, time.Second, clock.Now)

	mustGet := func() {
		t.Helper()
		if _, err := c.Get(context.Background(), "cr1", "ct1"); err != nil {
			t.Fatal(err)
		}
	}
	mustGet()
	clock.Advance(59 * time.Second)
	mustGet()
	if n := srv.callCount(); n != 1 {
		t.Fatalf("still fresh: server called %d, want 1", n)
	}
	clock.Advance(2 * time.Second) // past TTL
	mustGet()
	if n := srv.callCount(); n != 2 {
		t.Fatalf("expired: server called %d, want 2", n)
	}
}

func TestPolicyCacheFailOpenOnRPCError(t *testing.T) {
	srv := &fakePolicyServer{fail: true}
	clock := newFakeClock(t0)
	c := NewPolicyCache(newPolicyClient(t, srv), 60*time.Second, 100, time.Second, clock.Now)

	if _, err := c.Get(context.Background(), "cr1", "ct1"); err == nil {
		t.Fatal("RPC failure with cold cache must return an error (pipeline fails open)")
	}
	// Errors are not cached: recovery is immediate.
	srv.mu.Lock()
	srv.fail = false
	srv.resp = map[string]*policyapi.GetPolicyResponse{"cr1": foundResp(testPolicy())}
	srv.mu.Unlock()
	got, err := c.Get(context.Background(), "cr1", "ct1")
	if err != nil || got.Policy == nil {
		t.Fatalf("after recovery Get = (%v, %v)", got.Policy, err)
	}
}

func TestPolicyCacheLRUEviction(t *testing.T) {
	srv := &fakePolicyServer{resp: map[string]*policyapi.GetPolicyResponse{
		"a": foundResp(testPolicy()),
		"b": foundResp(testPolicy()),
		"c": foundResp(testPolicy()),
	}}
	clock := newFakeClock(t0)
	c := NewPolicyCache(newPolicyClient(t, srv), time.Hour, 2, time.Second, clock.Now)
	ctx := context.Background()

	c.Get(ctx, "a", "ct")
	c.Get(ctx, "b", "ct")
	c.Get(ctx, "a", "ct") // refresh a: b is now LRU
	c.Get(ctx, "c", "ct") // evicts b
	base := srv.callCount()
	c.Get(ctx, "a", "ct") // still cached
	if n := srv.callCount(); n != base {
		t.Fatalf("a should be cached, calls %d -> %d", base, n)
	}
	c.Get(ctx, "b", "ct") // evicted: refetch
	if n := srv.callCount(); n != base+1 {
		t.Fatalf("b should have been evicted, calls = %d want %d", n, base+1)
	}
}
