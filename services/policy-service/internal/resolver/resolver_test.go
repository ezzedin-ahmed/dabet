package resolver

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/pkg/obs"

	"dabet/services/policy-service/internal/cache/cachetest"
	"dabet/services/policy-service/internal/metrics"
	"dabet/services/policy-service/internal/policy"
	"dabet/services/policy-service/internal/store"
	"dabet/services/policy-service/internal/store/memstore"
)

const (
	creatorA = "9d4ecafe-0000-0000-0000-00000000000a"
	creatorB = "9d4ecafe-0000-0000-0000-00000000000b"
)

type fixture struct {
	repo *memstore.Mem
	c    *cachetest.Fake
	res  *Resolver
	std  *obs.Metrics
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	reg := prometheus.NewRegistry()
	repo := memstore.New()
	c := cachetest.New()
	std := obs.NewMetrics(reg)
	return &fixture{repo: repo, c: c, std: std,
		res: New(repo, c, metrics.New(reg), std, DefaultTTL)}
}

func (f *fixture) mustCreate(t *testing.T, creatorID string, scope policy.Scope, scopeID string) *policy.Policy {
	t.Helper()
	now := store.Now()
	p := &policy.Policy{ID: policy.NewID(), CreatorID: creatorID, Scope: scope, ScopeID: scopeID, CreatedAt: now, UpdatedAt: now}
	p.Spam = policy.SpamNone
	p.RestrictedContentAction = policy.RCActionAuto
	p.RestrictedWords = []string{}
	p.RestrictedContent = []policy.RestrictedContentEntry{}
	if err := f.repo.Create(context.Background(), p); err != nil {
		t.Fatalf("create %s/%s: %v", scope, scopeID, err)
	}
	return p
}

func TestResolutionPrecedence(t *testing.T) {
	cases := []struct {
		name       string
		scopes     []policy.Scope // which scopes have a policy
		wantResult string
	}{
		{"content beats platform and creator", []policy.Scope{policy.ScopeContent, policy.ScopePlatform, policy.ScopeCreator}, ResultContent},
		{"platform beats creator", []policy.Scope{policy.ScopePlatform, policy.ScopeCreator}, ResultPlatform},
		{"creator when nothing narrower", []policy.Scope{policy.ScopeCreator}, ResultCreator},
		{"absent everywhere means none", nil, ResultNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			created := map[policy.Scope]*policy.Policy{}
			for _, s := range tc.scopes {
				scopeID := map[policy.Scope]string{
					policy.ScopeContent:  "ct_9f2a",
					policy.ScopePlatform: creatorA + ":twitch",
					policy.ScopeCreator:  creatorA,
				}[s]
				created[s] = f.mustCreate(t, creatorA, s, scopeID)
			}
			p, result, err := f.res.Resolve(context.Background(), creatorA, "twitch", "ct_9f2a")
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if result != tc.wantResult {
				t.Errorf("result = %q, want %q", result, tc.wantResult)
			}
			if tc.wantResult == ResultNone {
				if p != nil {
					t.Error("expected nil policy for none")
				}
				return
			}
			want := created[policy.Scope(tc.wantResult)]
			if p == nil || p.ID != want.ID {
				t.Errorf("resolved policy = %+v, want id %s", p, want.ID)
			}
		})
	}
}

// A resolved policy is the whole winning document: a content-scoped policy
// that omits restricted_words means no restricted words, never the
// creator-scoped list (docs §6.2).
func TestWholeDocumentNoFieldMerge(t *testing.T) {
	f := newFixture(t)
	creatorPolicy := f.mustCreate(t, creatorA, policy.ScopeCreator, creatorA)
	creatorPolicy.RestrictedWords = []string{"badword"}
	if err := f.repo.Update(context.Background(), creatorPolicy); err != nil {
		t.Fatal(err)
	}
	f.mustCreate(t, creatorA, policy.ScopeContent, "ct_quiet") // empty document

	p, result, err := f.res.Resolve(context.Background(), creatorA, "twitch", "ct_quiet")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if result != ResultContent {
		t.Fatalf("result = %q, want content", result)
	}
	if len(p.RestrictedWords) != 0 {
		t.Errorf("restricted_words leaked from creator scope: %v", p.RestrictedWords)
	}
}

func TestPositiveResultIsCached(t *testing.T) {
	f := newFixture(t)
	f.mustCreate(t, creatorA, policy.ScopeCreator, creatorA)

	if _, _, err := f.res.Resolve(context.Background(), creatorA, "twitch", "ct_1"); err != nil {
		t.Fatal(err)
	}
	dbCalls := f.repo.Calls("GetByScope")
	p, result, err := f.res.Resolve(context.Background(), creatorA, "twitch", "ct_1")
	if err != nil {
		t.Fatal(err)
	}
	if result != ResultCreator || p == nil {
		t.Fatalf("cached resolve = (%v, %q)", p, result)
	}
	if got := f.repo.Calls("GetByScope"); got != dbCalls {
		t.Errorf("second resolve hit the repository: %d calls, want %d", got, dbCalls)
	}
}

func TestNegativeResultIsCached(t *testing.T) {
	f := newFixture(t)

	p, result, err := f.res.Resolve(context.Background(), creatorB, "youtube", "ct_none")
	if err != nil {
		t.Fatal(err)
	}
	if p != nil || result != ResultNone {
		t.Fatalf("first resolve = (%v, %q), want (nil, none)", p, result)
	}
	if f.c.Len() != 1 {
		t.Fatalf("negative result was not stored in the cache (%d entries)", f.c.Len())
	}
	dbCalls := f.repo.Calls("GetByScope")

	p, result, err = f.res.Resolve(context.Background(), creatorB, "youtube", "ct_none")
	if err != nil {
		t.Fatal(err)
	}
	if p != nil || result != ResultNone {
		t.Fatalf("second resolve = (%v, %q), want (nil, none)", p, result)
	}
	if got := f.repo.Calls("GetByScope"); got != dbCalls {
		t.Errorf("cached negative still hit the repository: %d calls, want %d", got, dbCalls)
	}
}

func TestMemcachedDownReadsThrough(t *testing.T) {
	f := newFixture(t)
	created := f.mustCreate(t, creatorA, policy.ScopeCreator, creatorA)
	f.c.SetDown(true)

	for i := 0; i < 2; i++ {
		p, result, err := f.res.Resolve(context.Background(), creatorA, "twitch", "ct_1")
		if err != nil {
			t.Fatalf("resolve with memcached down: %v", err)
		}
		if result != ResultCreator || p == nil || p.ID != created.ID {
			t.Fatalf("resolve = (%v, %q), want the creator policy", p, result)
		}
	}
	// Every resolve read through to the repository: three scope lookups
	// each (content miss, platform miss, creator hit).
	if got := f.repo.Calls("GetByScope"); got != 6 {
		t.Errorf("repository calls = %d, want 6", got)
	}
	if up := testutil.ToFloat64(f.std.DependencyUp.WithLabelValues("memcached")); up != 0 {
		t.Errorf("dependency_up{memcached} = %v, want 0", up)
	}

	// Recovery: cache back up, next resolve repopulates it.
	f.c.SetDown(false)
	if _, _, err := f.res.Resolve(context.Background(), creatorA, "twitch", "ct_1"); err != nil {
		t.Fatal(err)
	}
	if up := testutil.ToFloat64(f.std.DependencyUp.WithLabelValues("memcached")); up != 1 {
		t.Errorf("dependency_up{memcached} after recovery = %v, want 1", up)
	}
	if f.c.Len() != 1 {
		t.Errorf("cache entries after recovery = %d, want 1", f.c.Len())
	}
}

func TestResolveDoesNotServeOtherCreators(t *testing.T) {
	f := newFixture(t)
	f.mustCreate(t, creatorA, policy.ScopeCreator, creatorA)

	p, result, err := f.res.Resolve(context.Background(), creatorB, "twitch", "ct_1")
	if err != nil {
		t.Fatal(err)
	}
	if p != nil || result != ResultNone {
		t.Errorf("creator B resolved creator A's policy: (%v, %q)", p, result)
	}
}

func TestTTLSecondsConversion(t *testing.T) {
	r := New(memstore.New(), cachetest.New(), metrics.New(prometheus.NewRegistry()), obs.NewMetrics(prometheus.NewRegistry()), 300*time.Second)
	if r.ttl != 300 {
		t.Errorf("ttl = %d, want 300", r.ttl)
	}
}
