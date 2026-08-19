package mod

import (
	"container/list"
	"context"
	"sync"
	"time"

	"dabet/pkg/policyapi"
)

// CachedPolicy is one resolved-policy cache entry. Policy == nil is the
// cached NEGATIVE answer — "no policy at any scope" — which per §6.7 must
// be cached exactly like a positive one. Matcher is the restricted-words
// automaton compiled once per policy and cached alongside it (§7.4); nil
// when the policy has no restricted words.
type CachedPolicy struct {
	Policy  *policyapi.ResolvedPolicy
	Matcher *Matcher
}

// PolicyGetter is what the pipeline needs from the policy layer.
type PolicyGetter interface {
	Get(ctx context.Context, creatorID, contentID string) (CachedPolicy, error)
}

// PolicyCache is the in-process LRU of §6.8: TTL 60 s, bounded to 100K
// entries (A10), negative results cached with the same TTL. An RPC error
// with a cold (or expired) entry returns the error and caches nothing —
// the pipeline fails open (§4.7).
//
// KEYING DEVIATION (documented): §6.8 keys the cache
// (creator_id, platform, content_id), but adapter events deliberately
// carry no platform field (§1.4), so the cache is keyed
// (creator_id, content_id) and GetPolicy is called with an empty
// platform. platform-scoped policies therefore never match from this
// call path; resolution is effectively content > creator > none.
type PolicyCache struct {
	client  policyapi.PolicyServiceClient
	ttl     time.Duration
	maxSize int
	timeout time.Duration
	now     func() time.Time

	mu    sync.Mutex
	ll    *list.List // front = most recently used
	items map[string]*list.Element
}

type policyEntry struct {
	key     string
	val     CachedPolicy
	expires time.Time
}

// NewPolicyCache builds the cache over a PolicyService client. timeout
// bounds each GetPolicy RPC.
func NewPolicyCache(client policyapi.PolicyServiceClient, ttl time.Duration, maxSize int, timeout time.Duration, now func() time.Time) *PolicyCache {
	return &PolicyCache{
		client:  client,
		ttl:     ttl,
		maxSize: maxSize,
		timeout: timeout,
		now:     now,
		ll:      list.New(),
		items:   make(map[string]*list.Element),
	}
}

// Get returns the cached entry or resolves it via GetPolicy.
func (c *PolicyCache) Get(ctx context.Context, creatorID, contentID string) (CachedPolicy, error) {
	key := creatorID + "\x00" + contentID
	now := c.now()

	c.mu.Lock()
	if el, ok := c.items[key]; ok {
		e := el.Value.(*policyEntry)
		if now.Before(e.expires) {
			c.ll.MoveToFront(el)
			val := e.val
			c.mu.Unlock()
			return val, nil
		}
		c.ll.Remove(el)
		delete(c.items, key)
	}
	c.mu.Unlock()

	rpcCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.client.GetPolicy(rpcCtx, &policyapi.GetPolicyRequest{
		CreatorId: creatorID,
		Platform:  "", // adapter events carry no platform (§1.4); see type comment
		ContentId: contentID,
	})
	if err != nil {
		return CachedPolicy{}, err
	}

	var val CachedPolicy
	if resp.GetFound() {
		val.Policy = resp.GetPolicy()
		if words := val.Policy.GetRestrictedWords(); len(words) > 0 {
			val.Matcher = NewMatcher(words)
		}
	}

	c.mu.Lock()
	if el, ok := c.items[key]; ok {
		c.ll.Remove(el)
		delete(c.items, key)
	}
	c.items[key] = c.ll.PushFront(&policyEntry{key: key, val: val, expires: now.Add(c.ttl)})
	for c.ll.Len() > c.maxSize {
		back := c.ll.Back()
		c.ll.Remove(back)
		delete(c.items, back.Value.(*policyEntry).key)
	}
	c.mu.Unlock()
	return val, nil
}
