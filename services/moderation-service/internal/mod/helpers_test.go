package mod

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"dabet/pkg/contracts"
	"dabet/pkg/obs"
	"dabet/pkg/policyapi"
)

var t0 = time.Date(2026, 8, 19, 14, 2, 11, 0, time.UTC)

// fakeClock is a mutex-guarded manual clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{t: start} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// fakeProducer records produced events and can be told to fail.
type fakeProducer struct {
	mu      sync.Mutex
	records []producedRecord
	fail    bool
}

type producedRecord struct {
	Topic string
	Key   string
	Value []byte
}

func (f *fakeProducer) Produce(_ context.Context, topic string, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return context.DeadlineExceeded
	}
	f.records = append(f.records, producedRecord{Topic: topic, Key: string(key), Value: append([]byte(nil), value...)})
	return nil
}

func (f *fakeProducer) byTopic(topic string) []producedRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []producedRecord
	for _, r := range f.records {
		if r.Topic == topic {
			out = append(out, r)
		}
	}
	return out
}

type fakePolicyGetter struct {
	mu    sync.Mutex
	val   CachedPolicy
	err   error
	calls int
}

func (f *fakePolicyGetter) Get(context.Context, string, string) (CachedPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.val, f.err
}

type fakeCredits struct{ ok bool }

func (f *fakeCredits) OK(context.Context, string) bool { return f.ok }

type fakeEmbedder struct {
	vec []float32
	err error
}

func (f *fakeEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = f.vec
	}
	return out, nil
}

type fakeClassifier struct {
	mu       sync.Mutex
	verdicts []int
	err      error
	calls    int
	batches  [][]string
}

func (f *fakeClassifier) Classify(_ context.Context, _ *policyapi.ResolvedPolicy, texts []string) ([]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.batches = append(f.batches, texts)
	if f.err != nil {
		return nil, f.err
	}
	out := make([]int, len(texts))
	copy(out, f.verdicts)
	return out, nil
}

// redisProbe is a go-redis hook that counts every command actually
// reaching the client and can make each one slow. Counting is the point:
// the F2 fix is that an open breaker issues NO call, which is only
// provable by watching the client, not the metrics. The delay stands in
// for the dial timeout and retry ladder a real outage costs.
type redisProbe struct {
	mu    sync.Mutex
	calls int
	delay time.Duration
}

func (p *redisProbe) DialHook(next redis.DialHook) redis.DialHook { return next }

func (p *redisProbe) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		p.enter()
		return next(ctx, cmd)
	}
}

func (p *redisProbe) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		p.enter()
		return next(ctx, cmds)
	}
}

func (p *redisProbe) enter() {
	p.mu.Lock()
	p.calls++
	d := p.delay
	p.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
}

func (p *redisProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *redisProbe) setDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

// pipeEnv wires a pipeline over fakes + miniredis.
type pipeEnv struct {
	pipe    *Pipeline
	met     *Metrics
	prod    *fakeProducer
	clock   *fakeClock
	mr      *miniredis.Miniredis
	rdb     *redis.Client
	probe   *redisProbe
	policy  *fakePolicyGetter
	credits *fakeCredits
	embed   *fakeEmbedder
	llm     *fakeClassifier
	usage   *UsageAggregator
}

func newPipeEnv(t *testing.T, cfg Config) *pipeEnv {
	t.Helper()
	mr := miniredis.RunT(t)
	// MaxRetries: -1 disables go-redis' internal retry ladder so a test's
	// client-call count is the number of logical operations, not a
	// multiple of it. Production tunes the same knob via MOD_REDIS_*.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	probe := &redisProbe{}
	rdb.AddHook(probe)
	t.Cleanup(func() { _ = rdb.Close() })

	reg := prometheus.NewRegistry()
	met := NewMetrics(reg, obs.NewMetrics(reg))
	prod := &fakeProducer{}
	clock := newFakeClock(t0)

	pub := NewPublisher(prod, 10*time.Millisecond, func() {
		met.FailOpen.WithLabelValues("kafka", "").Inc()
	})
	pub.baseDelay = time.Millisecond
	pub.sleep = func(time.Duration) {}

	env := &pipeEnv{
		met:     met,
		prod:    prod,
		clock:   clock,
		mr:      mr,
		rdb:     rdb,
		probe:   probe,
		policy:  &fakePolicyGetter{},
		credits: &fakeCredits{ok: true},
		embed:   &fakeEmbedder{vec: []float32{1, 0, 0}},
		llm:     &fakeClassifier{},
		usage:   NewUsageAggregator("inst-1", pub, clock.Now),
	}
	env.pipe = NewPipeline(cfg, env.policy, env.credits, NewRedisState(rdb), env.embed, env.llm, pub, env.usage, met, clock.Now)
	return env
}

// warmRedis opens the client connection and returns the probe count at
// that point, so a later assertion measures cascade traffic rather than
// go-redis' one-off connection handshake.
func (e *pipeEnv) warmRedis(t *testing.T) int {
	t.Helper()
	if err := e.rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return e.probe.count()
}

func (e *pipeEnv) process(t *testing.T, msg contracts.Message) {
	t.Helper()
	val, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	e.pipe.Process(context.Background(), val)
}

func testMessage(id, text string) contracts.Message {
	return contracts.Message{
		MessageID:  id,
		ContentID:  "ct_9f2a",
		AuthorID:   "sd_3b71",
		CreatorID:  "9d4e-creator",
		Text:       text,
		IngestedAt: t0.Add(-500 * time.Millisecond),
	}
}

func i32(v int32) *int32 { return &v }

func testPolicy(mut ...func(*policyapi.ResolvedPolicy)) *policyapi.ResolvedPolicy {
	p := &policyapi.ResolvedPolicy{
		PolicyId:                "pol_7a13",
		Spam:                    policyapi.SpamMode_SPAM_MODE_NONE,
		RestrictedContentAction: policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO,
	}
	for _, m := range mut {
		m(p)
	}
	return p
}

func cachedPolicy(p *policyapi.ResolvedPolicy) CachedPolicy {
	cp := CachedPolicy{Policy: p}
	if p != nil && len(p.GetRestrictedWords()) > 0 {
		cp.Matcher = NewMatcher(p.GetRestrictedWords())
	}
	return cp
}

func unmarshalFlagged(t *testing.T, rec producedRecord) contracts.Flagged {
	t.Helper()
	var f contracts.Flagged
	if err := json.Unmarshal(rec.Value, &f); err != nil {
		t.Fatal(err)
	}
	return f
}
