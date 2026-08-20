package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The latency injector turns the instant-response mock into something
// that behaves like the vLLM stage of docs §4.6.
//
// This matters more than it looks. The real LLM hop is budgeted at
// 1 000 ms and dominates the entire latency budget; the §7.9 batcher
// (32 messages or 50 ms, A18) and the §7.5 sampler (30/min per content,
// A17) only exhibit their real behaviour when the thing they are
// protecting is actually slow. Against an instant mock, every batch
// leaves at size 1 on the linger timer, no queue ever forms, and a load
// run would "prove" that the cascade is fast while never exercising the
// mechanism the cascade exists to protect.
//
// Everything here is off by default: with no MOCKLLM_* variables set,
// the handler behaves exactly as before, which is what test/e2e's
// deterministic FLAGME expectations depend on.
//
//	MOCKLLM_P50_MS          median service time, ms (default 0 = instant)
//	MOCKLLM_P99_MS          99th percentile service time, ms (default = p50)
//	MOCKLLM_MS_PER_MESSAGE  extra ms per listed message in the batch
//	MOCKLLM_MS_PER_KB       extra ms per KB of prompt (the token-rate model)
//	MOCKLLM_CONCURRENCY     max in-flight requests; excess queues (default 0 = unlimited)
//	MOCKLLM_ERROR_RATE      fraction of requests answered 503 (default 0)
//	MOCKLLM_TIMEOUT_RATE    fraction of requests held past the caller's deadline
//	MOCKLLM_TIMEOUT_HANG_MS how long a held request sleeps (default 5000)
//	MOCKLLM_SEED            PRNG seed (default 1)
type injector struct {
	p50, p99     time.Duration
	msPerMessage float64
	msPerKB      float64
	errorRate    float64
	timeoutRate  float64
	timeoutHang  time.Duration

	sem chan struct{}

	mu  sync.Mutex
	rng *rand.Rand

	inflight  atomic.Int64
	maxInflig atomic.Int64
	queued    atomic.Int64
	served    atomic.Int64
	errored   atomic.Int64
	hung      atomic.Int64
	// sigma of the fitted lognormal; zero means "always exactly p50".
	sigma float64
	mu0   float64
}

func envFloat(name string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func envDur(name string, def time.Duration) time.Duration {
	ms := envFloat(name, float64(def)/float64(time.Millisecond))
	return time.Duration(ms * float64(time.Millisecond))
}

// newInjector reads the configuration from the environment.
//
// The service-time distribution is a lognormal fitted to the requested
// p50 and p99, which is the right shape for a batched GPU service: a
// tight median with a long right tail from queueing and from the
// occasional long generation. p99 == p50 collapses it to a constant.
func newInjector(log *slog.Logger) *injector {
	in := &injector{
		p50:          envDur("MOCKLLM_P50_MS", 0),
		msPerMessage: envFloat("MOCKLLM_MS_PER_MESSAGE", 0),
		msPerKB:      envFloat("MOCKLLM_MS_PER_KB", 0),
		errorRate:    envFloat("MOCKLLM_ERROR_RATE", 0),
		timeoutRate:  envFloat("MOCKLLM_TIMEOUT_RATE", 0),
		timeoutHang:  envDur("MOCKLLM_TIMEOUT_HANG_MS", 5*time.Second),
		rng:          rand.New(rand.NewPCG(uint64(envFloat("MOCKLLM_SEED", 1)), 0x5DEECE66D)),
	}
	in.p99 = envDur("MOCKLLM_P99_MS", in.p50)
	if in.p50 > 0 && in.p99 > in.p50 {
		in.mu0 = math.Log(float64(in.p50))
		// z(0.99) = 2.3263478740408408
		in.sigma = math.Log(float64(in.p99)/float64(in.p50)) / 2.3263478740408408
	} else if in.p50 > 0 {
		in.mu0 = math.Log(float64(in.p50))
	}
	if c := int(envFloat("MOCKLLM_CONCURRENCY", 0)); c > 0 {
		in.sem = make(chan struct{}, c)
	}
	if in.active() {
		log.Info("latency injection enabled",
			"p50_ms", in.p50.Milliseconds(), "p99_ms", in.p99.Milliseconds(),
			"ms_per_message", in.msPerMessage, "ms_per_kb", in.msPerKB,
			"concurrency", cap(in.sem),
			"error_rate", in.errorRate, "timeout_rate", in.timeoutRate)
	}
	return in
}

// active reports whether any injection is configured. When nothing is,
// wrap returns the handler untouched so the deterministic path that
// test/e2e depends on is bit-for-bit unchanged.
func (in *injector) active() bool {
	return in.p50 > 0 || in.msPerMessage > 0 || in.msPerKB > 0 ||
		in.sem != nil || in.errorRate > 0 || in.timeoutRate > 0
}

// draw samples one service time for a request of the given shape.
func (in *injector) draw(messages int, promptBytes int) time.Duration {
	base := time.Duration(0)
	if in.p50 > 0 {
		in.mu.Lock()
		z := in.rng.NormFloat64()
		in.mu.Unlock()
		base = time.Duration(math.Exp(in.mu0 + in.sigma*z))
	}
	extra := in.msPerMessage*float64(messages) + in.msPerKB*float64(promptBytes)/1024
	return base + time.Duration(extra*float64(time.Millisecond))
}

func (in *injector) coin(p float64) bool {
	if p <= 0 {
		return false
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.rng.Float64() < p
}

// wrap decorates the completions handler with the configured
// behaviour: a concurrency ceiling (requests above it queue, which is
// what makes a real vLLM's latency climb under load rather than its
// throughput collapse), a service-time draw, and error/timeout
// injection.
func (in *injector) wrap(next http.HandlerFunc) http.HandlerFunc {
	if !in.active() {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if in.sem != nil {
			in.queued.Add(1)
			select {
			case in.sem <- struct{}{}:
				in.queued.Add(-1)
			case <-r.Context().Done():
				in.queued.Add(-1)
				return
			}
			defer func() { <-in.sem }()
		}
		cur := in.inflight.Add(1)
		defer in.inflight.Add(-1)
		for {
			m := in.maxInflig.Load()
			if cur <= m || in.maxInflig.CompareAndSwap(m, cur) {
				break
			}
		}

		// Sizing comes from Content-Length rather than from parsing the
		// body: the wrapper must not consume the request the inner
		// handler is about to decode, and a copy per request would put
		// the mock's own allocation in the measurement. ~100 bytes per
		// listed message is close enough for a service-time model.
		promptBytes := int(r.ContentLength)
		if promptBytes < 0 {
			promptBytes = 0
		}
		messages := promptBytes / 100

		switch {
		case in.coin(in.errorRate):
			in.errored.Add(1)
			sleepCtx(r.Context(), in.draw(messages, promptBytes))
			http.Error(w, `{"error":"injected upstream failure"}`, http.StatusServiceUnavailable)
			return
		case in.coin(in.timeoutRate):
			in.hung.Add(1)
			// Hold the request past the caller's 1 000 ms deadline
			// (§7.9). The client gives up and fails the whole batch
			// open; the mock never answers, which is exactly what a
			// saturated GPU looks like from the caller's side.
			sleepCtx(r.Context(), in.timeoutHang)
			return
		}

		sleepCtx(r.Context(), in.draw(messages, promptBytes))
		if r.Context().Err() != nil {
			return
		}
		in.served.Add(1)
		next(w, r)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// stats exposes what the injector did, so a load run can report the
// mock's own saturation alongside the service's llm_* metrics and tell
// "the LLM was slow" apart from "the caller never got that far".
func (in *injector) stats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"p50_ms":          in.p50.Milliseconds(),
		"p99_ms":          in.p99.Milliseconds(),
		"concurrency_cap": cap(in.sem),
		"inflight":        in.inflight.Load(),
		"max_inflight":    in.maxInflig.Load(),
		"queued":          in.queued.Load(),
		"served":          in.served.Load(),
		"errored":         in.errored.Load(),
		"hung":            in.hung.Load(),
	})
}
