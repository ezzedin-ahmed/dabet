package trigger

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"dabet/services/clusters-job/internal/chstore"
	"dabet/services/clusters-job/internal/job"
)

type fakeLister struct{ creators []string }

func (f *fakeLister) ListCreators(context.Context) ([]string, error) { return f.creators, nil }

type fakeStats struct {
	total  int64
	recent int64
}

func (f *fakeStats) EstimatePoints(context.Context, string, time.Time, time.Time) (int64, error) {
	return f.total, nil
}
func (f *fakeStats) EstimateRecentPoints(context.Context, string, time.Time) (int64, error) {
	return f.recent, nil
}

type fakeAssigned struct{ assigned int64 }

func (f *fakeAssigned) AssignedSince(context.Context, string, time.Time) (int64, error) {
	return f.assigned, nil
}

type fakeState struct {
	mu     sync.Mutex
	states map[string]chstore.State
	puts   []chstore.State
}

func (f *fakeState) GetState(_ context.Context, creatorID string) (chstore.State, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.states[creatorID]
	return st, ok, nil
}
func (f *fakeState) PutState(_ context.Context, st chstore.State) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, st)
	return nil
}

type fakeRunner struct {
	mu   sync.Mutex
	runs []job.Decision
	res  job.Result
	ch   chan job.Decision
}

func (f *fakeRunner) Run(_ context.Context, d job.Decision) (job.Result, error) {
	f.mu.Lock()
	f.runs = append(f.runs, d)
	f.mu.Unlock()
	if f.ch != nil {
		f.ch <- d
	}
	return f.res, nil
}

func testScheduler(stats *fakeStats, assigned *fakeAssigned, state *fakeState, runner *fakeRunner) *Scheduler {
	cfg := Config{
		Interval:          time.Hour,
		WindowDays:        30,
		BootstrapMin:      100,
		UnassignedRate:    0.30,
		UnassignedMinBase: 100,
		Cooldown:          30 * time.Minute,
	}
	now := func() time.Time { return time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC) }
	return New(cfg, &fakeLister{creators: []string{"cr-1"}}, stats, assigned, state, runner,
		slog.New(slog.NewTextHandler(io.Discard, nil)), now)
}

func TestEvaluateBootstrap(t *testing.T) {
	s := testScheduler(&fakeStats{total: 150}, &fakeAssigned{}, &fakeState{}, &fakeRunner{})
	d, ok, err := s.Evaluate(context.Background(), "cr-1")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v, want bootstrap", ok, err)
	}
	if d.Trigger != job.TriggerBootstrap || d.CreatorID != "cr-1" {
		t.Errorf("decision = %+v", d)
	}
	if !d.From.Equal(d.To.AddDate(0, 0, -30)) {
		t.Errorf("window = %v..%v, want 30 days", d.From, d.To)
	}
}

func TestEvaluateBootstrapBelowThreshold(t *testing.T) {
	s := testScheduler(&fakeStats{total: 99}, &fakeAssigned{}, &fakeState{}, &fakeRunner{})
	if _, ok, err := s.Evaluate(context.Background(), "cr-1"); err != nil || ok {
		t.Fatalf("ok=%v err=%v, want no trigger below 100 points", ok, err)
	}
}

func stateAt(lastRun time.Time, lastCount int64) *fakeState {
	return &fakeState{states: map[string]chstore.State{
		"cr-1": {CreatorID: "cr-1", LastRunAt: lastRun, LastPointCount: lastCount},
	}}
}

func TestEvaluateDoubled(t *testing.T) {
	lastRun := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC) // 2h ago, past cooldown
	s := testScheduler(&fakeStats{total: 200}, &fakeAssigned{}, stateAt(lastRun, 100), &fakeRunner{})
	d, ok, err := s.Evaluate(context.Background(), "cr-1")
	if err != nil || !ok || d.Trigger != job.TriggerDoubled {
		t.Fatalf("d=%+v ok=%v err=%v, want doubled", d, ok, err)
	}
}

func TestEvaluateCooldownBlocks(t *testing.T) {
	lastRun := time.Date(2026, 8, 19, 11, 45, 0, 0, time.UTC) // 15m ago, within cooldown
	s := testScheduler(&fakeStats{total: 1000, recent: 1000}, &fakeAssigned{assigned: 0},
		stateAt(lastRun, 100), &fakeRunner{})
	if _, ok, _ := s.Evaluate(context.Background(), "cr-1"); ok {
		t.Fatal("cooldown must suppress periodic triggers")
	}
}

func TestEvaluateUnassignedRate(t *testing.T) {
	lastRun := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	// Not doubled (150 < 2*100); 200 embedded in the hour, 100 assigned:
	// rate 0.5 > 0.30.
	s := testScheduler(&fakeStats{total: 150, recent: 200}, &fakeAssigned{assigned: 100},
		stateAt(lastRun, 100), &fakeRunner{})
	d, ok, err := s.Evaluate(context.Background(), "cr-1")
	if err != nil || !ok || d.Trigger != job.TriggerUnassigned {
		t.Fatalf("d=%+v ok=%v err=%v, want unassigned", d, ok, err)
	}
}

func TestEvaluateUnassignedBelowRate(t *testing.T) {
	lastRun := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	s := testScheduler(&fakeStats{total: 150, recent: 200}, &fakeAssigned{assigned: 180},
		stateAt(lastRun, 100), &fakeRunner{})
	if _, ok, _ := s.Evaluate(context.Background(), "cr-1"); ok {
		t.Fatal("rate 0.1 must not trigger")
	}
}

func TestEvaluateUnassignedNeedsBase(t *testing.T) {
	lastRun := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	// Only 50 embedded points in the hour: too thin for the rate to mean
	// anything, even though nothing was assigned.
	s := testScheduler(&fakeStats{total: 150, recent: 50}, &fakeAssigned{assigned: 0},
		stateAt(lastRun, 100), &fakeRunner{})
	if _, ok, _ := s.Evaluate(context.Background(), "cr-1"); ok {
		t.Fatal("thin hour must not trigger on rate")
	}
}

func TestOnDemandExecutionAndState(t *testing.T) {
	runner := &fakeRunner{res: job.Result{PointsRead: 40}, ch: make(chan job.Decision, 1)}
	state := &fakeState{}
	s := testScheduler(&fakeStats{}, &fakeAssigned{}, state, runner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Loop(ctx); close(done) }()

	d := job.Decision{CreatorID: "cr-9", Trigger: job.TriggerOnDemand,
		From:  time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		To:    time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC),
		JobID: "job-1"}
	if !s.Enqueue(d) {
		t.Fatal("enqueue failed")
	}
	select {
	case got := <-runner.ch:
		if got != d {
			t.Errorf("runner got %+v, want %+v", got, d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("on-demand job never ran")
	}
	cancel()
	<-done
	// On-demand runs rewrite history; they do not reset the periodic
	// baseline state.
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.puts) != 0 {
		t.Errorf("on-demand run wrote state %+v", state.puts)
	}
}

func TestSweepRunsAndPersistsState(t *testing.T) {
	runner := &fakeRunner{res: job.Result{PointsRead: 123}, ch: make(chan job.Decision, 64)}
	state := &fakeState{}
	s := testScheduler(&fakeStats{total: 150}, &fakeAssigned{}, state, runner)
	s.cfg.Interval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Loop(ctx); close(done) }()

	select {
	case got := <-runner.ch:
		if got.Trigger != job.TriggerBootstrap || got.CreatorID != "cr-1" {
			t.Errorf("sweep ran %+v, want bootstrap for cr-1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sweep never ran the bootstrap job")
	}
	cancel()
	<-done

	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.puts) == 0 {
		t.Fatal("periodic run did not persist state")
	}
	if st := state.puts[0]; st.CreatorID != "cr-1" || st.LastPointCount != 123 {
		t.Errorf("persisted state = %+v", st)
	}
}

func TestEnqueueFullQueue(t *testing.T) {
	s := testScheduler(&fakeStats{}, &fakeAssigned{}, &fakeState{}, &fakeRunner{})
	s.cfg.OnDemandQueueDepth = 0 // default 64 still applies from New; fill it
	for i := 0; i < 64; i++ {
		if !s.Enqueue(job.Decision{JobID: "x"}) {
			t.Fatalf("queue rejected enqueue %d before capacity", i)
		}
	}
	if s.Enqueue(job.Decision{JobID: "overflow"}) {
		t.Error("full queue accepted an enqueue")
	}
}
