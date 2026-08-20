// Package runner executes one scenario end to end: provision the
// account, snapshot every /metrics endpoint, drive the schedule, sample
// lag while it runs, inject any faults, drain, snapshot again, and
// produce the report.
package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"dabet/pkg/contracts"
	"dabet/test/load/internal/drill"
	"dabet/test/load/internal/gen"
	"dabet/test/load/internal/hist"
	"dabet/test/load/internal/kadmlag"
	"dabet/test/load/internal/promx"
	"dabet/test/load/internal/report"
	"dabet/test/load/internal/scenario"
	"dabet/test/load/internal/sched"
	"dabet/test/load/internal/setup"
	"dabet/test/load/internal/sink"
	"dabet/test/load/internal/tail"
)

// Options are the run-wide settings that are not part of the scenario.
type Options struct {
	Brokers        []string
	Endpoints      setup.Endpoints
	AdapterURL     string
	StripeSecret   string
	Targets        []promx.Target
	ModerationName string // which target is the primary moderation-service
	ConsumerGroup  string
	ComposeProject string
	DryRunDrills   bool
	// GeneratorCeiling, when known from a prior selfbench run, is
	// carried into the report so every run states how much headroom the
	// harness had.
	GeneratorCeiling float64
	// SkipProvision reuses an already-provisioned creator, so a series
	// of runs does not create an account per scenario.
	Account *setup.Account
	Log     io.Writer
}

// Runner executes scenarios.
type Runner struct {
	opt Options
}

// New builds a runner.
func New(opt Options) *Runner { return &Runner{opt: opt} }

func (r *Runner) logf(format string, args ...any) {
	if r.opt.Log != nil {
		fmt.Fprintf(r.opt.Log, format+"\n", args...)
	}
}

// Run executes one scenario and returns its result.
func (r *Runner) Run(ctx context.Context, sc scenario.Scenario, runID string) (*report.Result, error) {
	machine := report.Machine{
		CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		OS: runtime.GOOS, Arch: runtime.GOARCH,
	}
	res := report.New(sc.Name, sc.Hypothesis, runID, machine)
	if cfg, err := json.Marshal(sc); err == nil {
		res.Config = cfg
	}
	res.Note("%s", sc.Proves)

	pop := sc.Population
	pop.RunID = runID
	if err := pop.Mix.Validate(); err != nil {
		return nil, err
	}

	schedule, err := sched.Compile(sc.Profile)
	if err != nil {
		return nil, err
	}
	res.Offered.ProfileName = sc.ProfileName
	res.Offered.TargetPeakRate = sc.Profile.PeakRate()
	res.Offered.Scheduled = schedule.Total()
	res.Offered.GeneratorCeiling = r.opt.GeneratorCeiling

	// ---- self-benchmark takes the short path: no stack involved.
	if sc.Mode == scenario.ModeSelfBench {
		return r.runSelfBench(ctx, sc, pop, schedule, res)
	}

	// ---- provision
	acct := r.opt.Account
	if acct == nil {
		cl := setup.New(r.opt.Endpoints, r.opt.StripeSecret, 0)
		if err := cl.WaitHealthy(ctx, 2*time.Minute); err != nil {
			return nil, err
		}
		acct, err = cl.Provision(ctx, sc.Policy)
		if err != nil {
			return nil, fmt.Errorf("provision: %w", err)
		}
		r.logf("provisioned creator %s policy %s", acct.CreatorID, acct.PolicyID)
	}
	pop.CreatorID = acct.CreatorID

	// The policy must be visible to moderation-service before the first
	// message arrives: a miss is cached as "no policy" for the local TTL
	// (A10, 60 s) and every detector would be silently skipped for that
	// whole window. The cache is keyed (creator_id, content_id) and this
	// run mints fresh content ids, so there is no stale negative entry —
	// but policy-service's own Memcached layer still needs a moment.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(2 * time.Second):
	}

	// ---- lag sampler
	lagCl, err := kadmlag.New(r.opt.Brokers, r.opt.ConsumerGroup)
	if err != nil {
		return nil, err
	}
	defer lagCl.Close()
	if n, err := lagCl.Partitions(ctx, contracts.TopicMessages); err == nil {
		res.Kafka.MessagesPartitions = n
	}

	// ---- verdict tailer
	tailer, err := tail.New(r.opt.Brokers, "loadtest-tail-"+runID, gen.RunPrefix(runID))
	if err != nil {
		return nil, err
	}
	if sc.TrackPerContent {
		tailer.TrackPerContent()
	}
	// Nothing in this run can legitimately take longer than the profile
	// plus its drain window plus a generous margin; anything that does
	// is a clock that moved, not a message that was slow.
	tailer.SetMaxPlausible(sc.Profile.Duration() + sc.DrainFor + 5*time.Minute)
	tailCtx, stopTail := context.WithCancel(ctx)
	var tailWG sync.WaitGroup
	tailWG.Add(1)
	go func() { defer tailWG.Done(); tailer.Run(tailCtx) }()
	defer func() { stopTail(); tailWG.Wait(); tailer.Close() }()
	// Give the tailer time to join its group and reach the end of the
	// topic before the first verdict is published, or the run loses its
	// opening verdicts.
	time.Sleep(3 * time.Second)

	// ---- baseline scrape
	scraper := promx.NewScraper(r.opt.Targets)
	before, beforeErrs := scraper.ScrapeAll(ctx)
	for name, err := range beforeErrs {
		res.Services[name] = report.ServiceView{ScrapeError: err.Error()}
	}

	// ---- sink
	snk, err := r.buildSink(sc)
	if err != nil {
		return nil, err
	}
	defer snk.Close()

	workers := sc.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	gens := make([]*gen.Generator, workers)
	for i := range gens {
		gens[i] = gen.NewGenerator(pop, i)
	}
	sendLag := hist.New()
	catCount := make([]int64, len(gen.Categories))
	var catMu sync.Mutex
	catLocal := make([][]int64, workers)
	for i := range catLocal {
		catLocal[i] = make([]int64, len(gen.Categories))
	}
	catIndex := map[gen.Category]int{}
	for i, c := range gen.Categories {
		catIndex[c] = i
	}

	// ---- background samplers and drills
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun() // belt and braces: the happy path cancels explicitly below
	start := time.Now()
	var samples []kadmlag.Sample
	var sampleMu sync.Mutex
	var bg sync.WaitGroup
	every := sc.SampleEvery
	if every <= 0 {
		every = 2 * time.Second
	}
	bg.Add(1)
	go func() {
		defer bg.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				s, err := lagCl.Sample(context.WithoutCancel(runCtx),
					[]string{contracts.TopicMessages}, false)
				if err != nil {
					continue
				}
				sampleMu.Lock()
				samples = append(samples, s)
				sampleMu.Unlock()
			}
		}
	}()

	var drillRunner *drill.Runner
	if len(sc.Drills) > 0 {
		if err := drill.Available(ctx); err != nil && !r.opt.DryRunDrills {
			return nil, fmt.Errorf("scenario %s needs docker: %w", sc.Name, err)
		}
		drillRunner = drill.New(r.opt.ComposeProject, r.opt.DryRunDrills)
		defer drillRunner.Restore(context.WithoutCancel(ctx))
		bg.Add(1)
		go func() {
			defer bg.Done()
			for _, d := range sc.Drills {
				select {
				case <-runCtx.Done():
					return
				case <-time.After(time.Until(start.Add(d.At))):
				}
				ev := drillRunner.Do(context.WithoutCancel(runCtx), start,
					d.Container, d.Action, d.Expect, d.Note)
				r.logf("  drill t+%.0fs %s %s%s", ev.OffsetS, ev.Action, ev.Container,
					map[bool]string{true: " ERR: " + ev.Err, false: ""}[ev.Err != ""])
			}
		}()
	}

	// Per-partition end offsets BEFORE the run. The imbalance figure has
	// to describe what this run produced, not the cumulative contents of
	// a topic that previous runs also wrote to.
	var baseline map[int32]int64
	if s0, err := lagCl.Sample(ctx, []string{contracts.TopicMessages}, true); err == nil {
		baseline = make(map[int32]int64, len(s0.Partitions))
		for _, pl := range s0.Partitions {
			baseline[pl.Partition] = pl.End
		}
	}

	// ---- drive
	r.logf("driving %s: %s", sc.Name, sc.ProfileName)
	driver := &sched.Driver{Schedule: schedule, Workers: workers, Granularity: 200 * time.Microsecond}
	stats := driver.Run(ctx, func(idx int64, intended time.Time, lag time.Duration) {
		w := int(idx % int64(workers))
		rec := gens[w].Next(intended)
		catLocal[w][catIndex[rec.Category]]++
		sendLag.Record(lag)
		_ = snk.Send(ctx, rec)
	})
	if err := snk.Flush(context.WithoutCancel(ctx)); err != nil {
		res.Note("sink flush error: %v", err)
	}
	sendDone := time.Now()
	r.logf("  sent %d in %.1fs (%.0f msg/s)", stats.Sent, sendDone.Sub(start).Seconds(),
		float64(stats.Sent)/sendDone.Sub(start).Seconds())

	catMu.Lock()
	for _, local := range catLocal {
		for i, v := range local {
			catCount[i] += v
		}
	}
	catMu.Unlock()

	// ---- drain
	drain := sc.DrainFor
	if drain > 0 {
		r.logf("  draining for %s", drain)
		select {
		case <-ctx.Done():
		case <-time.After(drain):
		}
	}
	stopRun()
	bg.Wait()

	// One final lag sample after the drain window, taken outside the
	// sampler so it is always present even on a short run.
	if s, err := lagCl.Sample(context.WithoutCancel(ctx), []string{contracts.TopicMessages}, true); err == nil {
		sampleMu.Lock()
		samples = append(samples, s)
		sampleMu.Unlock()
	}

	// ---- final scrape and assembly
	after, afterErrs := scraper.ScrapeAll(ctx)
	for name, err := range afterErrs {
		res.Services[name] = report.ServiceView{ScrapeError: err.Error()}
	}

	elapsed := sendDone.Sub(start)
	st := snk.Stats()
	res.Offered.Sent = stats.Sent
	res.Offered.Acked = st.Acked
	res.Offered.Failed = st.Failed
	res.Offered.Bytes = st.Bytes
	res.Offered.DurationS = elapsed.Seconds()
	if elapsed > 0 {
		res.Offered.AchievedRate = float64(stats.Sent) / elapsed.Seconds()
	}
	res.Offered.SendLag = sendLag.Summarize()

	r.fillKafka(res, samples, baseline)
	r.fillModeration(res, before, after, elapsed+drain)
	r.fillServices(res, before, after)
	res.Verdicts = tailer.Result()
	if n := res.Verdicts.ClockSkewedSamples; n > 0 {
		res.Note("%d verdicts were discarded as clock skew (measured latency above the plausible "+
			"bound). The host clock moved during the run; treat this run's latency as unusable.", n)
		res.Fatal("host clock stable through the run", "0 skewed samples",
			strconv.FormatInt(n, 10), false)
	}
	res.EndedAt = time.Now()

	// Generated category counts, so intent can be compared with the
	// detector hits the service actually reported.
	gencat := map[string]int64{}
	for i, c := range gen.Categories {
		gencat[string(c)] = catCount[i]
	}
	res.Extra["generated_by_category"] = gencat
	if drillRunner != nil {
		res.Extra["drills"] = drillRunner.Events()
	}
	if sc.TrackPerContent {
		res.Extra["sampler_coverage"] = r.samplerCoverage(sc, pop, res, elapsed)
	}

	r.evaluate(res, sc)
	return res, nil
}

func (r *Runner) buildSink(sc scenario.Scenario) (sink.Sink, error) {
	switch sc.Mode {
	case scenario.ModeAdapter:
		w := sc.Workers
		if w <= 0 {
			w = 64
		}
		return sink.NewAdapter(r.opt.AdapterURL, w), nil
	case scenario.ModeSelfBench:
		return sink.NewNull(true), nil
	default:
		return sink.NewKafka(sink.DefaultKafkaConfig(r.opt.Brokers))
	}
}

// runSelfBench drives the generator against the null sink to find its
// ceiling. The schedule is set far above anything achievable, so the
// driver runs flat out and the achieved rate is the answer.
func (r *Runner) runSelfBench(ctx context.Context, sc scenario.Scenario, pop gen.Config, schedule *sched.Schedule, res *report.Result) (*report.Result, error) {
	workers := sc.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	pop.CreatorID = "00000000-0000-0000-0000-000000000000"
	gens := make([]*gen.Generator, workers)
	for i := range gens {
		gens[i] = gen.NewGenerator(pop, i)
	}
	snk := sink.NewNull(true)
	sendLag := hist.New()

	bounded, cancel := context.WithTimeout(ctx, sc.Profile.Duration()+5*time.Second)
	defer cancel()
	start := time.Now()
	driver := &sched.Driver{Schedule: schedule, Workers: workers, Granularity: 200 * time.Microsecond}
	stats := driver.Run(bounded, func(idx int64, intended time.Time, lag time.Duration) {
		rec := gens[int(idx%int64(workers))].Next(intended)
		sendLag.Record(lag)
		_ = snk.Send(bounded, rec)
	})
	elapsed := time.Since(start)

	st := snk.Stats()
	res.Offered.Sent = stats.Sent
	res.Offered.Acked = st.Acked
	res.Offered.Bytes = st.Bytes
	res.Offered.DurationS = elapsed.Seconds()
	res.Offered.AchievedRate = float64(stats.Sent) / elapsed.Seconds()
	res.Offered.SendLag = sendLag.Summarize()
	res.EndedAt = time.Now()
	res.Note("generator ceiling on this machine with %d workers: %.0f msg/s, %.1f MB/s of encoded records",
		workers, res.Offered.AchievedRate, float64(st.Bytes)/elapsed.Seconds()/1e6)
	res.Note("every other scenario reports its achieved rate against this number; a run that " +
		"approaches it measured the harness rather than Dabet")
	return res, nil
}

// fillKafka turns the lag samples into the report's broker view,
// including the least-squares slope that distinguishes "lag, accepted"
// (§4.7) from "lag growing without bound".
func (r *Runner) fillKafka(res *report.Result, samples []kadmlag.Sample, baseline map[int32]int64) {
	res.Kafka.LagSamples = samples
	if len(samples) == 0 {
		return
	}
	var peak int64
	for _, s := range samples {
		if s.Total > peak {
			peak = s.Total
		}
	}
	res.Kafka.PeakLag = peak
	last := samples[len(samples)-1]
	res.Kafka.FinalLag = last.Total

	// Slope over the send window only: the drain tail is expected to
	// fall and would mask growth.
	res.Kafka.LagSlopePerS = slope(samples)

	if len(last.Partitions) > 0 {
		// Per-partition production is reported as THIS run's delta: the
		// absolute end offset carries every previous run on the same
		// topic, which would dilute exactly the concentration the
		// hot-spot scenario is trying to show.
		parts := make([]kadmlag.PartitionLag, 0, len(last.Partitions))
		for _, pl := range last.Partitions {
			if baseline != nil {
				pl.End -= baseline[pl.Partition]
				if pl.End < 0 {
					pl.End = 0
				}
			}
			parts = append(parts, pl)
		}
		var sum, max float64
		for _, p := range parts {
			sum += float64(p.End)
			max = math.Max(max, float64(p.End))
		}
		if n := float64(len(parts)); n > 0 && sum > 0 {
			res.Kafka.PartitionImbalance = max / (sum / n)
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i].End > parts[j].End })
		if len(parts) > 5 {
			parts = parts[:5]
		}
		res.Kafka.BusiestPartitions = parts
	}
}

// slope is the least-squares gradient of total lag against time, over
// the rising part of the series (up to the peak), in messages/second.
func slope(samples []kadmlag.Sample) float64 {
	peakAt := 0
	for i, s := range samples {
		if s.Total > samples[peakAt].Total {
			peakAt = i
		}
	}
	rise := samples[:peakAt+1]
	if len(rise) < 3 {
		return 0
	}
	t0 := rise[0].At
	var n, sx, sy, sxy, sxx float64
	for _, s := range rise {
		x := s.At.Sub(t0).Seconds()
		y := float64(s.Total)
		n++
		sx += x
		sy += y
		sxy += x * y
		sxx += x * x
	}
	den := n*sxx - sx*sx
	if den == 0 {
		return 0
	}
	return (n*sxy - sx*sy) / den
}

// fillModeration diffs the moderation-service scrapes.
func (r *Runner) fillModeration(res *report.Result, before, after map[string]*promx.Snapshot, window time.Duration) {
	m := &res.Moderation
	m.MessagesByOutcome = map[string]float64{}
	m.DetectorHits = map[string]float64{}
	m.DetectorActions = map[string]float64{}
	m.StageP95S = map[string]float64{}
	m.LLMRequests = map[string]float64{}
	m.FailOpen = map[string]float64{}
	m.Instances = map[string]float64{}

	for name, aft := range after {
		bef, ok := before[name]
		if !ok {
			continue
		}
		if !aft.Has("moderation_messages_total") {
			continue
		}
		delta, restarted := promx.Delta(bef, aft)
		if restarted {
			res.Note("%s restarted mid-run: its counters were reset, so its contribution is a lower bound", name)
		}
		for k, v := range delta.ByLabel("moderation_messages_total", "outcome", nil) {
			m.MessagesByOutcome[k] += v
		}
		for k, v := range delta.ByLabel("moderation_detector_hits_total", "detector", nil) {
			m.DetectorHits[k] += v
		}
		for k, v := range delta.ByLabel("moderation_detector_hits_total", "action", nil) {
			m.DetectorActions[k] += v
		}
		consumed := delta.Sum("kafka_messages_consumed_total", map[string]string{"topic": contracts.TopicMessages})
		m.ConsumedTotal += consumed
		m.Instances[name] = consumed
		m.SamplerSkipped += delta.Sum("sampler_skipped_total", nil)
		for k, v := range delta.ByLabel("llm_requests_total", "outcome", nil) {
			m.LLMRequests[k] += v
		}
		for _, s := range delta.Series("fail_open_total", nil) {
			key := s.Labels["component"]
			if reason := s.Labels["reason"]; reason != "" {
				key += "/" + reason
			}
			m.FailOpen[key] += s.Value
			m.FailOpenTotal += s.Value
		}

		if bh, ok1 := bef.Histogram("moderation_e2e_latency_seconds", nil); ok1 {
			if ah, ok2 := aft.Histogram("moderation_e2e_latency_seconds", nil); ok2 {
				d := promx.SubHistogram(bh, ah)
				if d.Count > 0 {
					m.E2ELatency = mergeView(m.E2ELatency, d, m.E2ELatencyPresent)
					m.E2ELatencyPresent = true
				}
			}
		}
		if bh, ok1 := bef.Histogram("llm_batch_size", nil); ok1 {
			if ah, ok2 := aft.Histogram("llm_batch_size", nil); ok2 {
				d := promx.SubHistogram(bh, ah)
				m.LLMBatches += d.Count
				if d.Count > 0 {
					m.LLMBatchMean = d.Sum / d.Count
					m.LLMBatchP50 = d.Quantile(0.5)
				}
			}
		}
		if bh, ok1 := bef.Histogram("llm_latency_seconds", nil); ok1 {
			if ah, ok2 := aft.Histogram("llm_latency_seconds", nil); ok2 {
				d := promx.SubHistogram(bh, ah)
				if d.Count > 0 {
					m.LLMLatencyP95S = d.Quantile(0.95)
				}
			}
		}
		for _, stage := range []string{"seen", "policy", "credits", "rate_limit", "duplicate",
			"semantic", "restricted_words", "sampler", "llm", "publish"} {
			sel := map[string]string{"stage": stage}
			bh, ok1 := bef.Histogram("moderation_stage_duration_seconds", sel)
			ah, ok2 := aft.Histogram("moderation_stage_duration_seconds", sel)
			if ok1 && ok2 {
				d := promx.SubHistogram(bh, ah)
				if d.Count > 0 {
					m.StageP95S[stage] = d.Quantile(0.95)
				}
			}
		}
	}
	if window > 0 {
		m.ConsumeRate = m.ConsumedTotal / window.Seconds()
	}
}

// mergeView keeps the first instance's latency view and, when several
// moderation replicas are scraped, notes that the SLI is per-instance.
// Merging histograms across instances would be the mathematically right
// thing, and SubHistogram already supports it — but the buckets are
// identical so summing counts is exactly what the second branch does.
func mergeView(prev report.LatencyView, d promx.Histogram, had bool) report.LatencyView {
	v := report.ViewOf(d)
	if !had {
		return v
	}
	// Two instances: report the union weighted by count for the scalar
	// summaries; the bucket-derived fractions combine linearly.
	total := prev.Count + v.Count
	if total == 0 {
		return prev
	}
	w := func(a, b float64) float64 { return (a*prev.Count + b*v.Count) / total }
	return report.LatencyView{
		Count:            total,
		MeanS:            w(prev.MeanS, v.MeanS),
		P50S:             w(prev.P50S, v.P50S),
		P95S:             math.Max(prev.P95S, v.P95S),
		P99S:             math.Max(prev.P99S, v.P99S),
		P95LowerS:        math.Min(prev.P95LowerS, v.P95LowerS),
		P95UpperS:        math.Max(prev.P95UpperS, v.P95UpperS),
		FractionUnder1S:  w(prev.FractionUnder1S, v.FractionUnder1S),
		FractionUnder2S5: w(prev.FractionUnder2S5, v.FractionUnder2S5),
	}
}

// fillServices records the shared §4.5 surface of every scraped
// service, including whether kafka_consumer_lag_messages was exported
// at all.
func (r *Runner) fillServices(res *report.Result, before, after map[string]*promx.Snapshot) {
	for name, aft := range after {
		v := report.ServiceView{FailOpen: map[string]float64{}}
		v.ConsumerLagFamilyPresent = aft.Has("kafka_consumer_lag_messages")
		if bef, ok := before[name]; ok {
			delta, _ := promx.Delta(bef, aft)
			for _, s := range delta.Series("fail_open_total", nil) {
				if s.Value == 0 {
					continue
				}
				key := s.Labels["component"]
				if reason := s.Labels["reason"]; reason != "" {
					key += "/" + reason
				}
				v.FailOpen[key] += s.Value
			}
		}
		for _, s := range aft.Series("dependency_up", nil) {
			if s.Value == 0 {
				v.DependencyDown = append(v.DependencyDown, s.Labels["dependency"])
			}
		}
		sort.Strings(v.DependencyDown)
		res.Services[name] = v
	}
}

// samplerCoverage reconstructs the §7.5 table: for a run where every
// message is LLM-flag text, verdicts per content over messages sent to
// that content is the LLM coverage of that content.
func (r *Runner) samplerCoverage(sc scenario.Scenario, pop gen.Config, res *report.Result, elapsed time.Duration) any {
	type row struct {
		ContentID       string  `json:"content_id"`
		OfferedPerMin   float64 `json:"offered_msg_per_min"`
		Sent            float64 `json:"sent"`
		Verdicts        int64   `json:"llm_verdicts"`
		CoveragePercent float64 `json:"llm_coverage_percent"`
		SpecPercent     string  `json:"spec_coverage_7_5"`
	}
	total := 0.0
	for _, w := range pop.Weights {
		total += w
	}
	spec := map[int]string{}
	var rows []row
	for i, w := range pop.Weights {
		id := gen.ContentID(i)
		share := w / total
		sent := float64(res.Offered.Sent) * share
		perMin := 0.0
		if elapsed > 0 {
			perMin = sent / elapsed.Minutes()
		}
		v := res.Verdicts.ByContent[id]
		cov := 0.0
		if sent > 0 {
			cov = 100 * float64(v) / sent
		}
		rows = append(rows, row{
			ContentID: id, OfferedPerMin: perMin, Sent: sent,
			Verdicts: v, CoveragePercent: cov, SpecPercent: spec[i],
		})
	}
	// The spec's own table, for side-by-side reading.
	table := []map[string]string{
		{"traffic": "100 msg/month", "spec_coverage": "100%"},
		{"traffic": "20 msg/min", "spec_coverage": "100%"},
		{"traffic": "100 msg/min", "spec_coverage": "~30%"},
		{"traffic": "6 000 msg/min", "spec_coverage": "~0.5%"},
	}
	ceiling := pop.Weights
	_ = ceiling
	return map[string]any{
		"measured":       rows,
		"spec_table_7_5": table,
		"note": "coverage = flagged.v1 verdicts for that content / messages sent to it; the run " +
			"sends only LLM-flag text, so every message that reaches stage 9 produces a verdict",
	}
}

// evaluate applies the scenario's pass/fail criteria.
func (r *Runner) evaluate(res *report.Result, sc scenario.Scenario) {
	c := sc.Criteria

	if c.MaxSendLagP95Millis > 0 {
		got := res.Offered.SendLag.P95MS
		res.Fatal("generator kept its schedule",
			fmt.Sprintf("send lag p95 < %.0f ms", c.MaxSendLagP95Millis),
			fmt.Sprintf("%.1f ms", got), got <= c.MaxSendLagP95Millis)
	}

	if c.MinConsumeFraction > 0 {
		want := c.MinConsumeFraction
		got := 0.0
		if res.Offered.Sent > 0 {
			got = res.Moderation.ConsumedTotal / float64(res.Offered.Sent)
		}
		res.Check("moderation consumed what was produced",
			fmt.Sprintf(">= %.0f%%", 100*want), fmt.Sprintf("%.1f%%", 100*got), got >= want)
	}

	if c.MaxP95Seconds > 0 {
		switch {
		case res.Verdicts.SLILatency.Count > 0:
			// Prefer the topic-derived measurement: full resolution,
			// and measured against the ideal-clock send time, so it is
			// free of coordinated omission.
			got := res.Verdicts.SLILatency.P95MS / 1000
			res.Check("p95 end-to-end latency (N1)",
				fmt.Sprintf("< %.2f s", c.MaxP95Seconds),
				fmt.Sprintf("%.3f s (from flagged.v1)", got), got < c.MaxP95Seconds)
		case res.Moderation.E2ELatencyPresent:
			got := res.Moderation.E2ELatency.P95UpperS
			res.Check("p95 end-to-end latency (N1)",
				fmt.Sprintf("< %.2f s", c.MaxP95Seconds),
				fmt.Sprintf("<= %.2f s (SLI histogram bucket bound)", got), got <= c.MaxP95Seconds)
		default:
			res.Check("p95 end-to-end latency (N1)",
				fmt.Sprintf("< %.2f s", c.MaxP95Seconds), "no flagged messages: no SLI", false)
		}
	}

	if !c.AllowFailOpen {
		got := res.Moderation.FailOpenTotal
		res.Check("fail_open_total (§4.5: zero in steady state)",
			fmt.Sprintf("<= %.0f", c.MaxFailOpen), strconv.FormatFloat(got, 'f', 0, 64),
			got <= c.MaxFailOpen)
	}

	if c.MaxFinalLag > 0 {
		got := res.Kafka.FinalLag
		res.Check("backlog drained after the run",
			fmt.Sprintf("final lag <= %d", c.MaxFinalLag),
			strconv.FormatInt(got, 10), got <= c.MaxFinalLag)
	}

	if c.MaxLagSlope > 0 {
		got := res.Kafka.LagSlopePerS
		res.Check("lag not growing without bound (§4.7)",
			fmt.Sprintf("slope <= %.0f msg/s", c.MaxLagSlope),
			fmt.Sprintf("%+.1f msg/s", got), got <= c.MaxLagSlope)
	}
}
