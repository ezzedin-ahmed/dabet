// Command dabet-load runs the Dabet load and stress scenarios against
// a live local stack and writes a machine-readable result plus a
// readable summary.
//
//	dabet-load -list
//	dabet-load -scenario selfbench
//	dabet-load -scenario baseline -out results/
//	dabet-load -scenario baseline -rate 4000 -duration 60s
//	dabet-load -scenario failopen            # needs docker
//
// Everything it needs from the stack is reachable on the host ports
// published by deploy/compose/docker-compose.yml; see -help for the
// overrides.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"dabet/test/load/internal/promx"
	"dabet/test/load/internal/report"
	"dabet/test/load/internal/runner"
	"dabet/test/load/internal/scenario"
	"dabet/test/load/internal/sched"
	"dabet/test/load/internal/setup"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	var (
		list        = flag.Bool("list", false, "list the scenario catalogue and exit")
		names       = flag.String("scenario", "baseline", "comma-separated scenarios to run, or 'all'")
		brokers     = flag.String("brokers", env("KAFKA_BROKERS", "localhost:9092"), "Kafka bootstrap brokers")
		outDir      = flag.String("out", "", "directory for the JSON results (default: stdout only)")
		rate        = flag.Float64("rate", 0, "override the scenario's steady rate, msg/s")
		duration    = flag.Duration("duration", 0, "override the scenario's duration")
		workers     = flag.Int("workers", 0, "override the number of send goroutines")
		group       = flag.String("group", env("MOD_CONSUMER_GROUP", "moderation-service"), "moderation consumer group, for lag sampling")
		project     = flag.String("compose-project", "dabet", "compose project name, for the drills")
		dryDrills   = flag.Bool("dry-drills", false, "log the fault injections without executing them")
		noSelfBench = flag.Bool("no-selfbench", false, "skip the generator self-benchmark that precedes a run")
		userURL     = flag.String("user-url", env("LOAD_USER_URL", "http://localhost:8081"), "user-service base URL")
		creditsURL  = flag.String("credits-url", env("LOAD_CREDITS_URL", "http://localhost:8082"), "credits-service base URL")
		policyURL   = flag.String("policy-url", env("LOAD_POLICY_URL", "http://localhost:8083"), "policy-service base URL")
		adapterURL  = flag.String("adapter-url", env("LOAD_ADAPTER_URL", "http://localhost:8084"), "provider-adapter base URL")
		stripe      = flag.String("stripe-secret", env("STRIPE_WEBHOOK_SECRET", "whsec_local_dev"), "stripe webhook secret, for the credit top-up")
		modMetrics  = flag.String("moderation-metrics", env("LOAD_MODERATION_METRICS",
			"http://localhost:9085"), "comma-separated moderation-service metrics URLs (one per replica)")
	)
	flag.Parse()

	if *list {
		for _, s := range scenario.Builtin() {
			fmt.Printf("%-12s %s\n             %s\n\n", s.Name, s.Proves, s.Hypothesis)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	selected, err := selectScenarios(*names)
	if err != nil {
		fail(err)
	}

	targets := []promx.Target{
		{Name: "user-service", URL: env("LOAD_USER_METRICS", "http://localhost:9081")},
		{Name: "credits-service", URL: env("LOAD_CREDITS_METRICS", "http://localhost:9082")},
		{Name: "policy-service", URL: env("LOAD_POLICY_METRICS", "http://localhost:9083")},
		{Name: "provider-adapter", URL: env("LOAD_ADAPTER_METRICS", "http://localhost:9084")},
		{Name: "review-service", URL: env("LOAD_REVIEW_METRICS", "http://localhost:9086")},
		{Name: "insights-service", URL: env("LOAD_INSIGHTS_METRICS", "http://localhost:9087")},
	}
	for i, u := range strings.Split(*modMetrics, ",") {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		name := "moderation-service"
		if i > 0 {
			name = fmt.Sprintf("moderation-service-%d", i+1)
		}
		targets = append(targets, promx.Target{Name: name, URL: u})
	}

	opt := runner.Options{
		Brokers: strings.Split(*brokers, ","),
		Endpoints: setup.Endpoints{
			User: *userURL, Credits: *creditsURL, Policy: *policyURL,
		},
		AdapterURL:     *adapterURL,
		StripeSecret:   *stripe,
		Targets:        targets,
		ConsumerGroup:  *group,
		ComposeProject: *project,
		DryRunDrills:   *dryDrills,
		Log:            os.Stderr,
	}

	runID := strconv.FormatInt(time.Now().Unix()%1_000_000, 36)

	// Establish the generator ceiling first, unless asked not to: every
	// scenario's report states its achieved rate against it, and a run
	// without that number cannot claim it measured the system.
	if !*noSelfBench && !onlySelfBench(selected) {
		sb, err := scenario.ByName("selfbench")
		if err == nil {
			r := runner.New(opt)
			res, err := r.Run(ctx, sb, runID+"b")
			if err == nil {
				opt.GeneratorCeiling = res.Offered.AchievedRate
				fmt.Fprintf(os.Stderr, "generator ceiling: %.0f msg/s\n", opt.GeneratorCeiling)
			}
		}
	}

	run := runner.New(opt)
	anyFail := false
	for _, sc := range selected {
		sc = applyOverrides(sc, *rate, *duration, *workers)
		fmt.Fprintf(os.Stderr, "\n=== %s ===\n", sc.Name)
		res, err := run.Run(ctx, sc, runID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "scenario %s: %v\n", sc.Name, err)
			anyFail = true
			continue
		}
		res.WriteTable(os.Stdout)
		if *outDir != "" {
			if err := writeJSON(*outDir, sc.Name, runID, res); err != nil {
				fmt.Fprintf(os.Stderr, "write result: %v\n", err)
			}
		}
		if !res.Pass {
			anyFail = true
		}
		if ctx.Err() != nil {
			break
		}
	}
	if anyFail {
		os.Exit(1)
	}
}

func onlySelfBench(ss []scenario.Scenario) bool {
	for _, s := range ss {
		if s.Mode != scenario.ModeSelfBench {
			return false
		}
	}
	return true
}

func selectScenarios(names string) ([]scenario.Scenario, error) {
	if names == "all" {
		return scenario.Builtin(), nil
	}
	var out []scenario.Scenario
	for _, n := range strings.Split(names, ",") {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		s, err := scenario.ByName(n)
		if err != nil {
			return nil, fmt.Errorf("%w (have: %s)", err, strings.Join(scenario.Names(), ", "))
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no scenario selected")
	}
	return out, nil
}

// applyOverrides rescales a scenario's profile. A rate override scales
// every segment by the same factor, so a staircase stays a staircase
// and its peak becomes the requested rate.
func applyOverrides(sc scenario.Scenario, rate float64, dur time.Duration, workers int) scenario.Scenario {
	if workers > 0 {
		sc.Workers = workers
	}
	if rate > 0 {
		peak := sc.Profile.PeakRate()
		if peak > 0 {
			f := rate / peak
			segs := make([]sched.Segment, len(sc.Profile.Segments))
			for i, s := range sc.Profile.Segments {
				segs[i] = sched.Segment{Duration: s.Duration, From: s.From * f, To: s.To * f}
			}
			sc.Profile = sched.Profile{Segments: segs}
			sc.ProfileName = fmt.Sprintf("%s (rescaled to peak %.0f msg/s)", sc.ProfileName, rate)
		}
	}
	if dur > 0 {
		total := sc.Profile.Duration()
		if total > 0 {
			f := float64(dur) / float64(total)
			segs := make([]sched.Segment, len(sc.Profile.Segments))
			for i, s := range sc.Profile.Segments {
				segs[i] = sched.Segment{
					Duration: time.Duration(float64(s.Duration) * f),
					From:     s.From, To: s.To,
				}
			}
			sc.Profile = sched.Profile{Segments: segs}
			sc.ProfileName = fmt.Sprintf("%s (rescaled to %s)", sc.ProfileName, dur)
		}
	}
	return sc
}

func writeJSON(dir, name, runID string, res *report.Result) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%s.json", name, runID))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := res.WriteJSON(f); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s\n", path)
	return nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(2)
}
