// Package drill executes the §4.7 fail-open faults by driving docker
// directly.
//
// This is the one part of the harness that requires the local compose
// stack and cannot run anywhere else: there is no in-process way to
// take Redis away from moderation-service, and a mock that pretends to
// be a broken Redis would be testing the mock. `docker stop` is the
// honest fault.
package drill

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Event is one executed fault, recorded for the run report.
type Event struct {
	At        time.Time `json:"at"`
	OffsetS   float64   `json:"offset_s"`
	Container string    `json:"container"`
	Action    string    `json:"action"`
	Expect    string    `json:"expect,omitempty"`
	Note      string    `json:"note,omitempty"`
	Err       string    `json:"error,omitempty"`
	TookMS    float64   `json:"took_ms"`
}

// Runner executes docker commands against the compose project.
type Runner struct {
	// Project is the compose project name; deploy/compose sets
	// `name: dabet`, so containers are dabet-<service>-1.
	Project string
	// DryRun logs what would happen without touching anything.
	DryRun bool

	mu     sync.Mutex
	events []Event
	// stopped tracks what this runner took down, so Restore can put
	// everything back even if the run is interrupted.
	stopped map[string]bool
}

// New builds a drill runner.
func New(project string, dryRun bool) *Runner {
	if project == "" {
		project = "dabet"
	}
	return &Runner{Project: project, DryRun: dryRun, stopped: map[string]bool{}}
}

// Container maps a compose service name to its container name.
func (r *Runner) Container(service string) string {
	return fmt.Sprintf("%s-%s-1", r.Project, service)
}

// Do executes one action (stop or start) against a compose service.
func (r *Runner) Do(ctx context.Context, start time.Time, service, action, expect, note string) Event {
	ev := Event{
		At: time.Now(), OffsetS: time.Since(start).Seconds(),
		Container: service, Action: action, Expect: expect, Note: note,
	}
	t0 := time.Now()
	if !r.DryRun {
		cmd := exec.CommandContext(ctx, "docker", action, r.Container(service))
		out, err := cmd.CombinedOutput()
		if err != nil {
			ev.Err = fmt.Sprintf("%v: %s", err, strings.TrimSpace(string(out)))
		}
	}
	ev.TookMS = float64(time.Since(t0)) / float64(time.Millisecond)

	r.mu.Lock()
	switch action {
	case "stop":
		if ev.Err == "" {
			r.stopped[service] = true
		}
	case "start":
		delete(r.stopped, service)
	}
	r.events = append(r.events, ev)
	r.mu.Unlock()
	return ev
}

// Restore starts anything this runner stopped and did not restart. It
// is deferred by the runner so an aborted drill does not leave the
// machine with half a stack.
func (r *Runner) Restore(ctx context.Context) {
	r.mu.Lock()
	var pending []string
	for s := range r.stopped {
		pending = append(pending, s)
	}
	r.mu.Unlock()
	for _, s := range pending {
		r.Do(ctx, time.Now(), s, "start", "", "restore after run")
	}
}

// Events returns the timeline.
func (r *Runner) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

// Available reports whether docker can be driven at all, so a run
// without it fails fast and says why instead of silently skipping the
// faults and reporting a green drill.
func Available(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker not usable (%v): %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}
