package promx

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Target is one scrapeable service.
type Target struct {
	Name string `json:"name"`
	URL  string `json:"url"` // base, /metrics is appended
}

// Scraper fetches a set of targets concurrently.
type Scraper struct {
	Targets []Target
	Client  *http.Client
}

// NewScraper builds a scraper with a short timeout: a service that
// cannot answer /metrics within a couple of seconds under load is
// itself a finding, and blocking the sampler on it would distort the
// time series the sampler exists to record.
func NewScraper(targets []Target) *Scraper {
	return &Scraper{Targets: targets, Client: &http.Client{Timeout: 5 * time.Second}}
}

// ScrapeOne fetches and parses a single target.
func (s *Scraper) ScrapeOne(ctx context.Context, t Target) (*Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("scrape %s: status %d", t.Name, resp.StatusCode)
	}
	return Parse(t.Name, time.Now(), io.LimitReader(resp.Body, 32<<20))
}

// ScrapeAll fetches every target. Failures are returned per target
// rather than aborting: killing one service mid-run is a scenario
// (§4.7 drills), not an error.
func (s *Scraper) ScrapeAll(ctx context.Context) (map[string]*Snapshot, map[string]error) {
	var mu sync.Mutex
	snaps := make(map[string]*Snapshot, len(s.Targets))
	errs := make(map[string]error)
	var wg sync.WaitGroup
	for _, t := range s.Targets {
		wg.Add(1)
		go func(t Target) {
			defer wg.Done()
			snap, err := s.ScrapeOne(ctx, t)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs[t.Name] = err
				return
			}
			snaps[t.Name] = snap
		}(t)
	}
	wg.Wait()
	return snaps, errs
}
