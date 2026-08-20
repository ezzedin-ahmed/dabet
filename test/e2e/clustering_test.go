//go:build e2e && e2e_full

// Area D's second half: live centroid assignment (§8.5), batch clustering
// (§8.6) and the topics API (§8.8). None of it runs in the default profile —
// clustering-service and clusters-job need etcd and Milvus, and Milvus wants
// several GB — so this file carries its own build tag and `make e2e` never
// compiles it. Run it against `make up-full` plus the e2e-extra overlay:
//
//	make up-full-e2e && make e2e-full
//
// The fixture is what makes any of this reachable. Batch clustering needs a
// corpus with actual structure, and mockembed's default hash embedding gives
// every distinct string its own near-orthogonal direction — a corpus of pure
// noise, which HDBSCAN correctly refuses to cluster. The similarity markers
// (see tools/mockembed) build three topics of two themes each instead, tight
// enough to be found and far enough apart to be told apart.
package e2e

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	clusterHealthTimeout = 120 * time.Second
	embedTimeout         = 180 * time.Second
	clustersJobTimeout   = 240 * time.Second
	reclusterTimeout     = 240 * time.Second
)

// The fixture's shape. Six (topic, theme) pairs across three topics.
//
// A topic's members sit at 0.99 from their theme centroid and each theme
// centroid at 0.90 from the topic centroid, so within a theme the cosine is
// ~0.98, across themes of one topic ~0.79, and across topics ~0. That is a
// topic that visibly subdivides — the input §8.6's two HDBSCAN passes are
// built for — rather than one undifferentiated blob.
var clusterFixture = []struct {
	topic string
	theme string
	label string // pinned through mockllm's [[label:…]] marker
	text  string
}{
	{"tickets", "resale", "Ticket resale", "anyone reselling a spare ticket for tonight"},
	{"tickets", "queue", "Ticket resale", "the queue for tickets is completely stuck again"},
	{"merch", "hoodies", "Merch drop", "is the new hoodie restocked in medium yet"},
	{"merch", "posters", "Merch drop", "the tour poster print quality looks incredible"},
	{"speedrun", "glitches", "Speedrun talk", "that wall clip skips the entire second act"},
	{"speedrun", "records", "Speedrun talk", "world record fell again this morning by four seconds"},
}

// Contents are separate because insights-service samples per content at 60
// messages/minute (§8.4). A single content would silently drop everything
// past the sixtieth message as insights_messages_dropped_total{reason=sampled}
// and the corpus would never reach the bootstrap threshold.
const (
	seedContents       = 6
	seedPerContent     = 40 // 240 messages: comfortably past CLUSTERS_BOOTSTRAP_MIN=100
	followUpPerFixture = 5  // second batch, once centroids exist
	clusterAssignFloor = 0.75
)

type clusterScenario struct {
	*scenario

	// The hour the corpus was seeded in. The counts backfill can only rewrite
	// hours that have closed, so whether this equals the hour the recluster
	// runs in decides what step (i) is able to assert.
	seededHour time.Time

	// Metric baselines, per service, taken before the batch they describe.
	clusteringBefore []sample
	jobBefore        []sample

	topics []topicItem

	// The creator that must not be able to see any of it.
	otherToken string
}

func TestClusteringAndTopics(t *testing.T) {
	s := &clusterScenario{scenario: newScenario("cluster")}
	waitHealthy(t, clusterHealthTimeout)
	waitClusteringHealthy(t, clusterHealthTimeout)

	steps := []struct {
		name string
		fn   func(*testing.T, *clusterScenario)
	}{
		{"a_creator_with_credits_and_a_permissive_policy", stepClusterBootstrap},
		{"b_cold_creator_embeddings_are_unassigned", stepColdAssignment},
		{"c_bootstrap_run_discovers_topics", stepBootstrapRun},
		{"d_topics_are_labelled_in_clickhouse", stepTopicsInClickHouse},
		{"e_topics_api_lists_them_for_the_owner", stepTopicsAPI},
		{"f_live_assignment_transitions_to_assigned", stepAssignmentTransition},
		{"g_topic_detail_and_themes", stepTopicDetailAndThemes},
		{"h_another_creator_gets_404", stepTopicOwnership},
		{"i_recluster_emits_usage_and_rewrites_counts", stepRecluster},
	}
	for _, step := range steps {
		if !t.Run(step.name, func(t *testing.T) { step.fn(t, s) }) {
			t.Fatalf("step %s failed; the remaining steps depend on it", step.name)
		}
	}
}

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

// waitClusteringHealthy blocks until the two profile-only services answer.
// waitHealthy deliberately does not know about them, because they are absent
// from the default profile.
func waitClusteringHealthy(t *testing.T, timeout time.Duration) {
	t.Helper()
	poll(t, timeout, "clusters-job /healthz", func() error {
		resp, err := client.Get(clustersJobURL + "/healthz")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		return nil
	})
	poll(t, timeout, "clustering-service /metrics", func() error {
		_, err := scrape(clusteringMetricsURL)
		return err
	})
}

// fixtureText builds one message: human wording, the label mockllm should
// return for the cluster, and the marker that puts the vector in the right
// place. variant makes every message's residual distinct so a theme is a
// cloud of points rather than one repeated point.
func fixtureText(i int, variant string) string {
	f := clusterFixture[i]
	return fmt.Sprintf("%s %s [[label:%s]] [[sim:%s/%s:0.90/0.99:%s]]",
		f.text, variant, f.label, f.topic, f.theme, variant)
}

// injectTo posts one message on an explicit content. The scenario's own
// inject is pinned to a single channel, and this suite needs several.
func injectTo(t *testing.T, s *scenario, channel, author, text string) {
	t.Helper()
	mustStatus(t, do(t, client, http.MethodPost, adapterURL+"/mock/messages", "", map[string]string{
		"creator_id": s.creatorID,
		"channel":    channel,
		"author":     author,
		"text":       text,
	}), http.StatusAccepted, "inject into "+channel)
}

// chQuery runs one SQL statement against ClickHouse over HTTP and returns the
// rows as decoded JSON objects. No test has queried ClickHouse before, so this
// is the whole client: the two tables involved are read-only from here.
func chQuery(t *testing.T, sql string) []map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, clickhouseURL+"/?database="+clickhouseDB,
		strings.NewReader(sql+" FORMAT JSONEachRow"))
	if err != nil {
		t.Fatalf("build clickhouse request: %v", err)
	}
	req.Header.Set("X-ClickHouse-User", clickhouseUser)
	req.Header.Set("X-ClickHouse-Key", clickhousePassword)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("clickhouse query: %v", err)
	}
	defer resp.Body.Close()
	r := readResponse(t, resp)
	if r.status != http.StatusOK {
		t.Fatalf("clickhouse query failed (%d): %s\nsql: %s", r.status, truncate(r.body), sql)
	}
	var out []map[string]any
	for line := range strings.Lines(string(r.body)) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var row map[string]any
		response{body: []byte(line)}.json(t, &row)
		out = append(out, row)
	}
	return out
}

// chExec runs a statement that returns nothing.
func chExec(t *testing.T, sql string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, clickhouseURL+"/?database="+clickhouseDB,
		strings.NewReader(sql))
	if err != nil {
		t.Fatalf("build clickhouse request: %v", err)
	}
	req.Header.Set("X-ClickHouse-User", clickhouseUser)
	req.Header.Set("X-ClickHouse-Key", clickhousePassword)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("clickhouse exec: %v", err)
	}
	defer resp.Body.Close()
	if r := readResponse(t, resp); r.status != http.StatusOK {
		t.Fatalf("clickhouse exec failed (%d): %s\nsql: %s", r.status, truncate(r.body), sql)
	}
}

// ---------------------------------------------------------------------
// (a) a creator whose messages will be moderated and then embedded
// ---------------------------------------------------------------------

func stepClusterBootstrap(t *testing.T, s *clusterScenario) {
	bootstrapCreator(t, s.scenario)

	// A permissive policy. Insights consumes messages.v1 directly, so a
	// policy is not strictly required — but without one every message counts
	// as a policy miss, and without credits every message counts as
	// fail_open_total{reason="no_credits"}, both of which would leave the
	// shared stack in a state that makes the default suite's §4.5 health
	// assertion fail for a later run.
	doc := map[string]any{
		"scope":               "creator",
		"scope_id":            s.creatorID,
		"rate_limit_messages": 600,
		"rate_limit_seconds":  60,
		"spam":                "none",
		"restricted_words":    []string{},
	}
	mustStatus(t, do(t, client, http.MethodPost, policyURL+"/v1/policies", s.token, doc),
		http.StatusCreated, "create permissive policy")
}

// ---------------------------------------------------------------------
// (b) §8.5: a cold creator's embeddings are unassigned, and that is normal
// ---------------------------------------------------------------------

func stepColdAssignment(t *testing.T, s *clusterScenario) {
	s.clusteringBefore = mustScrape(t, "clustering-service", clusteringMetricsURL)
	s.jobBefore = mustScrape(t, "clusters-job", clustersJobMetricsCL)
	s.seededHour = time.Now().UTC().Truncate(time.Hour)

	seeded := 0
	for c := range seedContents {
		channel := fmt.Sprintf("%s-c%d", s.channel, c)
		for i := range seedPerContent {
			f := i % len(clusterFixture)
			variant := fmt.Sprintf("c%d-m%d", c, i)
			// A distinct author per message keeps the per-(content, author)
			// rate limiter and duplicate ring out of the way entirely.
			injectTo(t, s.scenario, channel, fmt.Sprintf("viewer-%d-%d", c, i),
				fixtureText(f, variant))
			seeded++
		}
	}
	t.Logf("seeded %d messages across %d contents", seeded, seedContents)

	// §8.5: "A cold creator whose clusters have never been built has
	// everything unassigned until the first clusters-job run." Milvus holds
	// no centroid for this creator, so every one of these must come back
	// unassigned — and crucially, must come back at all.
	poll(t, embedTimeout, "the seeded corpus to reach clustering-service", func() error {
		after := mustScrape(t, "clustering-service", clusteringMetricsURL)
		n := metricDelta(s.clusteringBefore, after, "clustering_assignments_total",
			map[string]string{"result": "unassigned"})
		if n < float64(seeded)*0.8 {
			return fmt.Errorf("clustering_assignments_total{result=unassigned} moved by %g of %d",
				n, seeded)
		}
		return nil
	})

	after := mustScrape(t, "clustering-service", clusteringMetricsURL)
	for _, result := range []string{"topic", "theme"} {
		if n := metricDelta(s.clusteringBefore, after, "clustering_assignments_total",
			map[string]string{"result": result}); n != 0 {
			t.Errorf("clustering_assignments_total{result=%q} moved by %g for a creator with "+
				"no centroids in Milvus; unassigned is the only possible outcome", result, n)
		}
	}
	// Nothing degraded on the way: a Milvus search failure also produces no
	// assignment, and would make the unassigned count above meaningless.
	if n := metricDelta(s.clusteringBefore, after, "fail_open_total",
		map[string]string{"component": "milvus"}); n != 0 {
		t.Fatalf("fail_open_total{component=milvus} moved by %g; the assignments above were "+
			"dropped searches, not genuine below-threshold results", n)
	}
}

// ---------------------------------------------------------------------
// (c) §8.6: the bootstrap trigger
// ---------------------------------------------------------------------

func stepBootstrapRun(t *testing.T, s *clusterScenario) {
	// The trigger is "first 100 messages for a creator", counted from the S3
	// listing, so the parquet has to be rolled first (compose sets
	// INSIGHTS_S3_ROLL_SECONDS=10) and then a sweep has to come round.
	poll(t, clustersJobTimeout, "a successful bootstrap clusters-job run", func() error {
		after := mustScrape(t, "clusters-job", clustersJobMetricsCL)
		if n := metricDelta(s.jobBefore, after, "clusters_job_runs_total",
			map[string]string{"trigger": "bootstrap", "outcome": "ok"}); n < 1 {
			failed := metricDelta(s.jobBefore, after, "clusters_job_runs_total",
				map[string]string{"outcome": "error"})
			return fmt.Errorf("bootstrap runs = %g (failed runs = %g)", n, failed)
		}
		return nil
	})

	after := mustScrape(t, "clusters-job", clustersJobMetricsCL)
	if n := metricDelta(s.jobBefore, after, "clusters_job_runs_total",
		map[string]string{"outcome": "error"}); n != 0 {
		t.Errorf("clusters_job_runs_total{outcome=error} moved by %g", n)
	}
	if n := metricDelta(s.jobBefore, after, "clusters_job_duration_seconds_count",
		map[string]string{"trigger": "bootstrap"}); n < 1 {
		t.Errorf("clusters_job_duration_seconds{trigger=bootstrap} has %g observations, want at least 1", n)
	}
}

// ---------------------------------------------------------------------
// (d) §8.7: labelled topic rows landed in ClickHouse
// ---------------------------------------------------------------------

func stepTopicsInClickHouse(t *testing.T, s *clusterScenario) {
	var rows []map[string]any
	poll(t, clustersJobTimeout, "topic rows for the creator in ClickHouse", func() error {
		rows = chQuery(t, fmt.Sprintf(
			`SELECT toString(topic_id) AS topic_id, toString(parent_id) AS parent_id,
			        label, description, version
			 FROM topics FINAL WHERE creator_id = toUUID('%s')`, s.creatorID))
		if len(rows) == 0 {
			return fmt.Errorf("no topic rows yet")
		}
		return nil
	})

	const zeroUUID = "00000000-0000-0000-0000-000000000000"
	var topics, themes int
	labels := map[string]int{}
	for _, r := range rows {
		label, _ := r["label"].(string)
		if strings.TrimSpace(label) == "" {
			t.Errorf("topic %v has an empty label; §8.6 labels every cluster", r["topic_id"])
		}
		labels[label]++
		if r["parent_id"] == zeroUUID {
			topics++
		} else {
			themes++
		}
	}
	t.Logf("clickhouse topics: %d topic row(s), %d theme row(s), labels %v", topics, themes, labels)
	if topics == 0 {
		t.Fatal("no topic-level rows (every row has a parent); §8.6's coarse pass found nothing")
	}

	// The labels came from the LLM, not from a fallback: every one of them
	// must be a label the fixture pinned through mockllm's marker. A generic
	// placeholder here would mean the labelling round trip failed and the
	// run fell back to §8.6's "generic label" path.
	want := map[string]bool{}
	for _, f := range clusterFixture {
		want[f.label] = true
	}
	for label := range labels {
		if !want[label] {
			t.Errorf("topic label %q is not one the fixture pinned %v; labelling fell back "+
				"rather than running", label, want)
		}
	}
}

// ---------------------------------------------------------------------
// (e) §8.8: GET /v1/topics
// ---------------------------------------------------------------------

type topicSeriesPoint struct {
	Bucket string `json:"bucket"`
	Count  int64  `json:"count"`
}

type topicItem struct {
	ID           string             `json:"id"`
	Label        string             `json:"label"`
	Description  string             `json:"description"`
	MessageCount int64              `json:"message_count"`
	Series       []topicSeriesPoint `json:"series"`
}

func listTopics(t *testing.T, token, query string) []topicItem {
	t.Helper()
	var out struct {
		Items []topicItem `json:"items"`
	}
	mustStatus(t, do(t, client, http.MethodGet, insightsURL+"/v1/topics"+query, token, nil),
		http.StatusOK, "GET /v1/topics").json(t, &out)
	return out.Items
}

func stepTopicsAPI(t *testing.T, s *clusterScenario) {
	poll(t, clustersJobTimeout, "GET /v1/topics to return the discovered topics", func() error {
		s.topics = listTopics(t, s.token, "")
		if len(s.topics) == 0 {
			return fmt.Errorf("no topics returned yet")
		}
		return nil
	})
	t.Logf("GET /v1/topics returned %d topic(s)", len(s.topics))

	for _, it := range s.topics {
		if it.ID == "" || strings.TrimSpace(it.Label) == "" {
			t.Errorf("topic item incomplete: %+v", it)
		}
	}
	// Ordered by volume over the window (§8.8).
	for i := 1; i < len(s.topics); i++ {
		if s.topics[i-1].MessageCount < s.topics[i].MessageCount {
			t.Errorf("topics are not ordered by volume: %d before %d",
				s.topics[i-1].MessageCount, s.topics[i].MessageCount)
		}
	}
	// The API is JWT-scoped, not open.
	if r := do(t, client, http.MethodGet, insightsURL+"/v1/topics", "", nil); r.status != http.StatusUnauthorized {
		t.Errorf("GET /v1/topics without a token: status %d, want 401", r.status)
	}
	// §8.8's parameters are validated rather than ignored.
	for _, q := range []string{"?granularity=fortnight", "?from=yesterday", "?to=nope"} {
		if r := do(t, client, http.MethodGet, insightsURL+"/v1/topics"+q, s.token, nil); r.status != http.StatusBadRequest {
			t.Errorf("GET /v1/topics%s: status %d, want 400", q, r.status)
		}
	}
	for _, g := range []string{"hour", "day", "month"} {
		mustStatus(t, do(t, client, http.MethodGet, insightsURL+"/v1/topics?granularity="+g, s.token, nil),
			http.StatusOK, "GET /v1/topics?granularity="+g)
	}
}

// ---------------------------------------------------------------------
// (f) §8.5: the transition unassigned -> assigned, now that centroids exist
// ---------------------------------------------------------------------

func stepAssignmentTransition(t *testing.T, s *clusterScenario) {
	before := mustScrape(t, "clustering-service", clusteringMetricsURL)

	// A fresh content, so insights' per-content sampler starts full.
	channel := s.channel + "-followup"
	sent := 0
	for i := range clusterFixture {
		for j := range followUpPerFixture {
			injectTo(t, s.scenario, channel, fmt.Sprintf("viewer-follow-%d-%d", i, j),
				fixtureText(i, fmt.Sprintf("follow-%d-%d", i, j)))
			sent++
		}
	}

	// The same vectors that were unassigned before the bootstrap run must now
	// land on a centroid: this is the transition, and it is only possible if
	// clusters-job really wrote centroids into Milvus and clustering-service
	// really searches them.
	poll(t, embedTimeout, "the follow-up batch to be assigned to topics", func() error {
		after := mustScrape(t, "clustering-service", clusteringMetricsURL)
		assigned := metricDelta(before, after, "clustering_assignments_total",
			map[string]string{"result": "topic"}) +
			metricDelta(before, after, "clustering_assignments_total",
				map[string]string{"result": "theme"})
		if assigned < 1 {
			unassigned := metricDelta(before, after, "clustering_assignments_total",
				map[string]string{"result": "unassigned"})
			return fmt.Errorf("assigned %g of %d (unassigned %g)", assigned, sent, unassigned)
		}
		return nil
	})

	after := mustScrape(t, "clustering-service", clusteringMetricsURL)
	assignedTopic := metricDelta(before, after, "clustering_assignments_total",
		map[string]string{"result": "topic"})
	assignedTheme := metricDelta(before, after, "clustering_assignments_total",
		map[string]string{"result": "theme"})
	unassigned := metricDelta(before, after, "clustering_assignments_total",
		map[string]string{"result": "unassigned"})
	t.Logf("follow-up batch of %d: topic=%g theme=%g unassigned=%g",
		sent, assignedTopic, assignedTheme, unassigned)

	// The fixture puts every follow-up message at ~0.96 from its topic
	// centroid, well above the 0.75 threshold, so a large unassigned residue
	// would mean the centroids do not describe the corpus they were built
	// from.
	if assignedTopic+assignedTheme < float64(sent)/2 {
		t.Errorf("only %g of %d follow-up messages were assigned; the centroids built from "+
			"this very corpus should match it above the %.2f threshold",
			assignedTopic+assignedTheme, sent, clusterAssignFloor)
	}

	// The counts those assignments produced are what gives §8.8 a series.
	poll(t, clustersJobTimeout, "the topics API to report a non-empty series", func() error {
		s.topics = listTopics(t, s.token, "")
		for _, it := range s.topics {
			if it.MessageCount > 0 && len(it.Series) > 0 {
				return nil
			}
		}
		return fmt.Errorf("every topic still has message_count 0 and an empty series")
	})

	var total int64
	for _, it := range s.topics {
		var seriesSum int64
		for _, p := range it.Series {
			if p.Count < 0 {
				t.Errorf("topic %s has a negative bucket count: %+v", it.ID, p)
			}
			if _, err := time.Parse(time.RFC3339, p.Bucket); err != nil {
				t.Errorf("topic %s bucket %q is not RFC 3339: %v", it.ID, p.Bucket, err)
			}
			seriesSum += p.Count
		}
		if len(it.Series) > 0 && seriesSum != it.MessageCount {
			t.Errorf("topic %s: series sums to %d but message_count is %d",
				it.ID, seriesSum, it.MessageCount)
		}
		total += it.MessageCount
	}
	t.Logf("topics API reports %d assigned message(s) across %d topic(s)", total, len(s.topics))
	if total <= 0 {
		t.Fatal("no topic carries a positive message_count")
	}
}

// ---------------------------------------------------------------------
// (g) §8.8: the detail and themes routes
// ---------------------------------------------------------------------

func stepTopicDetailAndThemes(t *testing.T, s *clusterScenario) {
	if len(s.topics) == 0 {
		t.Fatal("no topics to inspect")
	}
	target := s.topics[0]

	var got topicItem
	mustStatus(t, do(t, client, http.MethodGet, insightsURL+"/v1/topics/"+target.ID, s.token, nil),
		http.StatusOK, "GET /v1/topics/{id}").json(t, &got)
	if got.ID != target.ID || got.Label != target.Label {
		t.Errorf("GET /v1/topics/{id} = %+v, want the list entry %+v", got, target)
	}

	// §8.8 has no sample or message-retrieval route, and the detail object
	// must not smuggle text in: a topic is a shape and a count.
	raw := do(t, client, http.MethodGet, insightsURL+"/v1/topics/"+target.ID, s.token, nil)
	for _, f := range clusterFixture {
		if strings.Contains(string(raw.body), f.text) {
			t.Fatalf("GET /v1/topics/{id} echoes message text (%q); topics are not backed by "+
				"stored text (§8.1)", f.text)
		}
	}
	if r := do(t, client, http.MethodGet, insightsURL+"/v1/topics/"+target.ID+"/samples", s.token, nil); r.status == http.StatusOK {
		t.Error("a /samples route answered 200; §8.8 removed sample endpoints entirely")
	}

	// Themes. Which topics subdivide is a property of the data and of
	// HDBSCAN's second pass — §8.6 is explicit that "a topic that does not
	// subdivide has no themes" — so the route is exercised for every topic
	// and the assertions are on shape, not on a particular split.
	totalThemes := 0
	seen := map[string]bool{}
	for _, topic := range s.topics {
		var themes struct {
			Items []topicItem `json:"items"`
		}
		mustStatus(t, do(t, client, http.MethodGet, insightsURL+"/v1/topics/"+topic.ID+"/themes", s.token, nil),
			http.StatusOK, "GET /v1/topics/"+topic.ID+"/themes").json(t, &themes)
		totalThemes += len(themes.Items)
		for _, th := range themes.Items {
			if th.ID == "" || strings.TrimSpace(th.Label) == "" {
				t.Errorf("theme item incomplete: %+v", th)
			}
			if th.ID == topic.ID {
				t.Errorf("theme %s is its own parent topic", th.ID)
			}
			// A theme belongs to exactly one topic; the same id must not be
			// returned under two parents.
			if seen[th.ID] {
				t.Errorf("theme %s is returned under more than one topic", th.ID)
			}
			seen[th.ID] = true
		}
	}
	t.Logf("themes: %d across %d topic(s)", totalThemes, len(s.topics))
	if totalThemes == 0 {
		t.Logf("NOTE: no topic subdivided into themes on this run. The route answers 200 with " +
			"an empty list, which §8.6 permits, but the fixture was built to subdivide and " +
			"the second HDBSCAN pass found nothing.")
	}

	// An id that is well-formed but unknown is a 404, not a 500.
	if r := do(t, client, http.MethodGet,
		insightsURL+"/v1/topics/00000000-0000-0000-0000-0000000000ff", s.token, nil); r.status != http.StatusNotFound {
		t.Errorf("GET an unknown topic: status %d, want 404", r.status)
	}
}

// ---------------------------------------------------------------------
// (h) another creator cannot see this creator's topics
// ---------------------------------------------------------------------

func stepTopicOwnership(t *testing.T, s *clusterScenario) {
	other := newScenario("cluster-other")
	stepAuth(t, other)
	s.otherToken = other.token

	target := s.topics[0].ID
	for _, path := range []string{"/v1/topics/" + target, "/v1/topics/" + target + "/themes"} {
		r := do(t, client, http.MethodGet, insightsURL+path, s.otherToken, nil)
		if r.status != http.StatusNotFound {
			t.Errorf("GET %s as another creator: status %d, want 404\nbody: %s",
				path, r.status, truncate(r.body))
		}
	}
	// And their own list is empty rather than someone else's.
	if items := listTopics(t, s.otherToken, ""); len(items) != 0 {
		t.Errorf("a creator with no corpus sees %d topic(s)", len(items))
	}
}

// ---------------------------------------------------------------------
// (i) §8.6: on-demand recluster — usage.v1 and the topic_counts rewrite
// ---------------------------------------------------------------------

func stepRecluster(t *testing.T, s *clusterScenario) {
	before := mustScrape(t, "clusters-job", clustersJobMetricsCL)

	// Seed history in the previous hour, attributed to a topic id that no
	// longer exists. This is exactly the state the counts backfill was added
	// to repair: a run replaced the topics but left topic_counts pointing at
	// the superseded ids.
	//
	// It has to be the *previous* hour: the backfill clamps its upper bound
	// to BucketHour(now - CLUSTERS_BACKFILL_LAG) so it can never race
	// clustering-service's live writer, and the overlay already sets the lag
	// to the smallest value the service accepts (0s). Counts for the current
	// hour — including everything this test just produced — are therefore out
	// of reach by design, which is why the rewrite is proven here on seeded
	// history rather than on the live rows.
	const staleTopic = "11111111-1111-1111-1111-111111111111"
	staleHour := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	chExec(t, fmt.Sprintf(
		`INSERT INTO topic_counts (creator_id, content_id, topic_id, theme_id, bucket_hour, count)
		 VALUES (toUUID('%s'), 'stale-content', toUUID('%s'),
		         toUUID('00000000-0000-0000-0000-000000000000'), toDateTime('%s'), 4242)`,
		s.creatorID, staleTopic, staleHour.Format("2006-01-02 15:04:05")))

	staleCount := func() int64 {
		rows := chQuery(t, fmt.Sprintf(
			`SELECT sum(count) AS n FROM topic_counts
			 WHERE creator_id = toUUID('%s') AND topic_id = toUUID('%s')`, s.creatorID, staleTopic))
		if len(rows) == 0 {
			return 0
		}
		return int64(toFloat(rows[0]["n"]))
	}
	if got := staleCount(); got != 4242 {
		t.Fatalf("seeded stale counts = %d, want 4242", got)
	}

	// A window that covers the stale hour and everything this test produced.
	to := time.Now().UTC().Add(time.Hour)
	from := to.Add(-6 * time.Hour)
	var queued struct {
		JobID string `json:"job_id"`
	}
	mustStatus(t, do(t, client, http.MethodPost, clustersJobURL+"/v1/topics/recluster", s.token,
		map[string]string{"from": from.Format(time.RFC3339), "to": to.Format(time.RFC3339)}),
		http.StatusAccepted, "POST /v1/topics/recluster").json(t, &queued)
	if queued.JobID == "" {
		t.Fatal("recluster returned no job_id")
	}

	// It is creator-scoped from the token alone and JWT-gated.
	if r := do(t, client, http.MethodPost, clustersJobURL+"/v1/topics/recluster", "",
		map[string]string{"from": from.Format(time.RFC3339), "to": to.Format(time.RFC3339)}); r.status != http.StatusUnauthorized {
		t.Errorf("unauthenticated recluster: status %d, want 401", r.status)
	}
	if r := do(t, client, http.MethodPost, clustersJobURL+"/v1/topics/recluster", s.token,
		map[string]string{"from": to.Format(time.RFC3339), "to": from.Format(time.RFC3339)}); r.status != http.StatusUnprocessableEntity && r.status != http.StatusBadRequest {
		t.Errorf("recluster with from after to: status %d, want a validation failure", r.status)
	}

	poll(t, reclusterTimeout, "the on-demand run to complete", func() error {
		after := mustScrape(t, "clusters-job", clustersJobMetricsCL)
		if n := metricDelta(before, after, "clusters_job_runs_total",
			map[string]string{"trigger": "on_demand", "outcome": "ok"}); n < 1 {
			return fmt.Errorf("on_demand ok runs = %g", n)
		}
		return nil
	})
	after := mustScrape(t, "clusters-job", clustersJobMetricsCL)

	// The counts backfill ran for this trigger and rewrote the window.
	if n := metricDelta(before, after, "clusters_job_counts_backfill_total",
		map[string]string{"trigger": "on_demand", "outcome": "ok"}); n < 1 {
		skipped := metricDelta(before, after, "clusters_job_counts_backfill_total",
			map[string]string{"trigger": "on_demand", "outcome": "skipped"})
		failed := metricDelta(before, after, "clusters_job_counts_backfill_total",
			map[string]string{"trigger": "on_demand", "outcome": "error"})
		t.Fatalf("clusters_job_counts_backfill_total{trigger=on_demand,outcome=ok} moved by %g "+
			"(skipped %g, error %g); the backfill did not run", n, skipped, failed)
	}
	deletedRows := metricDelta(before, after, "clusters_job_counts_backfill_rows_total",
		map[string]string{"trigger": "on_demand", "op": "deleted"})
	writtenRows := metricDelta(before, after, "clusters_job_counts_backfill_rows_total",
		map[string]string{"trigger": "on_demand", "op": "written"})
	t.Logf("counts backfill: %g row(s) deleted, %g row(s) written", deletedRows, writtenRows)
	if deletedRows < 1 {
		t.Errorf("the backfill deleted %g rows; the seeded stale row was inside the window "+
			"and had to be removed", deletedRows)
	}
	// The recompute half writes a row per (content, topic, theme, hour) for
	// the points it clustered — but only for hours the lag clamp lets it
	// touch. Whether this run's own corpus qualifies depends on whether an
	// hour boundary passed while the test was running, so assert it when it
	// does and say so plainly when it does not.
	if now := time.Now().UTC().Truncate(time.Hour); now.After(s.seededHour) {
		if writtenRows < 1 {
			t.Errorf("the corpus was seeded in %s and the hour has since closed, so the "+
				"backfill should have rewritten its counts, but it wrote %g rows",
				s.seededHour.Format(time.RFC3339), writtenRows)
		}
	} else if writtenRows == 0 {
		t.Logf("NOTE: the backfill wrote 0 rows. Every embedding this test produced is in the "+
			"current hour (%s), and the backfill clamps its upper bound to "+
			"BucketHour(now - CLUSTERS_BACKFILL_LAG) so it can never race clustering-service's "+
			"live writer. 0s is the smallest lag the service accepts, so within-hour data is "+
			"out of reach by construction; the delete half is asserted above and the insert "+
			"half is only reachable from a run that straddles an hour boundary.",
			s.seededHour.Format(time.RFC3339))
	}

	// The stale attribution is gone from the table itself, not merely counted.
	poll(t, 60*time.Second, "the superseded topic_counts rows to disappear", func() error {
		if n := staleCount(); n != 0 {
			return fmt.Errorf("stale topic still carries %d messages", n)
		}
		return nil
	})

	// §8.6: reclustering emits usage.v1 with messages_reclustered, and only
	// credits-service turns that into money. Reaching the ledger is the
	// end-to-end proof that the event was published and consumed.
	poll(t, reclusterTimeout, "a messages_reclustered debit in the credit ledger", func() error {
		var out struct {
			Items []creditEntry `json:"items"`
		}
		mustStatus(t, do(t, client, http.MethodGet, creditsURL+"/v1/credits/entries?limit=100", s.token, nil),
			http.StatusOK, "GET /v1/credits/entries").json(t, &out)
		for _, e := range out.Items {
			if e.Reason != "messages_reclustered" {
				continue
			}
			if e.Delta >= 0 {
				return fmt.Errorf("messages_reclustered entry has delta %d, want a debit", e.Delta)
			}
			q, ok := e.Metadata["quantity"].(float64)
			if !ok || q <= 0 {
				return fmt.Errorf("messages_reclustered entry carries no positive quantity: %v", e.Metadata)
			}
			t.Logf("credits ledger: messages_reclustered debit of %d for %g message(s)", e.Delta, q)
			return nil
		}
		return fmt.Errorf("no messages_reclustered entry among %d ledger entries", len(out.Items))
	})

	// The topics survived the rewrite: a recluster replaces labels, it does
	// not empty the dashboard.
	if items := listTopics(t, s.token, ""); len(items) == 0 {
		t.Error("GET /v1/topics is empty after a recluster")
	}
}

// toFloat coerces a ClickHouse JSON scalar, which may arrive as a number or
// as a string for 64-bit types.
func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case string:
		var f float64
		fmt.Sscanf(n, "%g", &f)
		return f
	default:
		return 0
	}
}
