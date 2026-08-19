// Package youtube is the YouTube driver (docs §7.2 table, A14).
//
// # Verified against provider documentation (2026-08-19)
//
//   - LiveChatMessages: list —
//     https://developers.google.com/youtube/v3/live/docs/liveChatMessages/list
//     (page last updated 2025-10-31). GET
//     https://www.googleapis.com/youtube/v3/liveChat/messages, required
//     liveChatId and part (id, snippet, authorDetails); optional
//     maxResults (200-2000, default 500), pageToken, profileImageSize, hl.
//     The response carries nextPageToken, pollingIntervalMillis, offlineAt
//     and items[]. Documented errors: 403 forbidden / liveChatDisabled /
//     liveChatEnded, 404 liveChatNotFound, rateLimitExceeded.
//   - LiveBroadcasts: list —
//     https://developers.google.com/youtube/v3/live/docs/liveBroadcasts/list
//     GET https://www.googleapis.com/youtube/v3/liveBroadcasts, filter
//     broadcastStatus=active with broadcastType=all (the default, "event",
//     hides persistent broadcasts), maxResults 0-50 default 5. The chat id
//     is snippet.liveChatId. Scope youtube.force-ssl covers it, matching
//     §5.5.
//
// # Where the live docs differ from §7.2
//
//  1. The list page now opens with: "To poll for live chat messages, use
//     the liveChatMessages.streamList method. The streamList method pushes
//     new messages to the client as they become available, which reduces
//     the need for constant polling and helps to avoid exceeding your
//     quota." streamList is a server-streaming endpoint that takes the same
//     parameters and is resumed with the last nextPageToken. §7.2 and the
//     brief both specify polling liveChatMessages.list, so that is what is
//     implemented here; streamList is the obvious follow-up and would slot
//     in behind the same chatPoller seam, cutting both latency and quota.
//  2. The quota-cost table
//     (https://developers.google.com/youtube/v3/determine_quota_cost, last
//     updated 2026-06-01) states that live-streaming methods "are also
//     listed in the table" — but as of that revision the table contains no
//     liveBroadcasts, liveChatMessages, liveChatBans, liveChatModerators or
//     liveStreams rows at all. The per-call costs are therefore no longer
//     documented. CostList and CostDiscover below carry the long-standing
//     community values (5 and 1) as configurable defaults rather than
//     hard-coded facts, and the budget is drained on a quotaExceeded reply
//     because the provider, not our arithmetic, is the authority.
//
// # The quota cost model
//
// A project gets 10 000 units/day by default for everything except
// search.list and videos.insert. Charging 5 units per liveChatMessages.list
// call, that is 2 000 polls/day — one poll every 43 s for a single live
// chat, and 43·N seconds when an instance watches N chats. Quota, not
// pollingIntervalMillis, is the binding constraint at any real scale, and
// the driver paces itself accordingly (quota.Budget.Pace) instead of
// polling as fast as the server permits and dying at noon. Deployments with
// a raised quota set ADAPTER_YOUTUBE_DAILY_QUOTA and get proportionally
// tighter polling; a self-hosted or quota-exempt deployment sets 0 for
// unlimited.
//
// # Why the poll delay is outside the SLI
//
// §4.6's clock starts at adapter ingress, and this is the driver that
// forced that decision. Between a viewer typing and this driver reading the
// message sits pollingIntervalMillis plus whatever extra pacing the daily
// quota imposes — tens of seconds, against a whole-pipeline budget of about
// 1.2 s. That delay is the platform's and the quota's, not the pipeline's,
// and no design of ours can shrink it, so Message.ReceivedAt is stamped the
// instant the poll response is read and everything before it is out of
// scope for moderation_e2e_latency_seconds.
//
// Delete is implemented against the documented REST endpoint:
// DELETE https://www.googleapis.com/youtube/v3/liveChat/messages?id={id}
// with the connection's user OAuth token.
package youtube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/quota"
	"dabet/services/provider-adapter/internal/retry"
)

// DefaultBaseURL is the production API root.
const DefaultBaseURL = "https://www.googleapis.com/youtube/v3"

// DefaultDailyQuota is a project's default YouTube Data API allowance in
// units per day, for every endpoint except search.list and videos.insert.
const DefaultDailyQuota = 10000

// Documented-by-convention unit costs; see the package comment on why these
// are configurable rather than constants of the API.
const (
	// DefaultCostList is the unit cost of one liveChatMessages.list call.
	DefaultCostList = 5
	// DefaultCostDiscover is the unit cost of one liveBroadcasts.list call.
	DefaultCostDiscover = 1
)

// Poll bounds. The server dictates pollingIntervalMillis, but a broken or
// hostile value must not turn into a spin loop or an hour of silence.
const (
	DefaultMinPollInterval = 1 * time.Second
	DefaultMaxPollInterval = 5 * time.Minute
	// DefaultDiscoveryInterval is how often Watch re-runs liveBroadcasts.list
	// to notice a broadcast starting or ending.
	DefaultDiscoveryInterval = 60 * time.Second
	// maxResultsFloor is the API's documented minimum for maxResults.
	maxResultsFloor = 200
)

// Driver implements driver.Driver for YouTube.
type Driver struct {
	// HTTPClient is injectable for tests; nil means http.DefaultClient.
	HTTPClient *http.Client
	// BaseURL overrides DefaultBaseURL (tests, proxies).
	BaseURL string
	// Resolver maps opaque message ids back to native LiveChatMessage ids.
	Resolver driver.Resolver
	// Budget paces polling against the daily quota. nil means unlimited.
	Budget *quota.Budget
	// CostList / CostDiscover are the unit costs charged per call.
	CostList     int
	CostDiscover int
	// MaxResults is the page size requested from liveChatMessages.list.
	MaxResults int
	// MinPollInterval / MaxPollInterval clamp pollingIntervalMillis.
	MinPollInterval time.Duration
	MaxPollInterval time.Duration
	// DiscoveryInterval is the liveBroadcasts.list cadence inside Watch.
	DiscoveryInterval time.Duration
	// Backoff is the reconnect policy for transient failures.
	Backoff retry.Backoff
	// Log receives operational events. P4: never message text.
	Log *slog.Logger
}

// New returns a YouTube driver using resolver for opaque-id resolution and
// the documented default quota allowance.
func New(resolver driver.Resolver) *Driver {
	return &Driver{
		HTTPClient:        http.DefaultClient,
		BaseURL:           DefaultBaseURL,
		Resolver:          resolver,
		Budget:            quota.New(DefaultDailyQuota),
		CostList:          DefaultCostList,
		CostDiscover:      DefaultCostDiscover,
		MaxResults:        maxResultsFloor,
		MinPollInterval:   DefaultMinPollInterval,
		MaxPollInterval:   DefaultMaxPollInterval,
		DiscoveryInterval: DefaultDiscoveryInterval,
		Backoff:           retry.DefaultBackoff(),
		Log:               slog.Default(),
	}
}

// Platform implements driver.Driver.
func (d *Driver) Platform() string { return "youtube" }

func (d *Driver) baseURL() string {
	if d.BaseURL == "" {
		return DefaultBaseURL
	}
	return d.BaseURL
}

func (d *Driver) client() *http.Client {
	if d.HTTPClient == nil {
		return http.DefaultClient
	}
	return d.HTTPClient
}

func (d *Driver) log() *slog.Logger {
	if d.Log == nil {
		return slog.Default()
	}
	return d.Log
}

func (d *Driver) budget() *quota.Budget {
	if d.Budget == nil {
		return quota.Unlimited()
	}
	return d.Budget
}

// ---------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------

// liveBroadcastList is the liveBroadcasts.list response, narrowed to the
// fields the adapter uses.
type liveBroadcastList struct {
	Items []struct {
		ID      string `json:"id"`
		Snippet struct {
			Title      string `json:"title"`
			LiveChatID string `json:"liveChatId"`
		} `json:"snippet"`
		Status struct {
			LifeCycleStatus string `json:"lifeCycleStatus"`
		} `json:"status"`
	} `json:"items"`
	NextPageToken string `json:"nextPageToken"`
}

// DiscoverLive implements driver.Driver: liveBroadcasts.list filtered to
// active broadcasts, resolved to their live chat ids.
//
// The ContentRef's NativeChannelID is the liveChatId, not the YouTube
// channel id: it is the identifier liveChatMessages.list is keyed by, one
// creator can run several concurrent broadcasts, and it is what every
// message in the stream reports as its own snippet.liveChatId. Opaque
// content_id therefore identifies "this live chat", which is the
// granularity policies and Redis moderation state want.
func (d *Driver) DiscoverLive(ctx context.Context, conn driver.Connection) ([]driver.ContentRef, error) {
	if err := d.budget().Reserve(ctx, d.costDiscover()); err != nil {
		return nil, err
	}
	q := url.Values{
		"part":            {"id,snippet,status"},
		"broadcastStatus": {"active"},
		// broadcastType defaults to "event", which omits the persistent
		// broadcast a 24/7 channel streams on. "all" covers both.
		"broadcastType": {"all"},
		"maxResults":    {"50"},
	}
	var body liveBroadcastList
	if err := d.get(ctx, conn, "/liveBroadcasts", q, &body); err != nil {
		return nil, err
	}
	refs := make([]driver.ContentRef, 0, len(body.Items))
	for _, it := range body.Items {
		// A broadcast can be "active" by filter and still have chat off.
		if it.Snippet.LiveChatID == "" {
			continue
		}
		refs = append(refs, driver.ContentRef{
			NativeChannelID: it.Snippet.LiveChatID,
			Title:           it.Snippet.Title,
		})
	}
	return refs, nil
}

// ---------------------------------------------------------------------
// Watch
// ---------------------------------------------------------------------

// Watch implements driver.Driver.
//
// One YouTube connection is a channel, and a channel may have zero, one or
// several concurrently active broadcasts — so Watch is a supervisor: a
// discovery loop on liveBroadcasts.list, and one poller goroutine per live
// chat it finds. Chats that end are reaped, chats that appear are picked up
// on the next discovery pass, and every poller shares the same quota budget
// so pacing accounts for all of them together.
//
// Watch returns when ctx is cancelled (nil), when the connection's
// credentials are rejected (wrapping driver.ErrUnauthorized, so the ingest
// manager runs the §5.6 refresh and restarts us), or when the provider
// reports something permanent.
func (d *Driver) Watch(ctx context.Context, conn driver.Connection, out chan<- driver.Message) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type poller struct {
		cancel context.CancelFunc
		done   chan struct{}
	}
	var mu sync.Mutex
	running := make(map[string]*poller)
	// Reaping every poller before returning is what makes cancellation
	// leak-free: Watch never outlives its goroutines and never leaves one
	// behind writing to out.
	defer func() {
		mu.Lock()
		pending := make([]*poller, 0, len(running))
		for _, p := range running {
			p.cancel()
			pending = append(pending, p)
		}
		mu.Unlock()
		for _, p := range pending {
			<-p.done
		}
	}()

	// fatal carries the first connection-wide failure (auth, permanent) out
	// of a poller; per-chat endings are not fatal.
	fatal := make(chan error, 1)
	raise := func(err error) {
		select {
		case fatal <- err:
		default:
		}
	}

	active := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(running)
	}

	backoff := d.Backoff
	for {
		refs, err := d.DiscoverLive(ctx, conn)
		switch {
		case err == nil:
			backoff.Reset()
			live := make(map[string]struct{}, len(refs))
			for _, ref := range refs {
				live[ref.NativeChannelID] = struct{}{}
			}
			mu.Lock()
			for chatID, p := range running {
				if _, ok := live[chatID]; !ok {
					p.cancel()
					delete(running, chatID)
				}
			}
			for _, ref := range refs {
				if _, ok := running[ref.NativeChannelID]; ok {
					continue
				}
				pctx, pcancel := context.WithCancel(ctx)
				p := &poller{cancel: pcancel, done: make(chan struct{})}
				running[ref.NativeChannelID] = p
				chatID := ref.NativeChannelID
				go func() {
					defer close(p.done)
					perr := d.pollChat(pctx, conn, chatID, out, active)
					switch {
					case perr == nil || pctx.Err() != nil:
					case errors.Is(perr, driver.ErrGone):
						// Chat ended: normal, not a connection failure.
						d.log().Info("youtube live chat ended",
							"connection_id", conn.ID, "platform", "youtube")
					default:
						raise(perr)
					}
				}()
			}
			mu.Unlock()

		case ctx.Err() != nil:
			return nil
		case errors.Is(err, driver.ErrUnauthorized), driver.Terminal(err):
			return err
		default:
			// Transient discovery failure: keep the pollers we have and
			// try again after backoff (P2, fail open — an unreachable
			// discovery endpoint must not tear down live streams).
			d.log().Warn("youtube discovery failed", "connection_id", conn.ID,
				"platform", "youtube", "error", err.Error())
			if werr := backoff.Wait(ctx); werr != nil {
				return nil
			}
			continue
		}

		t := time.NewTimer(d.discoveryInterval())
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case err := <-fatal:
			t.Stop()
			return err
		case <-t.C:
		}
	}
}

// liveChatMessageList is the liveChatMessages.list response, narrowed.
type liveChatMessageList struct {
	NextPageToken         string `json:"nextPageToken"`
	PollingIntervalMillis int    `json:"pollingIntervalMillis"`
	OfflineAt             string `json:"offlineAt"`
	Items                 []struct {
		ID      string `json:"id"`
		Snippet struct {
			Type               string `json:"type"`
			LiveChatID         string `json:"liveChatId"`
			AuthorChannelID    string `json:"authorChannelId"`
			PublishedAt        string `json:"publishedAt"`
			HasDisplayContent  bool   `json:"hasDisplayContent"`
			DisplayMessage     string `json:"displayMessage"`
			TextMessageDetails struct {
				MessageText string `json:"messageText"`
			} `json:"textMessageDetails"`
		} `json:"snippet"`
		AuthorDetails struct {
			ChannelID       string `json:"channelId"`
			DisplayName     string `json:"displayName"`
			IsChatModerator bool   `json:"isChatModerator"`
			IsChatOwner     bool   `json:"isChatOwner"`
		} `json:"authorDetails"`
	} `json:"items"`
}

// pollChat polls one live chat until it ends or ctx is cancelled.
//
// active reports how many chats this connection is polling right now; the
// quota pacing divides the allowance between them.
func (d *Driver) pollChat(ctx context.Context, conn driver.Connection, liveChatID string, out chan<- driver.Message, active func() int) error {
	var pageToken string
	// The first call deliberately carries no pageToken: the API answers
	// with recent history, and pageToken thereafter delivers only what is
	// new. Replaying a little history on (re)start is harmless — the
	// duplicate detector downstream is keyed on (content, sender) — and it
	// is the only way to avoid a gap across a poller restart.
	backoff := d.Backoff
	for {
		q := url.Values{
			"liveChatId": {liveChatID},
			"part":       {"id,snippet,authorDetails"},
			"maxResults": {strconv.Itoa(d.maxResults())},
		}
		if pageToken != "" {
			q.Set("pageToken", pageToken)
		}

		// Hard quota stop before the call, not after: overshoot is what
		// costs an entire day of ingestion.
		if err := d.budget().Reserve(ctx, d.costList()); err != nil {
			return nil
		}

		var body liveChatMessageList
		// ReceivedAt is stamped by get(), at the instant the response body
		// came back — before parsing, before the channel send (§4.6).
		received, err := d.getAt(ctx, conn, "/liveChat/messages", q, &body)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return nil
			case errors.Is(err, driver.ErrGone):
				return err // liveChatEnded / liveChatDisabled / not found
			case errors.Is(err, driver.ErrUnauthorized), driver.Terminal(err):
				return err
			case errors.Is(err, errQuotaExceeded):
				// The provider is the authority on the balance; empty the
				// local bucket so Reserve does the waiting from here on.
				d.budget().Drain()
				d.log().Warn("youtube quota exceeded", "connection_id", conn.ID, "platform", "youtube")
				fallthrough
			default:
				d.log().Warn("youtube poll failed", "connection_id", conn.ID,
					"platform", "youtube", "error", err.Error())
				if werr := backoff.Wait(ctx); werr != nil {
					return nil
				}
				continue
			}
		}
		backoff.Reset()
		pageToken = body.NextPageToken

		for _, it := range body.Items {
			// Only real viewer text is moderated. Super Chats, membership
			// gifts, poll events and system notices all arrive on this
			// stream with other snippet.type values.
			if it.Snippet.Type != "textMessageEvent" {
				continue
			}
			text := it.Snippet.TextMessageDetails.MessageText
			if text == "" {
				text = it.Snippet.DisplayMessage
			}
			if text == "" {
				continue
			}
			author := it.AuthorDetails.ChannelID
			if author == "" {
				author = it.Snippet.AuthorChannelID
			}
			// P5: native ids only. The ingest loop mints the opaque ones.
			if err := driver.Send(ctx, out, driver.Message{
				NativeChannelID: liveChatID,
				NativeAuthorID:  author,
				NativeMessageID: it.ID,
				Text:            text,
				ReceivedAt:      received,
			}); err != nil {
				return nil // ctx cancelled
			}
		}

		if body.OfflineAt != "" {
			// The broadcast went offline; the chat is over.
			return fmt.Errorf("youtube: live chat went offline: %w", driver.ErrGone)
		}

		if err := retry.Sleep(ctx, d.pollDelay(body.PollingIntervalMillis, active())); err != nil {
			return nil
		}
	}
}

// pollDelay reconciles the server's instruction with the quota budget. The
// server's pollingIntervalMillis is a floor on politeness (polling faster
// earns rateLimitExceeded); the quota pace is a floor on solvency. The
// larger wins, clamped into [MinPollInterval, MaxPollInterval].
func (d *Driver) pollDelay(serverMillis, activeChats int) time.Duration {
	server := time.Duration(serverMillis) * time.Millisecond
	paced := d.budget().Pace(d.costList(), activeChats)
	delay := max(server, paced)
	return min(max(delay, d.minPoll()), d.maxPoll())
}

// ---------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------

// Delete implements driver.Driver via liveChatMessages.delete.
func (d *Driver) Delete(ctx context.Context, conn driver.Connection, _, messageID string) error {
	native, ok := d.Resolver.NativeMessageID(messageID)
	if !ok {
		// Unresolvable on this instance (restart or rebalance since
		// ingest): best-effort semantics, treated as already gone.
		return driver.ErrNotFound
	}
	u := d.baseURL() + "/liveChat/messages?id=" + url.QueryEscape(native)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+conn.AccessToken)
	resp, err := d.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// YouTube reports an ended live chat as 403 liveChatEnded: the content
	// is gone, terminal drop (§7.2).
	if resp.StatusCode == http.StatusForbidden {
		return driver.ErrGone
	}
	return driver.FromHTTPStatus(resp.StatusCode)
}

// ---------------------------------------------------------------------
// HTTP plumbing and error classification
// ---------------------------------------------------------------------

// errQuotaExceeded marks the daily-allowance exhaustion reasons. It is
// deliberately distinct from driver.ErrRateLimited (which it also is, for
// the deletion consumer's benefit) because the driver reacts differently:
// a rate limit is seconds, an exhausted quota is hours.
var errQuotaExceeded = fmt.Errorf("youtube: daily quota exceeded: %w", driver.ErrRateLimited)

// apiErrorBody is the standard Google API error envelope.
type apiErrorBody struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Errors  []struct {
			Domain string `json:"domain"`
			Reason string `json:"reason"`
		} `json:"errors"`
		Status string `json:"status"`
	} `json:"error"`
}

func (d *Driver) get(ctx context.Context, conn driver.Connection, path string, q url.Values, dst any) error {
	_, err := d.getAt(ctx, conn, path, q, dst)
	return err
}

// getAt performs one authenticated GET and reports the instant the response
// body was read — Message.ReceivedAt, and therefore the start of the §4.6
// latency clock for every message in it.
func (d *Driver) getAt(ctx context.Context, conn driver.Connection, path string, q url.Values, dst any) (time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.baseURL()+path+"?"+q.Encode(), nil)
	if err != nil {
		return time.Time{}, err
	}
	req.Header.Set("Authorization", "Bearer "+conn.AccessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := d.client().Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("youtube: %s: %w", path, err)
	}
	defer resp.Body.Close()
	// 8 MiB is far above a 2000-message page and bounds a broken provider.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	received := time.Now()
	if err != nil {
		return time.Time{}, fmt.Errorf("youtube: %s: %w", path, err)
	}
	if resp.StatusCode/100 != 2 {
		return time.Time{}, classify(path, resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return time.Time{}, fmt.Errorf("youtube: %s: undecodable response", path)
	}
	return received, nil
}

// classify maps a non-2xx YouTube reply onto the shared driver error
// classes, using the reason string when the API supplies one. P4: the
// provider message is never logged or wrapped in — only the reason code,
// which is a fixed enum and cannot carry chat text.
func classify(path string, status int, body []byte) error {
	var env apiErrorBody
	_ = json.Unmarshal(body, &env)
	reason := ""
	if len(env.Error.Errors) > 0 {
		reason = env.Error.Errors[0].Reason
	}
	switch reason {
	case "quotaExceeded", "dailyLimitExceeded":
		return errQuotaExceeded
	case "rateLimitExceeded", "userRateLimitExceeded":
		return fmt.Errorf("youtube: %s: rate limited: %w", path, driver.ErrRateLimited)
	case "liveChatEnded", "liveChatDisabled", "liveChatNotFound", "liveChatRemoved":
		return fmt.Errorf("youtube: %s: %s: %w", path, reason, driver.ErrGone)
	case "authError", "invalidCredentials":
		return fmt.Errorf("youtube: %s: %s: %w", path, reason, driver.ErrUnauthorized)
	case "forbidden", "insufficientPermissions", "insufficientLivePermissions", "liveStreamingNotEnabled":
		// A scope or channel-capability problem: reconnecting the account
		// (§5.5) is the only fix, so retrying is pointless.
		return fmt.Errorf("youtube: %s: %s: %w", path, reason, driver.ErrPermanent)
	}
	switch status {
	case http.StatusUnauthorized:
		return fmt.Errorf("youtube: %s: %w", path, driver.ErrUnauthorized)
	case http.StatusForbidden:
		// 403 with no recognised reason: treat as permanent rather than
		// spin. Quota and rate limits always carry a reason.
		return fmt.Errorf("youtube: %s: forbidden: %w", path, driver.ErrPermanent)
	case http.StatusNotFound:
		return fmt.Errorf("youtube: %s: %w", path, driver.ErrGone)
	case http.StatusTooManyRequests:
		return fmt.Errorf("youtube: %s: %w", path, driver.ErrRateLimited)
	}
	return fmt.Errorf("youtube: %s: provider returned status %d", path, status)
}

// ---------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------

func (d *Driver) costList() int {
	if d.CostList <= 0 {
		return DefaultCostList
	}
	return d.CostList
}

func (d *Driver) costDiscover() int {
	if d.CostDiscover <= 0 {
		return DefaultCostDiscover
	}
	return d.CostDiscover
}

func (d *Driver) maxResults() int {
	if d.MaxResults < maxResultsFloor {
		return maxResultsFloor
	}
	if d.MaxResults > 2000 {
		return 2000
	}
	return d.MaxResults
}

func (d *Driver) minPoll() time.Duration {
	if d.MinPollInterval <= 0 {
		return DefaultMinPollInterval
	}
	return d.MinPollInterval
}

func (d *Driver) maxPoll() time.Duration {
	if d.MaxPollInterval <= 0 {
		return DefaultMaxPollInterval
	}
	return max(d.MaxPollInterval, d.minPoll())
}

func (d *Driver) discoveryInterval() time.Duration {
	if d.DiscoveryInterval <= 0 {
		return DefaultDiscoveryInterval
	}
	return d.DiscoveryInterval
}
