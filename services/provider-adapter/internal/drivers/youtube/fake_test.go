package youtube

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// fakeYouTube is a local HTTP server that speaks the real YouTube Data API
// v3 live-chat dialect: liveBroadcasts.list answers with broadcast
// resources carrying snippet.liveChatId, and liveChatMessages.list answers
// with paginated liveChatMessage resources plus nextPageToken and
// pollingIntervalMillis. Tests script the pages; the driver has no idea it
// is not talking to Google.
type fakeYouTube struct {
	t *testing.T

	mu sync.Mutex
	// broadcasts are the active broadcasts liveBroadcasts.list reports.
	broadcasts []fakeBroadcast
	// pages are served in order per live chat id; the last page repeats
	// once the script is exhausted, which is what a quiet chat looks like.
	pages map[string][]fakePage
	// cursor tracks how far into each chat's script we are.
	cursor map[string]int

	// listTokens records the pageToken of every liveChatMessages.list call,
	// in order, so a test can assert the cursor actually advanced.
	listTokens []string
	// listAuth records the Authorization header of every list call, so the
	// refresh path can be asserted to have swapped the token.
	listAuth []string
	// broadcastCalls counts liveBroadcasts.list calls.
	broadcastCalls int

	// listStatus optionally overrides the reply for the nth (1-based) list
	// call. Returning ok=false serves the scripted page instead.
	listStatus func(call int) (status int, body string, ok bool)
	// broadcastStatus does the same for liveBroadcasts.list.
	broadcastStatus func(call int) (status int, body string, ok bool)

	server *httptest.Server
}

type fakeBroadcast struct {
	ID         string
	Title      string
	LiveChatID string
}

// fakePage is one liveChatMessages.list response.
type fakePage struct {
	Items                 []fakeItem
	NextPageToken         string
	PollingIntervalMillis int
	OfflineAt             string
}

// fakeItem is one liveChatMessage. Type defaults to textMessageEvent.
type fakeItem struct {
	ID       string
	Type     string
	AuthorID string
	Text     string
}

func newFakeYouTube(t *testing.T) *fakeYouTube {
	t.Helper()
	f := &fakeYouTube{
		t:      t,
		pages:  make(map[string][]fakePage),
		cursor: make(map[string]int),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/liveBroadcasts", f.handleBroadcasts)
	mux.HandleFunc("/liveChat/messages", f.handleMessages)
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeYouTube) URL() string { return f.server.URL }

func (f *fakeYouTube) setBroadcasts(bs ...fakeBroadcast) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.broadcasts = bs
}

func (f *fakeYouTube) setPages(liveChatID string, pages ...fakePage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pages[liveChatID] = pages
	f.cursor[liveChatID] = 0
}

func (f *fakeYouTube) tokens() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.listTokens...)
}

func (f *fakeYouTube) listCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.listTokens)
}

func (f *fakeYouTube) handleBroadcasts(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.broadcastCalls++
	call := f.broadcastCalls
	hook := f.broadcastStatus
	bs := append([]fakeBroadcast(nil), f.broadcasts...)
	f.mu.Unlock()

	if r.URL.Query().Get("broadcastStatus") != "active" {
		http.Error(w, "expected broadcastStatus=active", http.StatusBadRequest)
		return
	}
	if hook != nil {
		if status, body, ok := hook(call); ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
	}

	type item struct {
		ID      string `json:"id"`
		Snippet struct {
			Title      string `json:"title"`
			LiveChatID string `json:"liveChatId"`
		} `json:"snippet"`
		Status struct {
			LifeCycleStatus string `json:"lifeCycleStatus"`
		} `json:"status"`
	}
	out := struct {
		Kind  string `json:"kind"`
		Items []item `json:"items"`
	}{Kind: "youtube#liveBroadcastListResponse"}
	for _, b := range bs {
		var it item
		it.ID = b.ID
		it.Snippet.Title = b.Title
		it.Snippet.LiveChatID = b.LiveChatID
		it.Status.LifeCycleStatus = "live"
		out.Items = append(out.Items, it)
	}
	writeJSON(w, http.StatusOK, out)
}

func (f *fakeYouTube) handleMessages(w http.ResponseWriter, r *http.Request) {
	chatID := r.URL.Query().Get("liveChatId")
	token := r.URL.Query().Get("pageToken")

	f.mu.Lock()
	f.listTokens = append(f.listTokens, token)
	f.listAuth = append(f.listAuth, r.Header.Get("Authorization"))
	call := len(f.listTokens)
	hook := f.listStatus
	script := f.pages[chatID]
	idx := f.cursor[chatID]
	if idx < len(script)-1 {
		f.cursor[chatID] = idx + 1
	}
	f.mu.Unlock()

	if hook != nil {
		if status, body, ok := hook(call); ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
	}
	if len(script) == 0 {
		writeJSON(w, http.StatusNotFound, errorBody(404, "liveChatNotFound"))
		return
	}
	page := script[min(idx, len(script)-1)]

	type snippet struct {
		Type               string `json:"type"`
		LiveChatID         string `json:"liveChatId"`
		AuthorChannelID    string `json:"authorChannelId"`
		PublishedAt        string `json:"publishedAt"`
		HasDisplayContent  bool   `json:"hasDisplayContent"`
		DisplayMessage     string `json:"displayMessage"`
		TextMessageDetails struct {
			MessageText string `json:"messageText"`
		} `json:"textMessageDetails"`
	}
	type authorDetails struct {
		ChannelID       string `json:"channelId"`
		DisplayName     string `json:"displayName"`
		IsChatModerator bool   `json:"isChatModerator"`
	}
	type item struct {
		Kind          string        `json:"kind"`
		ID            string        `json:"id"`
		Snippet       snippet       `json:"snippet"`
		AuthorDetails authorDetails `json:"authorDetails"`
	}
	out := struct {
		Kind                  string `json:"kind"`
		NextPageToken         string `json:"nextPageToken"`
		PollingIntervalMillis int    `json:"pollingIntervalMillis"`
		OfflineAt             string `json:"offlineAt,omitempty"`
		PageInfo              struct {
			TotalResults   int `json:"totalResults"`
			ResultsPerPage int `json:"resultsPerPage"`
		} `json:"pageInfo"`
		Items []item `json:"items"`
	}{
		Kind:                  "youtube#liveChatMessageListResponse",
		NextPageToken:         page.NextPageToken,
		PollingIntervalMillis: page.PollingIntervalMillis,
		OfflineAt:             page.OfflineAt,
	}
	out.PageInfo.TotalResults = len(page.Items)
	out.PageInfo.ResultsPerPage = len(page.Items)
	for _, it := range page.Items {
		typ := it.Type
		if typ == "" {
			typ = "textMessageEvent"
		}
		var jt item
		jt.Kind = "youtube#liveChatMessage"
		jt.ID = it.ID
		jt.Snippet.Type = typ
		jt.Snippet.LiveChatID = chatID
		jt.Snippet.AuthorChannelID = it.AuthorID
		jt.Snippet.PublishedAt = "2026-08-19T14:02:11.412Z"
		jt.Snippet.HasDisplayContent = true
		jt.Snippet.DisplayMessage = it.Text
		jt.Snippet.TextMessageDetails.MessageText = it.Text
		jt.AuthorDetails.ChannelID = it.AuthorID
		jt.AuthorDetails.DisplayName = "viewer-" + it.AuthorID
		out.Items = append(out.Items, jt)
	}
	writeJSON(w, http.StatusOK, out)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// errorBody builds the standard Google API error envelope.
func errorBody(code int, reason string) map[string]any {
	return map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": "test",
			"errors": []map[string]any{
				{"domain": "youtube.liveChat", "reason": reason, "message": "test"},
			},
		},
	}
}

func errorJSON(code int, reason string) string {
	b, _ := json.Marshal(errorBody(code, reason))
	return string(b)
}
