package repo

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
)

// fakeConnections holds the connection-flow state of Fake; split from the
// auth maps for readability. Lazily initialised so pre-existing tests
// constructing Fake directly keep working.
func (f *Fake) initConnections() {
	if f.states == nil {
		f.states = make(map[string]*OAuthState)
	}
	if f.connections == nil {
		f.connections = make(map[string]*Connection)
	}
}

func (f *Fake) CreateOAuthState(_ context.Context, s *OAuthState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initConnections()
	cp := *s
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}
	f.states[s.State] = &cp
	return nil
}

func (f *Fake) ConsumeOAuthState(_ context.Context, state string) (*OAuthState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initConnections()
	s, ok := f.states[state]
	if !ok {
		return nil, ErrNotFound
	}
	delete(f.states, state)
	cp := *s
	return &cp, nil
}

func (f *Fake) UpsertConnection(_ context.Context, c *Connection) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initConnections()
	now := time.Now().UTC()

	// Active-uniqueness (A4): another creator holding the active
	// connection for this platform account is a conflict.
	for _, ex := range f.connections {
		if ex.Platform == c.Platform && ex.ProviderUserID == c.ProviderUserID &&
			ex.Status == "active" && ex.CreatorID != c.CreatorID {
			return "", ErrConnectionConflict
		}
	}

	// Reconnect path: refresh the creator's existing row in place.
	var target *Connection
	for _, ex := range f.connections {
		if ex.CreatorID == c.CreatorID && ex.Platform == c.Platform && ex.ProviderUserID == c.ProviderUserID {
			if target == nil || (ex.Status == "active" && target.Status != "active") ||
				(ex.Status == target.Status && ex.ConnectedAt.After(target.ConnectedAt)) {
				target = ex
			}
		}
	}
	if target != nil {
		target.DisplayName = c.DisplayName
		target.AccessToken = c.AccessToken
		target.RefreshToken = c.RefreshToken
		target.ExpiresAt = c.ExpiresAt
		target.Scopes = append([]string(nil), c.Scopes...)
		target.Status = "active"
		target.UpdatedAt = now
		return target.ID, nil
	}

	cp := *c
	cp.ID = uuid.NewString()
	cp.Scopes = append([]string(nil), c.Scopes...)
	cp.Status = "active"
	cp.ConnectedAt = now
	cp.UpdatedAt = now
	f.connections[cp.ID] = &cp
	return cp.ID, nil
}

func (f *Fake) ListConnections(_ context.Context, creatorID string) ([]Connection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initConnections()
	var out []Connection
	for _, c := range f.connections {
		if c.CreatorID == creatorID {
			out = append(out, *c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ConnectedAt.Equal(out[j].ConnectedAt) {
			return out[i].ConnectedAt.Before(out[j].ConnectedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (f *Fake) RevokeConnection(_ context.Context, id, creatorID string, now time.Time) (*Connection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initConnections()
	c, ok := f.connections[id]
	if !ok || c.CreatorID != creatorID {
		return nil, ErrNotFound
	}
	if c.Status == "revoked" {
		return nil, ErrAlreadyRevoked
	}
	before := *c
	c.Status = "revoked"
	c.UpdatedAt = now
	return &before, nil
}

func (f *Fake) ActiveConnectionCounts(context.Context) (map[string]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initConnections()
	out := make(map[string]int)
	for _, c := range f.connections {
		if c.Status == "active" {
			out[c.Platform]++
		}
	}
	return out, nil
}
