package repo

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Fake is an in-memory Repository for unit tests.
type Fake struct {
	mu            sync.Mutex
	creators      map[string]*Creator      // by id
	verifications map[string]*verification // by token hash
	refresh       map[string]*RefreshToken // by token hash
	states        map[string]*OAuthState   // by state
	connections   map[string]*Connection   // by id
	// expiryNotified stands in for connections.expired_notified_at (A6).
	expiryNotified map[string]time.Time // by connection id
}

type verification struct {
	creatorID  string
	expiresAt  time.Time
	consumedAt *time.Time
}

// NewFake returns an empty in-memory repository.
func NewFake() *Fake {
	return &Fake{
		creators:       make(map[string]*Creator),
		verifications:  make(map[string]*verification),
		refresh:        make(map[string]*RefreshToken),
		states:         make(map[string]*OAuthState),
		connections:    make(map[string]*Connection),
		expiryNotified: make(map[string]time.Time),
	}
}

var _ Repository = (*Fake)(nil)

func (f *Fake) CreateCreator(_ context.Context, email, fullname, passwordHash string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.creators {
		if strings.EqualFold(c.Email, email) { // citext semantics
			return "", ErrDuplicateEmail
		}
	}
	now := time.Now().UTC()
	c := &Creator{
		ID:           uuid.NewString(),
		Email:        email,
		Fullname:     fullname,
		PasswordHash: passwordHash,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	f.creators[c.ID] = c
	return c.ID, nil
}

func (f *Fake) CreatorByEmail(_ context.Context, email string) (*Creator, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.creators {
		if strings.EqualFold(c.Email, email) {
			cp := *c
			return &cp, nil
		}
	}
	return nil, ErrNotFound
}

func (f *Fake) CreatorByID(_ context.Context, id string) (*Creator, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.creators[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (f *Fake) CreateEmailVerification(_ context.Context, creatorID, tokenHash string, expiresAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verifications[tokenHash] = &verification{creatorID: creatorID, expiresAt: expiresAt}
	return nil
}

func (f *Fake) ConsumeEmailVerification(_ context.Context, tokenHash string, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.verifications[tokenHash]
	if !ok || v.consumedAt != nil || !v.expiresAt.After(now) {
		return ErrNotFound
	}
	v.consumedAt = &now
	if c, ok := f.creators[v.creatorID]; ok && c.EmailVerifiedAt == nil {
		t := now
		c.EmailVerifiedAt = &t
		c.UpdatedAt = now
	}
	return nil
}

func (f *Fake) InsertRefreshToken(_ context.Context, t *RefreshToken) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *t
	f.refresh[t.TokenHash] = &cp
	return nil
}

func (f *Fake) RefreshTokenByHash(_ context.Context, tokenHash string) (*RefreshToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.refresh[tokenHash]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (f *Fake) RotateRefreshToken(_ context.Context, oldID string, next *RefreshToken, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range f.refresh {
		if t.ID == oldID {
			if t.RevokedAt != nil {
				return ErrNotFound
			}
			ts := now
			t.RevokedAt = &ts
			cp := *next
			f.refresh[next.TokenHash] = &cp
			return nil
		}
	}
	return ErrNotFound
}

func (f *Fake) RevokeRefreshFamily(_ context.Context, familyID string, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range f.refresh {
		if t.FamilyID == familyID && t.RevokedAt == nil {
			ts := now
			t.RevokedAt = &ts
		}
	}
	return nil
}
