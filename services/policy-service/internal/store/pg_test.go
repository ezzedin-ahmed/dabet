package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"dabet/services/policy-service/internal/policy"
)

// TestPGRoundTrip runs against a real Postgres when POSTGRES_DSN is set
// and is skipped otherwise. It exercises migration idempotence plus the
// repository behaviours the unit tests assert against the fake.
func TestPGRoundTrip(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping real-database test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Applying twice must be a no-op.
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	creatorID := policy.NewID()
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM policy.policies WHERE creator_id = $1", creatorID)
	})

	repo := NewPG(pool)
	now := Now()
	msgs, secs := 5, 10
	p := &policy.Policy{
		ID:        policy.NewID(),
		CreatorID: creatorID,
		Scope:     policy.ScopeCreator,
		ScopeID:   creatorID,
		Document: policy.Document{
			RateLimitMessages: &msgs,
			RateLimitSeconds:  &secs,
			Spam:              policy.SpamIdentical,
			RestrictedWords:   []string{"foo", "bar"},
			RestrictedContent: []policy.RestrictedContentEntry{{
				Title: "Ticket scalping", Description: "Resale offers.", Examples: []string{"selling 2 tickets"},
			}},
			RestrictedContentAction: policy.RCActionReview,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := repo.Create(ctx, p); err != nil {
		t.Fatalf("create: %v", err)
	}

	dup := *p
	dup.ID = policy.NewID()
	if err := repo.Create(ctx, &dup); !errors.Is(err, ErrDuplicate) {
		t.Errorf("duplicate create err = %v, want ErrDuplicate", err)
	}

	got, err := repo.GetByID(ctx, p.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Scope != policy.ScopeCreator || got.Spam != policy.SpamIdentical ||
		len(got.RestrictedWords) != 2 || len(got.RestrictedContent) != 1 ||
		got.RestrictedContent[0].Title != "Ticket scalping" ||
		got.RateLimitMessages == nil || *got.RateLimitMessages != 5 {
		t.Errorf("round-tripped policy mismatch: %+v", got)
	}

	if _, err := repo.GetByScope(ctx, policy.ScopeCreator, creatorID); err != nil {
		t.Errorf("get by scope: %v", err)
	}
	if _, err := repo.GetByScope(ctx, policy.ScopeContent, "ct_missing_"+creatorID); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing scope err = %v, want ErrNotFound", err)
	}

	// List with cursor.
	second := *p
	second.ID = policy.NewID()
	second.Scope = policy.ScopeContent
	second.ScopeID = "ct_" + creatorID
	second.CreatedAt = Now()
	second.UpdatedAt = second.CreatedAt
	if err := repo.Create(ctx, &second); err != nil {
		t.Fatalf("create second: %v", err)
	}
	page1, err := repo.List(ctx, creatorID, ListFilter{}, nil, 1)
	if err != nil || len(page1) != 1 {
		t.Fatalf("list page1 = %v, %v", page1, err)
	}
	page2, err := repo.List(ctx, creatorID, ListFilter{}, &Cursor{
		CreatedAtUnixNano: page1[0].CreatedAt.UnixNano(), ID: page1[0].ID,
	}, 10)
	if err != nil || len(page2) != 1 || page2[0].ID == page1[0].ID {
		t.Fatalf("list page2 = %v, %v", page2, err)
	}
	filtered, err := repo.List(ctx, creatorID, ListFilter{Scope: policy.ScopeContent}, nil, 10)
	if err != nil || len(filtered) != 1 || filtered[0].Scope != policy.ScopeContent {
		t.Fatalf("filtered list = %v, %v", filtered, err)
	}

	// Whole-document update; scope stays immutable at the SQL level.
	p.Document = policy.Document{
		Spam:                    policy.SpamNone,
		RestrictedWords:         []string{},
		RestrictedContent:       []policy.RestrictedContentEntry{},
		RestrictedContentAction: policy.RCActionAuto,
	}
	p.UpdatedAt = Now()
	if err := repo.Update(ctx, p); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err = repo.GetByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RateLimitMessages != nil || len(got.RestrictedWords) != 0 || got.Spam != policy.SpamNone {
		t.Errorf("update did not replace whole document: %+v", got)
	}

	if err := repo.Delete(ctx, p.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := repo.Delete(ctx, p.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("second delete err = %v, want ErrNotFound", err)
	}
}
