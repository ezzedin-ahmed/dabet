package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"dabet/services/policy-service/internal/policy"
)

// PG is the Postgres store.Repo over the policy schema.
type PG struct {
	pool *pgxpool.Pool
}

// NewPG wraps pool.
func NewPG(pool *pgxpool.Pool) *PG { return &PG{pool: pool} }

var _ Repo = (*PG)(nil)

func timeFromUnixNano(n int64) time.Time { return time.Unix(0, n).UTC() }

const policyColumns = `id, creator_id, scope::text, scope_id,
	rate_limit_messages, rate_limit_seconds, spam::text,
	restricted_words, restricted_content, restricted_content_action::text,
	created_at, updated_at`

func scanPolicy(row pgx.Row) (*policy.Policy, error) {
	var (
		p       policy.Policy
		scope   string
		spam    string
		action  string
		content []byte
	)
	err := row.Scan(&p.ID, &p.CreatorID, &scope, &p.ScopeID,
		&p.RateLimitMessages, &p.RateLimitSeconds, &spam,
		&p.RestrictedWords, &content, &action,
		&p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	p.Scope = policy.Scope(scope)
	p.Spam = policy.SpamMode(spam)
	p.RestrictedContentAction = policy.RCAction(action)
	if p.RestrictedWords == nil {
		p.RestrictedWords = []string{}
	}
	if err := json.Unmarshal(content, &p.RestrictedContent); err != nil {
		return nil, fmt.Errorf("decode restricted_content: %w", err)
	}
	if p.RestrictedContent == nil {
		p.RestrictedContent = []policy.RestrictedContentEntry{}
	}
	return &p, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Create implements Repo.
func (s *PG) Create(ctx context.Context, p *policy.Policy) error {
	content, err := json.Marshal(p.RestrictedContent)
	if err != nil {
		return fmt.Errorf("encode restricted_content: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO policy.policies (
			id, creator_id, scope, scope_id,
			rate_limit_messages, rate_limit_seconds, spam,
			restricted_words, restricted_content, restricted_content_action,
			created_at, updated_at
		) VALUES ($1, $2, $3::policy.policy_scope_t, $4, $5, $6,
			$7::policy.spam_mode_t, $8, $9, $10::policy.rc_action_t, $11, $12)`,
		p.ID, p.CreatorID, string(p.Scope), p.ScopeID,
		p.RateLimitMessages, p.RateLimitSeconds, string(p.Spam),
		p.RestrictedWords, content, string(p.RestrictedContentAction),
		p.CreatedAt, p.UpdatedAt)
	if isUniqueViolation(err) {
		return ErrDuplicate
	}
	return err
}

// GetByID implements Repo.
func (s *PG) GetByID(ctx context.Context, id string) (*policy.Policy, error) {
	row := s.pool.QueryRow(ctx,
		"SELECT "+policyColumns+" FROM policy.policies WHERE id = $1", id)
	return scanPolicy(row)
}

// GetByScope implements Repo.
func (s *PG) GetByScope(ctx context.Context, scope policy.Scope, scopeID string) (*policy.Policy, error) {
	row := s.pool.QueryRow(ctx,
		"SELECT "+policyColumns+" FROM policy.policies WHERE scope = $1::policy.policy_scope_t AND scope_id = $2",
		string(scope), scopeID)
	return scanPolicy(row)
}

// List implements Repo.
func (s *PG) List(ctx context.Context, creatorID string, f ListFilter, after *Cursor, limit int) ([]*policy.Policy, error) {
	q := "SELECT " + policyColumns + " FROM policy.policies WHERE creator_id = $1"
	args := []any{creatorID}
	if f.Scope != "" {
		args = append(args, string(f.Scope))
		q += fmt.Sprintf(" AND scope = $%d::policy.policy_scope_t", len(args))
	}
	if f.ScopeID != "" {
		args = append(args, f.ScopeID)
		q += fmt.Sprintf(" AND scope_id = $%d", len(args))
	}
	if after != nil {
		args = append(args, timeFromUnixNano(after.CreatedAtUnixNano), after.ID)
		q += fmt.Sprintf(" AND (created_at, id) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit)
	q += fmt.Sprintf(" ORDER BY created_at, id LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*policy.Policy
	for rows.Next() {
		p, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Update implements Repo. Scope fields and created_at are deliberately
// absent from the SET list: they are immutable (docs §6.6).
func (s *PG) Update(ctx context.Context, p *policy.Policy) error {
	content, err := json.Marshal(p.RestrictedContent)
	if err != nil {
		return fmt.Errorf("encode restricted_content: %w", err)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE policy.policies SET
			rate_limit_messages = $2,
			rate_limit_seconds = $3,
			spam = $4::policy.spam_mode_t,
			restricted_words = $5,
			restricted_content = $6,
			restricted_content_action = $7::policy.rc_action_t,
			updated_at = $8
		WHERE id = $1`,
		p.ID, p.RateLimitMessages, p.RateLimitSeconds, string(p.Spam),
		p.RestrictedWords, content, string(p.RestrictedContentAction), p.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Delete implements Repo.
func (s *PG) Delete(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, "DELETE FROM policy.policies WHERE id = $1", id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
