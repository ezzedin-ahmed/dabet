// Package identity resolves a creator id to the address the A8 balance
// notifications are sent to.
//
// DEVIATION (documented). identity.creators is user-service's table, and
// §5.3's own migration notes that credits-service deliberately keeps no
// foreign keys into it. This is a read of one column pair, not a write
// and not a join in a query path: credits-service has to be able to
// address the creator it is billing, and the alternatives are worse —
// duplicating email into billing means keeping two copies of a mutable
// personal detail in sync, and a synchronous call to user-service on the
// notification path adds a dependency to something that must never block
// the ledger.
//
// The read is behind mail.Recipients, so replacing it with an internal
// user-service endpoint later is a constructor change.
package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound means no creator holds that id.
var ErrNotFound = errors.New("identity: creator not found")

// Postgres reads identity.creators.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres wraps pool.
func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

// Recipient returns the creator's email and full name. It runs on a mail
// worker, never on the ledger path.
func (p *Postgres) Recipient(ctx context.Context, creatorID string) (string, string, error) {
	var email, fullname string
	err := p.pool.QueryRow(ctx,
		`SELECT email::text, fullname FROM identity.creators WHERE id = $1`, creatorID,
	).Scan(&email, &fullname)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", fmt.Errorf("identity: read creator: %w", err)
	}
	return email, fullname, nil
}
