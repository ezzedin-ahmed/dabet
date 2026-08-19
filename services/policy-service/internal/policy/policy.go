// Package policy holds the domain model for Area B (docs §6): the policy
// document, its scopes, and the §6.5 validation and normalisation rules.
//
// P4 reminder: restricted words and restricted content are creator
// configuration that may mirror abusive text. They must never be logged at
// info level or above, and never appear in error messages or metric labels.
package policy

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Scope is where a policy attaches (docs §6.1).
type Scope string

const (
	ScopeCreator  Scope = "creator"
	ScopePlatform Scope = "platform"
	ScopeContent  Scope = "content"
)

// ValidScope reports whether s is one of the three scopes.
func ValidScope(s Scope) bool {
	return s == ScopeCreator || s == ScopePlatform || s == ScopeContent
}

// SpamMode mirrors the spam_mode_t enum (docs §6.3).
type SpamMode string

const (
	SpamNone      SpamMode = "none"
	SpamIdentical SpamMode = "identical"
	SpamSemantic  SpamMode = "semantic"
)

// RCAction mirrors the rc_action_t enum (docs §6.3/§6.4). It applies only
// to the LLM restricted_content check.
type RCAction string

const (
	RCActionAuto   RCAction = "auto"
	RCActionReview RCAction = "review"
)

// RestrictedContentEntry is one rubric entry of restricted_content.
type RestrictedContentEntry struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Examples    []string `json:"examples,omitempty"`
}

// Document is the mutable body of a policy: everything except identity and
// scope. POST and PUT both carry exactly this shape (plus scope fields on
// POST), which is what makes PUT a whole-document replacement — omitted
// fields become their defaults, never "inherit the previous value from a
// merge" (docs §6.2).
type Document struct {
	RateLimitMessages       *int                     `json:"rate_limit_messages"`
	RateLimitSeconds        *int                     `json:"rate_limit_seconds"`
	Spam                    SpamMode                 `json:"spam,omitempty"`
	RestrictedWords         []string                 `json:"restricted_words"`
	RestrictedContent       []RestrictedContentEntry `json:"restricted_content"`
	RestrictedContentAction RCAction                 `json:"restricted_content_action,omitempty"`
}

// Policy is a stored policy row.
type Policy struct {
	ID        string `json:"id"`
	CreatorID string `json:"creator_id"`
	Scope     Scope  `json:"scope"`
	ScopeID   string `json:"scope_id"`
	Document
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Platforms known to Dabet (docs §5.2 platform_t). Used only to validate
// the {creator_id}:{platform} shape of platform-scoped scope_ids.
var Platforms = map[string]bool{
	"youtube": true,
	"twitch":  true,
	"discord": true,
}

// NewID returns a random UUIDv4 string. Generated app-side so the fake and
// Postgres repositories behave identically.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
