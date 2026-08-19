// Package contracts holds the Kafka topic registry and event schemas of
// docs §4.2, plus the partition-key helpers. These are frozen cross-area
// contracts: changes require agreement across all four areas.
package contracts

import "time"

// Topic names.
const (
	TopicMessages  = "messages.v1"
	TopicFlagged   = "flagged.v1"
	TopicDeletions = "deletions.v1"
	TopicUsage     = "usage.v1"
)

// Detector identifies the cascade stage that flagged a message.
type Detector string

const (
	DetectorRateLimit         Detector = "rate_limit"
	DetectorDuplicate         Detector = "duplicate"
	DetectorSemanticSpam      Detector = "semantic_spam"
	DetectorRestrictedWord    Detector = "restricted_word"
	DetectorRestrictedContent Detector = "restricted_content"
)

// Action is the verdict outcome. Only restricted_content may carry
// ActionReview; every other detector is always ActionAutoDelete (§6.4).
type Action string

const (
	ActionAutoDelete Action = "auto_delete"
	ActionReview     Action = "review"
)

// EventType classifies usage.v1 events.
type EventType string

const (
	EventMessagesProcessed   EventType = "messages_processed"
	EventMessagesReclustered EventType = "messages_reclustered"
)

// Message is one messages.v1 event. message_id, content_id, and author_id
// are opaque adapter-issued strings (<=64 chars, P5); creator_id is a Dabet
// UUID resolved at ingest. There is deliberately no platform field.
type Message struct {
	MessageID  string    `json:"message_id"`
	ContentID  string    `json:"content_id"`
	AuthorID   string    `json:"author_id"`
	CreatorID  string    `json:"creator_id"`
	Text       string    `json:"text"`
	IngestedAt time.Time `json:"ingested_at"`
}

// Flagged is one flagged.v1 event. Text is carried because review needs to
// show it and nothing else stores it.
type Flagged struct {
	MessageID string    `json:"message_id"`
	ContentID string    `json:"content_id"`
	AuthorID  string    `json:"author_id"`
	CreatorID string    `json:"creator_id"`
	Text      string    `json:"text"`
	Detector  Detector  `json:"detector"`
	Action    Action    `json:"action"`
	PolicyID  string    `json:"policy_id"`
	FlaggedAt time.Time `json:"flagged_at"`
}

// Deletion is one deletions.v1 event. No text: the adapter needs only the
// identifiers to issue a platform delete.
type Deletion struct {
	MessageID string    `json:"message_id"`
	ContentID string    `json:"content_id"`
	CreatorID string    `json:"creator_id"`
	Reason    Detector  `json:"reason"`
	IssuedAt  time.Time `json:"issued_at"`
}

// Usage is one usage.v1 event. Producers emit aggregated quantities of
// work per creator per minute with a deterministic idempotency_key; only
// credits-service knows what an event costs.
type Usage struct {
	CreatorID      string    `json:"creator_id"`
	EventType      EventType `json:"event_type"`
	Quantity       int64     `json:"quantity"`
	WindowStart    time.Time `json:"window_start"`
	WindowEnd      time.Time `json:"window_end"`
	IdempotencyKey string    `json:"idempotency_key"`
}
