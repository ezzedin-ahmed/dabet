package contracts

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// Sample payloads copied verbatim from docs §4.2.
const (
	sampleMessage = `{
  "message_id":  "ytc_01J8XQ7K2M4N",
  "content_id":  "ct_9f2a",
  "author_id":   "sd_3b71",
  "creator_id":  "9d4e...",
  "text":        "…",
  "ingested_at": "2026-08-19T14:02:11.412Z"
}`
	sampleFlagged = `{
  "message_id":   "ytc_01J8XQ7K2M4N",
  "content_id":   "ct_9f2a",
  "author_id":    "sd_3b71",
  "creator_id":   "9d4e...",
  "text":         "…",
  "detector":     "restricted_content",
  "action":       "review",
  "policy_id":    "pol_7a13",
  "flagged_at":   "2026-08-19T14:02:11.914Z"
}`
	sampleDeletion = `{
  "message_id": "ytc_01J8XQ7K2M4N",
  "content_id": "ct_9f2a",
  "creator_id": "9d4e...",
  "reason":     "restricted_word",
  "issued_at":  "2026-08-19T14:02:11.916Z"
}`
	sampleUsage = `{
  "creator_id":      "9d4e...",
  "event_type":      "messages_processed",
  "quantity":        1000,
  "window_start":    "2026-08-19T14:00:00Z",
  "window_end":      "2026-08-19T14:01:00Z",
  "idempotency_key": "mod-7-14:00-9d4e"
}`
)

// roundTrip unmarshals sample into v, re-marshals it, and asserts the
// output is field-for-field identical to the sample.
func roundTrip(t *testing.T, sample string, v any) {
	t.Helper()
	if err := json.Unmarshal([]byte(sample), v); err != nil {
		t.Fatalf("unmarshal sample: %v", err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var want, got map[string]any
	if err := json.Unmarshal([]byte(sample), &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip mismatch:\n got %v\nwant %v", got, want)
	}
}

func TestMessageRoundTrip(t *testing.T) {
	var m Message
	roundTrip(t, sampleMessage, &m)
	if m.MessageID != "ytc_01J8XQ7K2M4N" || m.ContentID != "ct_9f2a" ||
		m.AuthorID != "sd_3b71" || m.CreatorID != "9d4e..." || m.Text != "…" {
		t.Errorf("fields: %+v", m)
	}
	want := time.Date(2026, 8, 19, 14, 2, 11, 412000000, time.UTC)
	if !m.IngestedAt.Equal(want) {
		t.Errorf("ingested_at = %v, want %v", m.IngestedAt, want)
	}
}

func TestFlaggedRoundTrip(t *testing.T) {
	var f Flagged
	roundTrip(t, sampleFlagged, &f)
	if f.Detector != DetectorRestrictedContent {
		t.Errorf("detector = %q", f.Detector)
	}
	if f.Action != ActionReview {
		t.Errorf("action = %q", f.Action)
	}
	if f.PolicyID != "pol_7a13" {
		t.Errorf("policy_id = %q", f.PolicyID)
	}
}

func TestDeletionRoundTrip(t *testing.T) {
	var d Deletion
	roundTrip(t, sampleDeletion, &d)
	if d.Reason != DetectorRestrictedWord {
		t.Errorf("reason = %q", d.Reason)
	}
}

func TestUsageRoundTrip(t *testing.T) {
	var u Usage
	roundTrip(t, sampleUsage, &u)
	if u.EventType != EventMessagesProcessed {
		t.Errorf("event_type = %q", u.EventType)
	}
	if u.Quantity != 1000 {
		t.Errorf("quantity = %d", u.Quantity)
	}
	if u.IdempotencyKey != "mod-7-14:00-9d4e" {
		t.Errorf("idempotency_key = %q", u.IdempotencyKey)
	}
}

func TestEnums(t *testing.T) {
	detectors := []Detector{
		DetectorRateLimit, DetectorDuplicate, DetectorSemanticSpam,
		DetectorRestrictedWord, DetectorRestrictedContent,
	}
	wantDetectors := []string{
		"rate_limit", "duplicate", "semantic_spam", "restricted_word", "restricted_content",
	}
	for i, d := range detectors {
		if string(d) != wantDetectors[i] {
			t.Errorf("detector %d = %q, want %q", i, d, wantDetectors[i])
		}
	}
	if ActionAutoDelete != "auto_delete" || ActionReview != "review" {
		t.Error("action enum values wrong")
	}
	if EventMessagesProcessed != "messages_processed" || EventMessagesReclustered != "messages_reclustered" {
		t.Error("event_type enum values wrong")
	}
}

func TestTopicNames(t *testing.T) {
	if TopicMessages != "messages.v1" || TopicFlagged != "flagged.v1" ||
		TopicDeletions != "deletions.v1" || TopicUsage != "usage.v1" {
		t.Error("topic name constants wrong")
	}
}
