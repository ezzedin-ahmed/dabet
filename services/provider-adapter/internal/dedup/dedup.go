// Package dedup is a bounded, insertion-ordered set of recently seen ids.
//
// Both WebSocket protocols can deliver the same chat message twice, and in
// both cases it happens exactly when a reconnect overlaps a live stream:
// Twitch keeps the old EventSub socket delivering events until the
// replacement socket has been welcomed, and a Discord RESUME replays the
// dispatches after the last sequence number we acknowledged — which is the
// last one we *processed*, not necessarily the last one that was sent.
//
// Deduplicating in the driver rather than downstream is deliberate. The
// duplicate detector in moderation (§7.4, A15) hashes message *text* per
// (content, sender) to catch a spammer repeating themselves; a redelivered
// frame is a transport artefact with the same platform message id, and
// letting it through would bill the creator twice (§7.10), double-count
// adapter_ingest_total, and make a viewer who said "gg" twice on purpose
// indistinguishable from a reconnect.
//
// Capacity bounds memory: a stream doing 100 messages/second replays at
// most a few seconds' worth across a reconnect, so a window of ~2 000 ids
// covers every realistic overlap while costing tens of kilobytes.
package dedup

// DefaultCapacity is the id window each socket keeps.
const DefaultCapacity = 2048

// Set remembers the last N ids in insertion order. It is not safe for
// concurrent use; each watch session owns one.
type Set struct {
	capacity int
	seen     map[string]struct{}
	order    []string
	next     int
}

// New returns a Set holding at most capacity ids; capacity <= 0 means
// DefaultCapacity.
func New(capacity int) *Set {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Set{
		capacity: capacity,
		seen:     make(map[string]struct{}, capacity),
		order:    make([]string, capacity),
	}
}

// Add records id and reports whether it is new. An empty id is always
// reported as new and never recorded: a provider that omits the id has
// given us nothing to deduplicate on, and silently collapsing such
// messages would drop real chat.
func (s *Set) Add(id string) bool {
	if id == "" {
		return true
	}
	if _, dup := s.seen[id]; dup {
		return false
	}
	// Evict the slot we are about to overwrite, which is the oldest id.
	if old := s.order[s.next]; old != "" {
		delete(s.seen, old)
	}
	s.order[s.next] = id
	s.next = (s.next + 1) % s.capacity
	s.seen[id] = struct{}{}
	return true
}

// Len reports how many ids are currently remembered.
func (s *Set) Len() int { return len(s.seen) }
