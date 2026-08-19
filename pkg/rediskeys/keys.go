// Package rediskeys builds the Redis keys of docs §4.3. Keys sharing
// state for one (content_id, author_id) pair carry a literal {content:author}
// hash tag from day one, so moving to Redis Cluster is a config change:
// all keys for a pair hash to the same slot.
package rediskeys

// Seen is the redelivery guard key, string, TTL 5 min (§7.4).
// Not hash-tagged: it is keyed by message alone.
func Seen(messageID string) string { return "seen:" + messageID }

// Dup holds the last N message hashes per (content, sender), TTL 5 min.
func Dup(contentID, authorID string) string {
	return "dup:{" + contentID + ":" + authorID + "}"
}

// Emb holds the last N packed embeddings per (content, sender), TTL 5 min.
func Emb(contentID, authorID string) string {
	return "emb:{" + contentID + ":" + authorID + "}"
}

// Rate is the rate-limit token bucket per (content, sender), TTL 2x window.
func Rate(contentID, authorID string) string {
	return "rate:{" + contentID + ":" + authorID + "}"
}

// Samp is the per-content LLM sampling bucket, TTL 5 min (§7.5).
func Samp(contentID string) string { return "samp:{" + contentID + "}" }
