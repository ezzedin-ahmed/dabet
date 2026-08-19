package contracts

import (
	"crypto/sha256"
	"encoding/hex"
)

// MessagesKey is the messages.v1 partition key: hash(author_id, content_id)
// so all messages from one sender in one stream land on one partition in
// order, which is what makes the Redis dedup and rate-limit state race-free
// without locking (§4.2). Deterministic; the NUL separator keeps
// ("a","bc") distinct from ("ab","c").
func MessagesKey(authorID, contentID string) []byte {
	h := sha256.Sum256([]byte(authorID + "\x00" + contentID))
	return []byte(hex.EncodeToString(h[:16]))
}

// FlaggedKey keys flagged.v1 by creator_id, making a creator's review
// queue a single contiguous partition read (§7.6).
func FlaggedKey(creatorID string) []byte { return []byte(creatorID) }

// DeletionsKey keys deletions.v1 by content_id so the adapter can batch
// deletions per stream.
func DeletionsKey(contentID string) []byte { return []byte(contentID) }

// UsageKey keys usage.v1 by creator_id.
func UsageKey(creatorID string) []byte { return []byte(creatorID) }
