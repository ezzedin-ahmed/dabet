package mod

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"dabet/pkg/rediskeys"
)

// takeTokenLua is the token-bucket body shared by the rate limiter (§7.4)
// and the sampler (§7.5): atomic refill-then-take. It is a Lua function
// rather than two copies of the arithmetic because the same bucket now
// runs from two scripts — the sampler's own, and the merged cascade below
// — and A17's ceiling and the rate limiter's window must not be able to
// drift apart.
//
// The clock is passed in as ARGV so the caller's clock is authoritative
// (and tests are deterministic); with messages.v1 partitioned by (author,
// content) a single consumer owns each key, so per-key time never runs
// backwards.
const takeTokenLua = `
local function take_token(key, capacity, refill, now, ttl)
  local tokens = tonumber(redis.call('HGET', key, 'tokens'))
  local ts = tonumber(redis.call('HGET', key, 'ts'))
  if tokens == nil or ts == nil then
    tokens = capacity
    ts = now
  end
  local elapsed = now - ts
  if elapsed < 0 then
    elapsed = 0
  end
  tokens = tokens + elapsed * refill
  if tokens > capacity then
    tokens = capacity
  end
  local allowed = 0
  if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
  end
  redis.call('HSET', key, 'tokens', tostring(tokens))
  redis.call('HSET', key, 'ts', tostring(now))
  redis.call('EXPIRE', key, ttl)
  return allowed
end
`

// bucketLua is the standalone token bucket, still used by the sampler,
// whose key (samp:{content_id}) is in a different slot family from the
// (content, author) keys and therefore may never share a script with them.
//
// KEYS[1] bucket key; ARGV: capacity, refill tokens/sec, now (seconds,
// fractional), ttl (seconds). Returns 1 if a token was taken, else 0.
const bucketLua = takeTokenLua + `
return take_token(KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4])
`

// cascadeLua is stages 4, 5 and 6 of §7.3 in one round trip.
//
// WHY ONE SCRIPT. rate:{ct:au}, dup:{ct:au} and emb:{ct:au} carry the same
// §4.3 hash tag, so Redis Cluster routes all three to one slot and one
// script may own them. seen:<message_id> and samp:{content_id} carry
// different tags and stay separate calls — see redisslots_test.go, which
// is the standing proof that this line is not crossed.
//
// WHY IT RETURNS EARLY. §7.3 is first-hit-wins, ordered strictly by cost,
// and the ordering is observable in the STATE as much as in the verdict: a
// message stopped by the rate limiter must not enter the duplicate window,
// and a message stopped by the duplicate detector must not be compared
// against — or appended to — the embedding window. So the script returns
// the instant a stage hits and touches nothing below that point. A merged
// script that updated all three structures unconditionally would silently
// change later verdicts, which is the one regression this merge could
// introduce.
//
//	KEYS[1] rate bucket, KEYS[2] duplicate window, KEYS[3] embedding window
//
//	ARGV[1]  rate stage enabled ("1"/"0")
//	ARGV[2]  rate capacity            ARGV[3] rate refill tokens/sec
//	ARGV[4]  now (fractional seconds) ARGV[5] rate TTL (seconds)
//	ARGV[6]  duplicate stage enabled ("1"/"0")
//	ARGV[7]  message hash             ARGV[8] depth     ARGV[9] TTL (seconds)
//	ARGV[10] semantic stage enabled ("1"/"0")
//	ARGV[11] embedding window depth
//
// Returns a flat array whose first element is the stage that hit — 0 none,
// 1 rate limit, 2 duplicate — followed, only in the 0 case with the
// semantic stage on, by the stored vectors, newest first. The cosine
// comparison itself stays in Go (see EmbAppend).
const cascadeLua = takeTokenLua + `
if ARGV[1] == '1' then
  if take_token(KEYS[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[5]) == 0 then
    return {1}
  end
end

if ARGV[6] == '1' then
  local depth = tonumber(ARGV[8])
  local cur = redis.call('LRANGE', KEYS[2], 0, depth - 1)
  local dup = 0
  for i = 1, #cur do
    if cur[i] == ARGV[7] then
      dup = 1
      break
    end
  end
  redis.call('LPUSH', KEYS[2], ARGV[7])
  redis.call('LTRIM', KEYS[2], 0, depth - 1)
  redis.call('EXPIRE', KEYS[2], ARGV[9])
  if dup == 1 then
    return {2}
  end
end

local out = {0}
if ARGV[10] == '1' then
  local vecs = redis.call('LRANGE', KEYS[3], 0, tonumber(ARGV[11]) - 1)
  for i = 1, #vecs do
    out[i + 1] = vecs[i]
  end
end
return out
`

// RedisState wraps the Redis-backed detector state. Every operation
// returns an error only for infrastructure failures; the pipeline treats
// any error as "Redis down" and fails open per §4.7.
type RedisState struct {
	rdb     redis.UniversalClient
	bucket  *redis.Script
	cascade *redis.Script
}

// NewRedisState builds the state layer over an existing client.
func NewRedisState(rdb redis.UniversalClient) *RedisState {
	return &RedisState{
		rdb:     rdb,
		bucket:  redis.NewScript(bucketLua),
		cascade: redis.NewScript(cascadeLua),
	}
}

// Seen implements the redelivery guard: SET seen:{message_id} 1 NX EX ttl.
// Returns true when the key already existed (a Kafka redelivery).
func (s *RedisState) Seen(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
	set, err := s.rdb.SetNX(ctx, rediskeys.Seen(messageID), "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return !set, nil
}

// TakeToken runs the token-bucket script against key. now comes from the
// pipeline clock. The rate limiter reaches the same arithmetic through
// Cascade; this entry point serves the sampler's own key (§7.5).
func (s *RedisState) TakeToken(ctx context.Context, key string, capacity, refillPerSec float64, now time.Time, ttl time.Duration) (bool, error) {
	nowSec := float64(now.UnixNano()) / 1e9
	n, err := s.bucket.Run(ctx, s.rdb, []string{key}, capacity, refillPerSec, nowSec, bucketTTLSeconds(ttl)).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// RateParams configures the rate-limit stage of one Cascade call.
type RateParams struct {
	Capacity     float64 // rate_limit_messages
	RefillPerSec float64 // messages / seconds
	Now          time.Time
	TTL          time.Duration // 2x the window (§7.4)
}

// DupParams configures the duplicate stage of one Cascade call.
type DupParams struct {
	Hash  string // HashText over the normalised text
	Depth int    // MOD_DUP_DEPTH (A15)
	TTL   time.Duration
}

// CascadeParams selects which of the three merged stages run. A nil
// pointer, or a zero EmbDepth, means the policy does not enable that stage
// and the script must not touch its key at all.
type CascadeParams struct {
	Rate     *RateParams
	Dup      *DupParams
	EmbDepth int // MOD_EMB_DEPTH; 0 = semantic stage off
}

// CascadeHit names the stage that stopped the message, if any.
type CascadeHit int

const (
	// CascadeNone means no merged stage hit; the message continues.
	CascadeNone CascadeHit = 0
	// CascadeRateLimited means the token bucket was empty (§7.4).
	CascadeRateLimited CascadeHit = 1
	// CascadeDuplicate means the hash was in the last-N window (A15).
	CascadeDuplicate CascadeHit = 2
)

// CascadeResult is one merged call's outcome. Vectors holds the embedding
// comparison window read for the semantic stage — populated only when Hit
// is CascadeNone and CascadeParams.EmbDepth was set, because the earlier
// stages short-circuit before the read.
type CascadeResult struct {
	Hit     CascadeHit
	Vectors [][]float32
}

// Cascade runs stages 4–6 of §7.3 in one round trip over the three keys of
// one (content, author) pair. It short-circuits internally, in cost order,
// exactly as the three separate calls it replaces did.
func (s *RedisState) Cascade(ctx context.Context, contentID, authorID string, p CascadeParams) (CascadeResult, error) {
	args := make([]any, 0, 11)
	if p.Rate != nil {
		args = append(args, 1, p.Rate.Capacity, p.Rate.RefillPerSec,
			float64(p.Rate.Now.UnixNano())/1e9, bucketTTLSeconds(p.Rate.TTL))
	} else {
		args = append(args, 0, 0, 0, 0, 1)
	}
	if p.Dup != nil {
		args = append(args, 1, p.Dup.Hash, p.Dup.Depth, int64(p.Dup.TTL/time.Second))
	} else {
		args = append(args, 0, "", 0, 1)
	}
	if p.EmbDepth > 0 {
		args = append(args, 1, p.EmbDepth)
	} else {
		args = append(args, 0, 0)
	}

	// The three keys are built here, inline in the call, rather than passed
	// in as strings: it is what lets redisslots_test.go prove statically
	// that every key of this multi-key script comes from one §4.3 hash tag.
	raw, err := s.cascade.Run(ctx, s.rdb, []string{
		rediskeys.Rate(contentID, authorID),
		rediskeys.Dup(contentID, authorID),
		rediskeys.Emb(contentID, authorID),
	}, args...).Slice()
	if err != nil {
		return CascadeResult{}, err
	}
	return decodeCascade(raw)
}

// decodeCascade turns the script's flat reply into a result. A reply that
// does not match the contract is an error, so the pipeline fails the
// message open rather than inventing a verdict from it.
func decodeCascade(raw []any) (CascadeResult, error) {
	if len(raw) == 0 {
		return CascadeResult{}, fmt.Errorf("moderation cascade script returned an empty reply")
	}
	code, ok := raw[0].(int64)
	if !ok {
		return CascadeResult{}, fmt.Errorf("moderation cascade script returned stage %T, want an integer", raw[0])
	}
	hit := CascadeHit(code)
	switch hit {
	case CascadeNone, CascadeRateLimited, CascadeDuplicate:
	default:
		return CascadeResult{}, fmt.Errorf("moderation cascade script returned unknown stage %d", code)
	}
	res := CascadeResult{Hit: hit}
	for _, v := range raw[1:] {
		packed, ok := v.(string)
		if !ok {
			continue
		}
		// A payload that is not a whole number of float32s is skipped, as
		// it was when the pipeline read the list itself: one corrupt entry
		// must not fail the message open.
		if vec, ok := unpackVector([]byte(packed)); ok {
			res.Vectors = append(res.Vectors, vec)
		}
	}
	return res, nil
}

// EmbAppend records vec in the comparison window for one (content, author)
// pair, trimming to depth and refreshing the TTL. It is the write half of
// the semantic stage; the read half rides the merged Cascade call and the
// cosine itself is computed in Go (maxSimilarity).
//
// It is race-free because messages.v1 partitioning guarantees a single
// consumer mutates any given (content, sender) key (§7.3), which is the
// same property that let the previous read-then-write pair be safe.
func (s *RedisState) EmbAppend(ctx context.Context, contentID, authorID string, vec []float32, depth int, ttl time.Duration) error {
	key := rediskeys.Emb(contentID, authorID)
	pipe := s.rdb.TxPipeline()
	pipe.LPush(ctx, key, packVector(vec))
	pipe.LTrim(ctx, key, 0, int64(depth-1))
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// maxSimilarity is A16's comparison: the largest cosine between vec and
// the stored window, 0 for an empty window. It stays in Go — Lua 5.1 has
// no float32, so a Redis-side cosine would compare in a different
// precision from the one Insights uses on the same vectors (§8.4) and
// could move a borderline message across the 0.95 threshold. The whole
// window is already in hand from the merged call, so keeping it here costs
// nothing.
func maxSimilarity(vec []float32, window [][]float32) float64 {
	maxSim := 0.0
	for _, prev := range window {
		if c := cosine(vec, prev); c > maxSim {
			maxSim = c
		}
	}
	return maxSim
}

// bucketTTLSeconds is EXPIRE's argument for a token bucket, floored at one
// second so a sub-second window cannot make the bucket evict itself on
// every write.
func bucketTTLSeconds(ttl time.Duration) int64 {
	if sec := int64(ttl / time.Second); sec >= 1 {
		return sec
	}
	return 1
}
