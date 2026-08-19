package mod

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"dabet/pkg/rediskeys"
)

// bucketLua is the shared token-bucket script for the rate limiter (§7.4)
// and the sampler (§7.5): atomic refill-then-take. The clock is passed in
// as ARGV so the caller's clock is authoritative (and tests are
// deterministic); with messages.v1 partitioned by (author, content) a
// single consumer owns each key, so per-key time never runs backwards.
//
// KEYS[1] bucket key; ARGV: capacity, refill tokens/sec, now (seconds,
// fractional), ttl (seconds). Returns 1 if a token was taken, else 0.
const bucketLua = `
local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
local ts = tonumber(redis.call('HGET', KEYS[1], 'ts'))
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
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
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens))
redis.call('HSET', KEYS[1], 'ts', tostring(now))
redis.call('EXPIRE', KEYS[1], ARGV[4])
return allowed
`

// dupLua does membership check + LPUSH + LTRIM + EXPIRE atomically (§7.4).
// KEYS[1] dup list; ARGV: hash, depth, ttl (seconds). Returns 1 on hit.
const dupLua = `
local depth = tonumber(ARGV[2])
local cur = redis.call('LRANGE', KEYS[1], 0, depth - 1)
local hit = 0
for i = 1, #cur do
  if cur[i] == ARGV[1] then
    hit = 1
    break
  end
end
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('LTRIM', KEYS[1], 0, depth - 1)
redis.call('EXPIRE', KEYS[1], ARGV[3])
return hit
`

// RedisState wraps the Redis-backed detector state. Every operation
// returns an error only for infrastructure failures; the pipeline treats
// any error as "Redis down" and fails open per §4.7.
type RedisState struct {
	rdb    redis.UniversalClient
	bucket *redis.Script
	dup    *redis.Script
}

// NewRedisState builds the state layer over an existing client.
func NewRedisState(rdb redis.UniversalClient) *RedisState {
	return &RedisState{
		rdb:    rdb,
		bucket: redis.NewScript(bucketLua),
		dup:    redis.NewScript(dupLua),
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
// pipeline clock.
func (s *RedisState) TakeToken(ctx context.Context, key string, capacity, refillPerSec float64, now time.Time, ttl time.Duration) (bool, error) {
	nowSec := float64(now.UnixNano()) / 1e9
	ttlSec := int64(ttl / time.Second)
	if ttlSec < 1 {
		ttlSec = 1
	}
	n, err := s.bucket.Run(ctx, s.rdb, []string{key}, capacity, refillPerSec, nowSec, ttlSec).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// DupCheck records hash in the recent-hash list at key and reports whether
// it was already present among the last depth entries.
func (s *RedisState) DupCheck(ctx context.Context, key, hash string, depth int, ttl time.Duration) (bool, error) {
	n, err := s.dup.Run(ctx, s.rdb, []string{key}, hash, depth, int64(ttl/time.Second)).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// EmbMaxSimilarity compares vec against the last depth vectors stored at
// key and returns the maximum cosine similarity, then appends vec. This is
// a read-then-write rather than one Lua script; it is race-free because
// messages.v1 partitioning guarantees a single consumer mutates any given
// (content, sender) key (§7.3).
func (s *RedisState) EmbMaxSimilarity(ctx context.Context, key string, vec []float32, depth int, ttl time.Duration) (float64, error) {
	vals, err := s.rdb.LRange(ctx, key, 0, int64(depth-1)).Result()
	if err != nil {
		return 0, err
	}
	maxSim := 0.0
	for _, v := range vals {
		prev, ok := unpackVector([]byte(v))
		if !ok {
			continue
		}
		if c := cosine(vec, prev); c > maxSim {
			maxSim = c
		}
	}
	pipe := s.rdb.TxPipeline()
	pipe.LPush(ctx, key, packVector(vec))
	pipe.LTrim(ctx, key, 0, int64(depth-1))
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return maxSim, nil
}
