package mod

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"dabet/pkg/rediskeys"
)

// This file is the durable half of the Redis Cluster work.
//
// §4.3 put a literal {content:author} hash tag on every key so that all
// state for one pair lands in one slot, and §4.3 also says every bucket
// operation is a Lua script so the read-modify-write is atomic. Those two
// facts only compose if each script's KEYS all share one tag: Redis
// Cluster refuses a script (and a MULTI) whose keys span slots, and no
// amount of hash tagging saves a script that reaches across families.
//
// The tag families are genuinely different, and deliberately so:
//
//	seen:{message_id}          tagged by message
//	dup:{content:author}       \
//	rate:{content:author}       > tagged by the (content, author) pair
//	emb:{content:author}       /
//	samp:{content_id}          tagged by content alone
//
// So "hash tags are applied" is not by itself enough. The tests below
// check the thing that actually matters — that no single script, and no
// single transaction, touches two of those families — from three angles:
// a static scan of the scripts, a static scan of the call sites, and a
// slot-checking hook on a live client that watches the real operations
// run. Add a cross-tag script later and at least one of them fails.

// ---------------------------------------------------------------------------
// Redis Cluster's key -> slot function (CRC16-CCITT of the hash tag, mod
// 16384). Implemented here rather than imported because go-redis keeps its
// copy in an internal package.
// ---------------------------------------------------------------------------

func crc16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc ^= uint16(c) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// hashTag returns the substring Redis actually hashes: the first non-empty
// {...} span, or the whole key when there is none.
func hashTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	closeIdx := strings.IndexByte(key[open+1:], '}')
	if closeIdx <= 0 {
		return key
	}
	return key[open+1 : open+1+closeIdx]
}

func keySlot(key string) uint16 { return crc16([]byte(hashTag(key))) % 16384 }

// TestSlotFunctionMatchesRedis pins the implementation above against the
// values in the Redis Cluster specification, so a bug in the checker
// cannot make the checks below vacuously pass.
func TestSlotFunctionMatchesRedis(t *testing.T) {
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Fatalf("CRC16(\"123456789\") = %#04x, want 0x31c3", got)
	}
	// The spec's worked example: these two keys hash to the same slot
	// because only what is between the braces is hashed.
	if keySlot("foo{hash_tag}") != keySlot("bar{hash_tag}") {
		t.Fatal("keys sharing a hash tag must share a slot")
	}
	if keySlot("{user1000}.following") != keySlot("{user1000}.followers") {
		t.Fatal("spec example: {user1000} keys must share a slot")
	}
	// An empty or unterminated tag hashes the whole key.
	if keySlot("foo{}bar") != keySlot("foo{}bar"[0:]) || hashTag("foo{}bar") != "foo{}bar" {
		t.Fatal("an empty tag must hash the whole key")
	}
	if hashTag("foo{bar") != "foo{bar" {
		t.Fatal("an unterminated tag must hash the whole key")
	}
}

// ---------------------------------------------------------------------------
// The audit itself, family by family.
// ---------------------------------------------------------------------------

// TestKeyFamilySlots documents the audit result as executable fact: which
// keys share a slot and which do not. Anyone changing rediskeys must come
// through here.
func TestKeyFamilySlots(t *testing.T) {
	const contentID, authorID, messageID = "ct-1", "au-1", "msg-1"

	pair := map[string]string{
		"dup":  rediskeys.Dup(contentID, authorID),
		"rate": rediskeys.Rate(contentID, authorID),
		"emb":  rediskeys.Emb(contentID, authorID),
	}
	var slots []uint16
	for name, key := range pair {
		if tag := hashTag(key); tag != contentID+":"+authorID {
			t.Errorf("%s key %q hash tag = %q, want %q", name, key, tag, contentID+":"+authorID)
		}
		slots = append(slots, keySlot(key))
	}
	for _, s := range slots {
		if s != slots[0] {
			t.Fatalf("dup/rate/emb for one pair span slots %v; §4.3's tag is not doing its job", slots)
		}
	}

	// The other two families are, correctly, elsewhere — and that is
	// exactly why no script may combine them.
	seen := rediskeys.Seen(messageID)
	samp := rediskeys.Samp(contentID)
	if hashTag(seen) != seen {
		t.Errorf("seen key %q unexpectedly carries a hash tag", seen)
	}
	if hashTag(samp) != contentID {
		t.Errorf("samp key %q hash tag = %q, want %q", samp, hashTag(samp), contentID)
	}
	if keySlot(seen) == slots[0] || keySlot(samp) == slots[0] || keySlot(seen) == keySlot(samp) {
		t.Log("note: two families collided into one slot for this sample id; that is luck, not a guarantee")
	}
}

// ---------------------------------------------------------------------------
// Static scan 1: every Lua script in this package uses KEYS[1] and nothing
// else. A script that read KEYS[2] would be a cross-slot failure waiting
// for cluster mode, however the caller happened to tag its keys.
// ---------------------------------------------------------------------------

var keysIndexRe = regexp.MustCompile(`KEYS\s*\[\s*([^\]]*?)\s*\]`)

func TestEveryLuaScriptUsesOnlyKEYS1(t *testing.T) {
	scripts := 0
	for file, lit := range packageStringLiterals(t) {
		if !strings.Contains(lit.value, "KEYS[") && !strings.Contains(lit.value, "KEYS [") {
			continue
		}
		scripts++
		for _, m := range keysIndexRe.FindAllStringSubmatch(lit.value, -1) {
			if m[1] != "1" {
				t.Errorf("%s:%d: Lua script indexes KEYS[%s]; in Redis Cluster a script may only touch "+
					"keys in one slot, and §4.3's hash tags cannot rescue a second key from another family. "+
					"Either give both keys the same tag or split the script.", file, lit.line, m[1])
			}
		}
		if strings.Contains(lit.value, "#KEYS") {
			t.Errorf("%s:%d: Lua script iterates over #KEYS; the key count is then unauditable statically. "+
				"Keep scripts to a single key.", file, lit.line)
		}
	}
	// bucketLua and dupLua. If a rename or a move makes this zero the scan
	// is checking nothing, which is worse than failing.
	if scripts < 2 {
		t.Fatalf("found %d Lua scripts to audit, want at least the 2 in redisstate.go", scripts)
	}
}

// ---------------------------------------------------------------------------
// Static scan 2: every script invocation passes at most one key. A script
// that only ever reads KEYS[1] but is handed two keys still fails in
// cluster mode, because the client routes on the whole key list.
// ---------------------------------------------------------------------------

func TestEveryScriptCallPassesOneKey(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nonTestGoFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range pkgs {
		ast.Inspect(pkg, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Run", "Eval", "EvalSha", "EvalRO", "EvalShaRO", "RunRO":
			default:
				return true
			}
			for _, arg := range call.Args {
				lit, ok := arg.(*ast.CompositeLit)
				if !ok {
					continue
				}
				at, ok := lit.Type.(*ast.ArrayType)
				if !ok {
					continue
				}
				if id, ok := at.Elt.(*ast.Ident); !ok || id.Name != "string" {
					continue
				}
				if len(lit.Elts) > 1 {
					pos := fset.Position(lit.Pos())
					t.Errorf("%s: %s is called with %d keys; a Redis Cluster script may only touch keys "+
						"in one slot. Confirm they share a §4.3 hash tag, or split the call.",
						pos, sel.Sel.Name, len(lit.Elts))
				}
			}
			return true
		})
	}
}

// ---------------------------------------------------------------------------
// Dynamic check: run every RedisState operation against miniredis with a
// hook that computes the slot of every key the client actually sends, and
// fail if any single command or transaction spans two slots. This is the
// one that catches a cross-tag key built at runtime, which no source scan
// can see.
// ---------------------------------------------------------------------------

// singleKeyAtArg1 lists the commands this package sends whose only key is
// the first argument. A command outside this set (and outside noKeyCmds)
// fails the audit rather than being waved through, so adding a multi-key
// command is a deliberate act.
var singleKeyAtArg1 = map[string]bool{
	"set": true, "setnx": true, "get": true, "getset": true,
	"lrange": true, "lpush": true, "ltrim": true, "llen": true,
	"expire": true, "pexpire": true, "ttl": true, "del": true, "unlink": true,
	"hget": true, "hset": true, "hgetall": true, "incr": true, "decr": true,
	"sadd": true, "srem": true, "smembers": true, "zadd": true,
}

// noKeyCmds carry no key at all and are therefore slot-free.
var noKeyCmds = map[string]bool{
	"multi": true, "exec": true, "discard": true, "ping": true,
	"script": true, "hello": true, "auth": true, "select": true,
	"client": true, "info": true, "command": true,
}

type slotAudit struct {
	t *testing.T
}

func (h *slotAudit) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *slotAudit) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.assertOneSlot("command", []redis.Cmder{cmd})
		return next(ctx, cmd)
	}
}

func (h *slotAudit) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		// A pipeline is not atomic, but a TxPipeline is a MULTI/EXEC and
		// Redis Cluster rejects one that spans slots outright — and
		// go-redis cannot even route it. Hold both to the same rule.
		h.assertOneSlot("pipeline", cmds)
		return next(ctx, cmds)
	}
}

func (h *slotAudit) assertOneSlot(kind string, cmds []redis.Cmder) {
	h.t.Helper()
	var keys []string
	var names []string
	for _, cmd := range cmds {
		name, cmdKeys := commandKeys(h.t, cmd)
		if len(cmdKeys) > 1 {
			// Report the per-command case precisely: this is the shape
			// that breaks a Lua script.
			if !sameSlot(cmdKeys) {
				h.t.Errorf("%s %q spans slots with keys %v: a Redis Cluster script/command may only "+
					"touch one slot", kind, name, cmdKeys)
			}
		}
		if len(cmdKeys) > 0 {
			names = append(names, name)
			keys = append(keys, cmdKeys...)
		}
	}
	if kind == "pipeline" && len(keys) > 1 && !sameSlot(keys) {
		h.t.Errorf("transaction over %v spans slots with keys %v: Redis Cluster cannot execute it",
			names, keys)
	}
}

func sameSlot(keys []string) bool {
	if len(keys) < 2 {
		return true
	}
	first := keySlot(keys[0])
	for _, k := range keys[1:] {
		if keySlot(k) != first {
			return false
		}
	}
	return true
}

// commandKeys returns the command name and the keys it addresses. EVAL and
// EVALSHA carry an explicit key count, which is exactly the number Redis
// Cluster routes on.
func commandKeys(t *testing.T, cmd redis.Cmder) (string, []string) {
	t.Helper()
	args := cmd.Args()
	if len(args) == 0 {
		return "", nil
	}
	name := strings.ToLower(fmt.Sprint(args[0]))
	switch name {
	case "eval", "evalsha", "eval_ro", "evalsha_ro":
		if len(args) < 3 {
			return name, nil
		}
		n, err := strconv.Atoi(fmt.Sprint(args[2]))
		if err != nil {
			t.Errorf("%s: unparseable numkeys %v", name, args[2])
			return name, nil
		}
		if n > 1 {
			t.Errorf("%s is invoked with numkeys=%d; §4.3's scripts must stay single-key so they are "+
				"legal in Redis Cluster", name, n)
		}
		var keys []string
		for i := 0; i < n && 3+i < len(args); i++ {
			keys = append(keys, fmt.Sprint(args[3+i]))
		}
		return name, keys
	}
	if noKeyCmds[name] {
		return name, nil
	}
	if !singleKeyAtArg1[name] {
		t.Errorf("Redis command %q is not classified in redisslots_test.go; add it to singleKeyAtArg1 "+
			"or noKeyCmds after checking how many keys it takes, so the cluster audit stays honest", name)
		return name, nil
	}
	if len(args) < 2 {
		return name, nil
	}
	return name, []string{fmt.Sprint(args[1])}
}

// TestEveryRedisOperationStaysInOneSlot drives every RedisState method
// against miniredis with the audit hook attached.
func TestEveryRedisOperationStaysInOneSlot(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	rdb.AddHook(&slotAudit{t: t})

	s := NewRedisState(rdb)
	ctx := context.Background()
	const contentID, authorID = "ct-1", "au-1"

	if _, err := s.Seen(ctx, "msg-1", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TakeToken(ctx, rediskeys.Rate(contentID, authorID), 3, 1, t0, 6*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TakeToken(ctx, rediskeys.Samp(contentID), 30, 0.5, t0, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DupCheck(ctx, rediskeys.Dup(contentID, authorID), "h1", 3, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	// Twice, so the second call exercises the read-then-TxPipeline path
	// with a non-empty history.
	for i := 0; i < 2; i++ {
		if _, err := s.EmbMaxSimilarity(ctx, rediskeys.Emb(contentID, authorID),
			[]float32{1, 0, 0}, 2, 5*time.Minute); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSlotAuditCatchesACrossTagTransaction proves the hook above is not
// vacuous: a transaction that deliberately mixes two tag families is
// reported.
func TestSlotAuditCatchesACrossTagTransaction(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	spy := &testing.T{}
	rdb.AddHook(&slotAudit{t: spy})

	ctx := context.Background()
	pipe := rdb.TxPipeline()
	pipe.Set(ctx, rediskeys.Dup("ct-1", "au-1"), "x", time.Minute)
	pipe.Set(ctx, rediskeys.Samp("ct-1"), "y", time.Minute)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if !spy.Failed() {
		t.Fatal("the slot audit did not notice a transaction spanning two hash-tag families")
	}
}

// ---------------------------------------------------------------------------
// helpers for the static scans
// ---------------------------------------------------------------------------

type stringLit struct {
	value string
	line  int
}

func nonTestGoFile(fi fs.FileInfo) bool {
	return strings.HasSuffix(fi.Name(), ".go") && !strings.HasSuffix(fi.Name(), "_test.go")
}

// packageStringLiterals returns every string literal in the package's
// non-test sources, keyed by "file#n" so the map cannot collide.
func packageStringLiterals(t *testing.T) map[string]stringLit {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nonTestGoFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]stringLit{}
	var names []string
	for name := range pkgs {
		names = append(names, name)
	}
	sort.Strings(names)
	n := 0
	for _, name := range names {
		ast.Inspect(pkgs[name], func(node ast.Node) bool {
			lit, ok := node.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			v, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			pos := fset.Position(lit.Pos())
			n++
			out[fmt.Sprintf("%s#%d", pos.Filename, n)] = stringLit{value: v, line: pos.Line}
			return true
		})
	}
	if n == 0 {
		t.Fatal("parsed no string literals; the scan would be vacuous")
	}
	return out
}
