// Package opaque mints and resolves the adapter-issued opaque IDs of docs
// §1.4 and §7.2:
//
//	content_id = base62(platform_tag || hash(platform_native_channel_id))
//
// The platform tag is embedded so the adapter can route a deletion back to
// the right driver from content_id alone, without a lookup. The values are
// opaque to every other service: <=64 chars, never parsed, pattern-matched,
// or platform-inferred outside provider-adapter (P5). The human-friendly
// "ct_"/"sd_"/"pm_" prefixes are cosmetic and carry no contract.
package opaque

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
)

// MaxIDLen is the contract ceiling for adapter-issued IDs (§4.2).
const MaxIDLen = 64

// ID prefixes. prefixMsgRaw carries the platform-native message id
// reversibly (tag || native bytes); prefixMsgHashed is the fallback when
// the native id is too long to fit reversibly in MaxIDLen, in which case
// the native id is remembered in the Index.
const (
	prefixContent   = "ct_"
	prefixSender    = "sd_"
	prefixMsgRaw    = "pm_"
	prefixMsgHashed = "ph_"
)

// hashLen is how many bytes of sha256(native id) go into a hashed payload.
// 12 bytes (96 bits) keeps collision probability negligible at any
// realistic channel/sender cardinality while staying far under MaxIDLen.
const hashLen = 12

// Platform tags. One byte, embedded as the first payload byte. Adding a
// platform is one new Driver implementation and one entry here (§7.2).
var tagByPlatform = map[string]byte{
	"youtube": 0x01,
	"twitch":  0x02,
	"discord": 0x03,
	"mock":    0x7F,
}

var platformByTag = func() map[byte]string {
	m := make(map[byte]string, len(tagByPlatform))
	for p, t := range tagByPlatform {
		m[t] = p
	}
	return m
}()

func tag(platform string) (byte, error) {
	t, ok := tagByPlatform[platform]
	if !ok {
		return 0, fmt.Errorf("opaque: unknown platform %q", platform)
	}
	return t, nil
}

func hashedPayload(t byte, native string) []byte {
	h := sha256.Sum256([]byte(native))
	return append([]byte{t}, h[:hashLen]...)
}

// MintContentID mints the opaque content_id for a platform-native channel
// id. Deterministic: the same channel always mints the same id.
func MintContentID(platform, nativeChannelID string) (string, error) {
	t, err := tag(platform)
	if err != nil {
		return "", err
	}
	return prefixContent + encodeBase62(hashedPayload(t, nativeChannelID)), nil
}

// MintAuthorID mints the opaque author (sender) id for a platform-native
// author id. Deduplication key only (§1.4); deterministic.
func MintAuthorID(platform, nativeAuthorID string) (string, error) {
	t, err := tag(platform)
	if err != nil {
		return "", err
	}
	return prefixSender + encodeBase62(hashedPayload(t, nativeAuthorID)), nil
}

// MintMessageID mints the opaque message_id from the platform-native
// message id. When the native id fits, it is embedded reversibly (tag ||
// native bytes) so a deletion can recover it with no lookup; otherwise the
// id is hashed and reversible=false tells the caller to remember the
// native id (Minter does this via its Index).
func MintMessageID(platform, nativeMessageID string) (id string, reversible bool, err error) {
	t, err := tag(platform)
	if err != nil {
		return "", false, err
	}
	raw := prefixMsgRaw + encodeBase62(append([]byte{t}, nativeMessageID...))
	if len(raw) <= MaxIDLen {
		return raw, true, nil
	}
	return prefixMsgHashed + encodeBase62(hashedPayload(t, nativeMessageID)), false, nil
}

// payload strips a known prefix and decodes the base62 payload.
func payload(id string) ([]byte, error) {
	for _, p := range []string{prefixContent, prefixSender, prefixMsgRaw, prefixMsgHashed} {
		if rest, ok := strings.CutPrefix(id, p); ok {
			b, err := decodeBase62(rest)
			if err != nil {
				return nil, err
			}
			if len(b) == 0 {
				return nil, fmt.Errorf("opaque: empty payload")
			}
			return b, nil
		}
	}
	return nil, fmt.Errorf("opaque: unrecognized id shape")
}

// Platform resolves the platform tag embedded in any adapter-minted id
// (content, author, or message). This is the routing half of the contract:
// the deletion consumer finds the owning driver from content_id alone.
func Platform(id string) (string, error) {
	b, err := payload(id)
	if err != nil {
		return "", err
	}
	p, ok := platformByTag[b[0]]
	if !ok {
		return "", fmt.Errorf("opaque: unknown platform tag 0x%02x", b[0])
	}
	return p, nil
}

// nativeFromMessageID recovers the native message id from a reversible
// message_id. Returns false for hashed message ids (Index handles those).
func nativeFromMessageID(id string) (string, bool) {
	rest, ok := strings.CutPrefix(id, prefixMsgRaw)
	if !ok {
		return "", false
	}
	b, err := decodeBase62(rest)
	if err != nil || len(b) < 2 {
		return "", false
	}
	return string(b[1:]), true
}

// Index remembers opaque->native mappings this instance has minted:
// content ids (hashes are one-way) and the rare over-long message ids.
// In-memory only — deletions for content ingested by another instance or
// before a restart resolve platform fine but may miss the native mapping;
// the deletion consumer counts those as not_found (best-effort by design,
// §7.2).
type Index struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewIndex returns an empty index.
func NewIndex() *Index { return &Index{m: make(map[string]string)} }

func (i *Index) put(opaqueID, native string) {
	i.mu.Lock()
	i.m[opaqueID] = native
	i.mu.Unlock()
}

func (i *Index) get(opaqueID string) (string, bool) {
	i.mu.RLock()
	n, ok := i.m[opaqueID]
	i.mu.RUnlock()
	return n, ok
}

// Minter mints opaque IDs and records the mappings needed to resolve them
// back. It implements driver.Resolver.
type Minter struct {
	idx *Index
}

// NewMinter returns a Minter over a fresh Index.
func NewMinter() *Minter { return &Minter{idx: NewIndex()} }

// ContentID mints a content_id and remembers its native channel id.
func (m *Minter) ContentID(platform, nativeChannelID string) (string, error) {
	id, err := MintContentID(platform, nativeChannelID)
	if err != nil {
		return "", err
	}
	m.idx.put(id, nativeChannelID)
	return id, nil
}

// AuthorID mints an author_id. Authors never need resolving back, so
// nothing is indexed.
func (m *Minter) AuthorID(platform, nativeAuthorID string) (string, error) {
	return MintAuthorID(platform, nativeAuthorID)
}

// MessageID mints a message_id, indexing the native id when the hashed
// fallback was used.
func (m *Minter) MessageID(platform, nativeMessageID string) (string, error) {
	id, reversible, err := MintMessageID(platform, nativeMessageID)
	if err != nil {
		return "", err
	}
	if !reversible {
		m.idx.put(id, nativeMessageID)
	}
	return id, nil
}

// NativeContentID implements driver.Resolver.
func (m *Minter) NativeContentID(contentID string) (string, bool) {
	return m.idx.get(contentID)
}

// NativeMessageID implements driver.Resolver.
func (m *Minter) NativeMessageID(messageID string) (string, bool) {
	if native, ok := nativeFromMessageID(messageID); ok {
		return native, true
	}
	return m.idx.get(messageID)
}
