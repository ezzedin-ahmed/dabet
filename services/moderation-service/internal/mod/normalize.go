// Package mod implements the moderation hot path of docs §7.3: the
// cheap-first, first-hit-wins cascade over messages.v1, verdict publishing
// to flagged.v1/deletions.v1, usage emission, and the fail-open policy of
// §4.7. P4: message text is never logged and never put in metric labels.
package mod

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// Normalize lowercases the text, strips zero-width characters, and
// collapses whitespace runs into single spaces (trimming the ends). The
// duplicate hash and the restricted-word matcher both operate on this form
// so neither is defeated by an added space or an invisible character
// (§7.4).
func Normalize(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	pendingSpace := false
	wrote := false
	for _, r := range strings.ToLower(s) {
		switch {
		case isZeroWidth(r):
			// Dropped entirely: "sp​am" hashes like "spam".
		case unicode.IsSpace(r):
			pendingSpace = true
		default:
			if pendingSpace && wrote {
				b.WriteByte(' ')
			}
			pendingSpace = false
			wrote = true
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isZeroWidth(r rune) bool {
	switch r {
	case '\u200b', // zero width space
		'\u200c', // zero width non-joiner
		'\u200d', // zero width joiner
		'\u2060', // word joiner
		'\ufeff', // zero width no-break space / BOM
		'\u00ad': // soft hyphen
		return true
	}
	return false
}

// HashText is the duplicate-detector hash: SHA-256 over normalised text.
func HashText(normalized string) string {
	h := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(h[:])
}
