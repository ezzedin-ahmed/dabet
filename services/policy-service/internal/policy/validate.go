package policy

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Validation limits from docs §6.5 (assumption A9). These bound storage
// and, more importantly, LLM prompt size.
const (
	RateLimitMessagesMin = 1
	RateLimitMessagesMax = 1000
	RateLimitSecondsMin  = 1
	RateLimitSecondsMax  = 3600

	RestrictedWordsMax   = 500
	RestrictedWordMinLen = 1
	RestrictedWordMaxLen = 64

	RestrictedContentMax = 20
	TitleMinLen          = 1
	TitleMaxLen          = 100
	DescriptionMinLen    = 1
	DescriptionMaxLen    = 500
	ExamplesMax          = 10
	ExampleMaxLen        = 200
)

// ValidationError renders as 400 validation_failed with the offending
// field and limit in details (docs §6.5). Message must never echo the
// offending value itself: restricted words and content are policy text and
// stay out of error bodies per P4.
type ValidationError struct {
	Field   string
	Limit   any // nil when the rule has no numeric limit
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// Details is the error-envelope details object.
func (e *ValidationError) Details() map[string]any {
	d := map[string]any{"field": e.Field}
	if e.Limit != nil {
		d["limit"] = e.Limit
	}
	return d
}

func fail(field string, limit any, format string, args ...any) *ValidationError {
	return &ValidationError{Field: field, Limit: limit, Message: fmt.Sprintf(format, args...)}
}

// ValidateAndNormalize checks every §6.5 rule and normalises the document
// in place: restricted_words are lowercased and deduplicated (first
// occurrence wins), absent enums get their defaults, and nil slices become
// empty ones. It returns the first violation found, or nil.
func (d *Document) ValidateAndNormalize() *ValidationError {
	// Rate limit: all-or-nothing, enforced before ranges so a lone field
	// is reported as the pairing violation it is.
	if (d.RateLimitMessages == nil) != (d.RateLimitSeconds == nil) {
		missing := "rate_limit_seconds"
		if d.RateLimitMessages == nil {
			missing = "rate_limit_messages"
		}
		return fail(missing, nil, "rate_limit_messages and rate_limit_seconds must be set together or not at all")
	}
	if d.RateLimitMessages != nil {
		if v := *d.RateLimitMessages; v < RateLimitMessagesMin || v > RateLimitMessagesMax {
			limit := RateLimitMessagesMax
			if v < RateLimitMessagesMin {
				limit = RateLimitMessagesMin
			}
			return fail("rate_limit_messages", limit, "rate_limit_messages must be between %d and %d", RateLimitMessagesMin, RateLimitMessagesMax)
		}
		if v := *d.RateLimitSeconds; v < RateLimitSecondsMin || v > RateLimitSecondsMax {
			limit := RateLimitSecondsMax
			if v < RateLimitSecondsMin {
				limit = RateLimitSecondsMin
			}
			return fail("rate_limit_seconds", limit, "rate_limit_seconds must be between %d and %d", RateLimitSecondsMin, RateLimitSecondsMax)
		}
	}

	switch d.Spam {
	case "":
		d.Spam = SpamNone
	case SpamNone, SpamIdentical, SpamSemantic:
	default:
		return fail("spam", nil, "spam must be one of none, identical, semantic")
	}

	switch d.RestrictedContentAction {
	case "":
		d.RestrictedContentAction = RCActionAuto
	case RCActionAuto, RCActionReview:
	default:
		return fail("restricted_content_action", nil, "restricted_content_action must be auto or review")
	}

	if len(d.RestrictedWords) > RestrictedWordsMax {
		return fail("restricted_words", RestrictedWordsMax, "restricted_words exceeds maximum of %d entries", RestrictedWordsMax)
	}
	words := make([]string, 0, len(d.RestrictedWords))
	seen := make(map[string]bool, len(d.RestrictedWords))
	for i, w := range d.RestrictedWords {
		n := utf8.RuneCountInString(w)
		if n < RestrictedWordMinLen {
			return fail(fmt.Sprintf("restricted_words[%d]", i), RestrictedWordMinLen, "restricted_words entries must be at least %d character", RestrictedWordMinLen)
		}
		if n > RestrictedWordMaxLen {
			return fail(fmt.Sprintf("restricted_words[%d]", i), RestrictedWordMaxLen, "restricted_words entries must be at most %d characters", RestrictedWordMaxLen)
		}
		lw := strings.ToLower(w)
		if !seen[lw] {
			seen[lw] = true
			words = append(words, lw)
		}
	}
	d.RestrictedWords = words

	if len(d.RestrictedContent) > RestrictedContentMax {
		return fail("restricted_content", RestrictedContentMax, "restricted_content exceeds maximum of %d entries", RestrictedContentMax)
	}
	for i, e := range d.RestrictedContent {
		if n := utf8.RuneCountInString(e.Title); n < TitleMinLen || n > TitleMaxLen {
			limit := any(TitleMaxLen)
			if n < TitleMinLen {
				limit = TitleMinLen
			}
			return fail(fmt.Sprintf("restricted_content[%d].title", i), limit, "restricted_content title must be between %d and %d characters", TitleMinLen, TitleMaxLen)
		}
		if n := utf8.RuneCountInString(e.Description); n < DescriptionMinLen || n > DescriptionMaxLen {
			limit := any(DescriptionMaxLen)
			if n < DescriptionMinLen {
				limit = DescriptionMinLen
			}
			return fail(fmt.Sprintf("restricted_content[%d].description", i), limit, "restricted_content description must be between %d and %d characters", DescriptionMinLen, DescriptionMaxLen)
		}
		if len(e.Examples) > ExamplesMax {
			return fail(fmt.Sprintf("restricted_content[%d].examples", i), ExamplesMax, "restricted_content examples exceed maximum of %d entries", ExamplesMax)
		}
		for j, ex := range e.Examples {
			if utf8.RuneCountInString(ex) > ExampleMaxLen {
				return fail(fmt.Sprintf("restricted_content[%d].examples[%d]", i, j), ExampleMaxLen, "restricted_content examples must be at most %d characters", ExampleMaxLen)
			}
		}
	}
	if d.RestrictedContent == nil {
		d.RestrictedContent = []RestrictedContentEntry{}
	}
	return nil
}

// ValidateScope checks the scope pair on creation (docs §6.6): for creator
// and platform scopes the scope_id must derive from the caller's own
// creator_id; content scope_ids are opaque and accepted unvalidated (A12).
func ValidateScope(scope Scope, scopeID, creatorID string) *ValidationError {
	if !ValidScope(scope) {
		return fail("scope", nil, "scope must be one of creator, platform, content")
	}
	if scopeID == "" {
		return fail("scope_id", nil, "scope_id is required")
	}
	switch scope {
	case ScopeCreator:
		if scopeID != creatorID {
			return fail("scope_id", nil, "creator-scoped scope_id must equal the caller's creator_id")
		}
	case ScopePlatform:
		rest, ok := strings.CutPrefix(scopeID, creatorID+":")
		if !ok || !Platforms[rest] {
			return fail("scope_id", nil, "platform-scoped scope_id must be {creator_id}:{platform} for the caller's creator_id")
		}
	case ScopeContent:
		// Opaque content_id, accepted unvalidated: an unowned one is
		// inert, never a privilege escalation (A12).
	}
	return nil
}
