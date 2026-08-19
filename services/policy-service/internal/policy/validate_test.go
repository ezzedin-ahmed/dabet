package policy

import (
	"reflect"
	"strings"
	"testing"
)

func intp(v int) *int { return &v }

func validDoc() Document {
	return Document{
		RateLimitMessages: intp(5),
		RateLimitSeconds:  intp(10),
		Spam:              SpamIdentical,
		RestrictedWords:   []string{"foo"},
		RestrictedContent: []RestrictedContentEntry{{
			Title:       "Ticket scalping",
			Description: "Offers to resell event tickets.",
			Examples:    []string{"selling 2 tickets for tonight DM me"},
		}},
		RestrictedContentAction: RCActionReview,
	}
}

func manyWords(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "w" + strings.Repeat("x", 3) + string(rune('a'+i%26)) + strings.Repeat("y", i%5) + itoa(i)
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func manyEntries(n int) []RestrictedContentEntry {
	out := make([]RestrictedContentEntry, n)
	for i := range out {
		out[i] = RestrictedContentEntry{Title: "t" + itoa(i), Description: "d"}
	}
	return out
}

func TestValidateAndNormalize(t *testing.T) {
	cases := []struct {
		name      string
		mutate    func(*Document)
		wantField string
		wantLimit any // nil means "don't check" when wantField is ""
	}{
		{"valid full document", func(d *Document) {}, "", nil},
		{"rate limit at minimums", func(d *Document) { d.RateLimitMessages = intp(1); d.RateLimitSeconds = intp(1) }, "", nil},
		{"rate limit at maximums", func(d *Document) { d.RateLimitMessages = intp(1000); d.RateLimitSeconds = intp(3600) }, "", nil},
		{"rate limit absent entirely", func(d *Document) { d.RateLimitMessages = nil; d.RateLimitSeconds = nil }, "", nil},
		{"messages without seconds", func(d *Document) { d.RateLimitSeconds = nil }, "rate_limit_seconds", nil},
		{"seconds without messages", func(d *Document) { d.RateLimitMessages = nil }, "rate_limit_messages", nil},
		{"messages below minimum", func(d *Document) { d.RateLimitMessages = intp(0) }, "rate_limit_messages", 1},
		{"messages above maximum", func(d *Document) { d.RateLimitMessages = intp(1001) }, "rate_limit_messages", 1000},
		{"seconds below minimum", func(d *Document) { d.RateLimitSeconds = intp(0) }, "rate_limit_seconds", 1},
		{"seconds above maximum", func(d *Document) { d.RateLimitSeconds = intp(3601) }, "rate_limit_seconds", 3600},
		{"invalid spam mode", func(d *Document) { d.Spam = "aggressive" }, "spam", nil},
		{"invalid action", func(d *Document) { d.RestrictedContentAction = "escalate" }, "restricted_content_action", nil},
		{"500 words allowed", func(d *Document) { d.RestrictedWords = manyWords(500) }, "", nil},
		{"501 words rejected", func(d *Document) { d.RestrictedWords = manyWords(501) }, "restricted_words", 500},
		{"empty word rejected", func(d *Document) { d.RestrictedWords = []string{"ok", ""} }, "restricted_words[1]", 1},
		{"64-char word allowed", func(d *Document) { d.RestrictedWords = []string{strings.Repeat("a", 64)} }, "", nil},
		{"65-char word rejected", func(d *Document) { d.RestrictedWords = []string{strings.Repeat("a", 65)} }, "restricted_words[0]", 64},
		{"20 content entries allowed", func(d *Document) { d.RestrictedContent = manyEntries(20) }, "", nil},
		{"21 content entries rejected", func(d *Document) { d.RestrictedContent = manyEntries(21) }, "restricted_content", 20},
		{"empty title rejected", func(d *Document) { d.RestrictedContent[0].Title = "" }, "restricted_content[0].title", 1},
		{"100-char title allowed", func(d *Document) { d.RestrictedContent[0].Title = strings.Repeat("t", 100) }, "", nil},
		{"101-char title rejected", func(d *Document) { d.RestrictedContent[0].Title = strings.Repeat("t", 101) }, "restricted_content[0].title", 100},
		{"empty description rejected", func(d *Document) { d.RestrictedContent[0].Description = "" }, "restricted_content[0].description", 1},
		{"500-char description allowed", func(d *Document) { d.RestrictedContent[0].Description = strings.Repeat("d", 500) }, "", nil},
		{"501-char description rejected", func(d *Document) { d.RestrictedContent[0].Description = strings.Repeat("d", 501) }, "restricted_content[0].description", 500},
		{"10 examples allowed", func(d *Document) { d.RestrictedContent[0].Examples = make([]string, 10) }, "", nil},
		{"11 examples rejected", func(d *Document) { d.RestrictedContent[0].Examples = make([]string, 11) }, "restricted_content[0].examples", 10},
		{"200-char example allowed", func(d *Document) { d.RestrictedContent[0].Examples = []string{strings.Repeat("e", 200)} }, "", nil},
		{"201-char example rejected", func(d *Document) { d.RestrictedContent[0].Examples = []string{strings.Repeat("e", 201)} }, "restricted_content[0].examples[0]", 200},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := validDoc()
			tc.mutate(&doc)
			err := doc.ValidateAndNormalize()
			if tc.wantField == "" {
				if err != nil {
					t.Fatalf("unexpected validation error: %v (field=%s)", err, err.Field)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected validation error on field %s, got none", tc.wantField)
			}
			if err.Field != tc.wantField {
				t.Errorf("field = %q, want %q", err.Field, tc.wantField)
			}
			if tc.wantLimit != nil && err.Limit != tc.wantLimit {
				t.Errorf("limit = %v, want %v", err.Limit, tc.wantLimit)
			}
			d := err.Details()
			if d["field"] != tc.wantField {
				t.Errorf("details field = %v, want %q", d["field"], tc.wantField)
			}
		})
	}
}

func TestNormalizationLowercasesAndDeduplicates(t *testing.T) {
	doc := Document{RestrictedWords: []string{"Foo", "foo", "BAR", "bar", "baz", "FOO"}}
	if err := doc.ValidateAndNormalize(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"foo", "bar", "baz"}
	if !reflect.DeepEqual(doc.RestrictedWords, want) {
		t.Errorf("restricted_words = %v, want %v", doc.RestrictedWords, want)
	}
}

func TestNormalizationDefaults(t *testing.T) {
	doc := Document{}
	if err := doc.ValidateAndNormalize(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.Spam != SpamNone {
		t.Errorf("spam default = %q, want %q", doc.Spam, SpamNone)
	}
	if doc.RestrictedContentAction != RCActionAuto {
		t.Errorf("action default = %q, want %q", doc.RestrictedContentAction, RCActionAuto)
	}
	if doc.RestrictedWords == nil || doc.RestrictedContent == nil {
		t.Error("nil slices should normalise to empty slices")
	}
}

func TestValidateScope(t *testing.T) {
	const me = "9d4ecafe-0000-0000-0000-000000000001"
	cases := []struct {
		name    string
		scope   Scope
		scopeID string
		ok      bool
	}{
		{"creator scope, own id", ScopeCreator, me, true},
		{"creator scope, someone else", ScopeCreator, "other-creator", false},
		{"platform scope, own id", ScopePlatform, me + ":twitch", true},
		{"platform scope, youtube", ScopePlatform, me + ":youtube", true},
		{"platform scope, unknown platform", ScopePlatform, me + ":myspace", false},
		{"platform scope, someone else's id", ScopePlatform, "other:twitch", false},
		{"platform scope, bare creator id", ScopePlatform, me, false},
		{"content scope, opaque id accepted", ScopeContent, "ct_9f2a", true},
		{"invalid scope", Scope("global"), me, false},
		{"empty scope_id", ScopeCreator, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateScope(tc.scope, tc.scopeID, me)
			if tc.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.ok && err == nil {
				t.Error("expected a validation error, got none")
			}
		})
	}
}
