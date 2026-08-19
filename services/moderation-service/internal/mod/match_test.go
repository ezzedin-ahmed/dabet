package mod

import "testing"

func TestMatcherWholeTokenSemantics(t *testing.T) {
	m := NewMatcher([]string{"ass"})
	// The Scunthorpe case: substring must NOT match.
	for _, txt := range []string{"what a class act", "classic", "passing by", "assassin"} {
		if m.Match(Normalize(txt)) {
			t.Errorf("substring text %q must not match", txt)
		}
	}
	for _, txt := range []string{"you ass", "ass", "ASS!", "an ass, truly", "kiss my ass"} {
		if !m.Match(Normalize(txt)) {
			t.Errorf("whole-token text %q must match", txt)
		}
	}
}

func TestMatcherPhraseEntries(t *testing.T) {
	m := NewMatcher([]string{"free money"})
	if !m.Match(Normalize("get FREE   money now")) {
		t.Error("normalised phrase must match despite case and extra spaces")
	}
	if !m.Match(Normalize("free money")) {
		t.Error("exact phrase must match")
	}
	if m.Match(Normalize("freemoney")) {
		t.Error("joined words must not match a phrase")
	}
	if m.Match(Normalize("carefree moneybags")) {
		t.Error("phrase inside larger tokens must not match")
	}
}

func TestMatcherMultiplePatternsAndFailLinks(t *testing.T) {
	m := NewMatcher([]string{"he", "she", "hers"})
	if m.Match(Normalize("ushers")) {
		t.Error("embedded overlapping patterns must not match")
	}
	if !m.Match(Normalize("it was she")) {
		t.Error("'she' should match as whole token")
	}
	if !m.Match(Normalize("hers alone")) {
		t.Error("'hers' should match as whole token")
	}
}

func TestMatcherUnicodeAndEdges(t *testing.T) {
	m := NewMatcher([]string{"scheiße"})
	if !m.Match(Normalize("SCHEISSE?? scheiße!")) {
		t.Error("unicode token should match at word edge")
	}
	empty := NewMatcher(nil)
	if empty.Match("anything at all") {
		t.Error("empty matcher must match nothing")
	}
	blank := NewMatcher([]string{"", "   "})
	if blank.Match("anything") {
		t.Error("blank patterns are dropped")
	}
}

func TestMatcherNormalizesPatterns(t *testing.T) {
	m := NewMatcher([]string{"  Bad   Word "})
	if !m.Match(Normalize("such a bad word here")) {
		t.Error("pattern should be normalised at compile time")
	}
}
