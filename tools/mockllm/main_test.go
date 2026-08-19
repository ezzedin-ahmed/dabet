package main

import "testing"

func TestClassifyNumberedMessages(t *testing.T) {
	prompt := `System: You are a chat moderation classifier.

Rules:
1. Spam — Repetitive junk.

Messages:
1. hello there
2. buy now FLAGME cheap
3. what a great stream
`
	got := classify(prompt)
	if len(got) != 3 {
		t.Fatalf("got %d results, want 3", len(got))
	}
	want := []int{0, 1, 0}
	for i, v := range got {
		if v.I != i+1 {
			t.Errorf("result %d has i=%d", i, v.I)
		}
		if v.Violates != want[i] {
			t.Errorf("message %d: violates=%d, want %d", i+1, v.Violates, want[i])
		}
	}
}

func TestClassifyFallbackSingleMessage(t *testing.T) {
	got := classify("no numbered list here, but FLAGME anyway")
	if len(got) != 1 || got[0].I != 1 || got[0].Violates != 1 {
		t.Errorf("fallback result = %+v", got)
	}
	got = classify("clean text")
	if len(got) != 1 || got[0].Violates != 0 {
		t.Errorf("fallback clean result = %+v", got)
	}
}
