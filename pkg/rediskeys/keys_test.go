package rediskeys

import "testing"

// Exact formats per docs §4.3, hash tags from day one: keys for one
// (content, author) pair carry a literal {content:author} hash tag so they
// land on one Redis Cluster slot.
func TestKeyFormats(t *testing.T) {
	cases := []struct{ got, want string }{
		{Seen("ytc_01J8XQ7K2M4N"), "seen:ytc_01J8XQ7K2M4N"},
		{Dup("ct_9f2a", "sd_3b71"), "dup:{ct_9f2a:sd_3b71}"},
		{Emb("ct_9f2a", "sd_3b71"), "emb:{ct_9f2a:sd_3b71}"},
		{Rate("ct_9f2a", "sd_3b71"), "rate:{ct_9f2a:sd_3b71}"},
		{Samp("ct_9f2a"), "samp:{ct_9f2a}"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
}

// All pair-scoped keys must share one hash tag so cluster mode co-locates
// them; extract the {...} segment and compare.
func TestPairKeysShareHashTag(t *testing.T) {
	tag := func(key string) string {
		start := -1
		for i, r := range key {
			if r == '{' {
				start = i + 1
			}
			if r == '}' && start >= 0 {
				return key[start:i]
			}
		}
		t.Fatalf("key %q has no hash tag", key)
		return ""
	}
	a := tag(Dup("ct_1", "sd_2"))
	if b := tag(Emb("ct_1", "sd_2")); b != a {
		t.Errorf("emb tag %q != dup tag %q", b, a)
	}
	if b := tag(Rate("ct_1", "sd_2")); b != a {
		t.Errorf("rate tag %q != dup tag %q", b, a)
	}
}
