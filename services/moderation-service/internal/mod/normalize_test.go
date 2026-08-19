package mod

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"lowercases", "HeLLo World", "hello world"},
		{"collapses whitespace", "buy   my\t\tthing\n now", "buy my thing now"},
		{"trims ends", "  spaced out  ", "spaced out"},
		{"strips zero-width", "sp\u200bam \u200cand \u200dmore\u2060", "spam and more"},
		{"strips bom and soft hyphen", "\ufeffdo\u00adnate", "donate"},
		{"empty", "", ""},
		{"only whitespace", " \t\n ", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Normalize(c.in); got != c.want {
				t.Fatalf("Normalize(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// The duplicate detector must not be defeated by case, whitespace, or
// zero-width tricks: all variants hash identically (§7.4).
func TestHashTextDefeatsTrivialVariation(t *testing.T) {
	base := HashText(Normalize("buy my merch now"))
	variants := []string{
		"BUY MY MERCH NOW",
		"buy  my   merch \t now",
		"buy my\u200b merch now",
		"  buy my merch now  ",
	}
	for _, v := range variants {
		if HashText(Normalize(v)) != base {
			t.Errorf("variant %q does not hash like base", v)
		}
	}
	if HashText(Normalize("buy my merch later")) == base {
		t.Error("different text must hash differently")
	}
}
