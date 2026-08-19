package contracts

import (
	"bytes"
	"testing"
)

func TestMessagesKeyDeterministic(t *testing.T) {
	a := MessagesKey("sd_3b71", "ct_9f2a")
	b := MessagesKey("sd_3b71", "ct_9f2a")
	if !bytes.Equal(a, b) {
		t.Error("MessagesKey is not deterministic")
	}
	if len(a) == 0 {
		t.Error("empty key")
	}
}

func TestMessagesKeyDistinct(t *testing.T) {
	base := MessagesKey("sd_3b71", "ct_9f2a")
	if bytes.Equal(base, MessagesKey("sd_3b72", "ct_9f2a")) {
		t.Error("different author, same key")
	}
	if bytes.Equal(base, MessagesKey("sd_3b71", "ct_9f2b")) {
		t.Error("different content, same key")
	}
	// Argument order matters and concatenation is unambiguous.
	if bytes.Equal(MessagesKey("a", "bc"), MessagesKey("ab", "c")) {
		t.Error("ambiguous concatenation")
	}
	if bytes.Equal(MessagesKey("x", "y"), MessagesKey("y", "x")) {
		t.Error("argument order ignored")
	}
}

func TestIdentityKeys(t *testing.T) {
	if string(FlaggedKey("9d4e")) != "9d4e" {
		t.Error("FlaggedKey must key by creator_id")
	}
	if string(DeletionsKey("ct_9f2a")) != "ct_9f2a" {
		t.Error("DeletionsKey must key by content_id")
	}
	if string(UsageKey("9d4e")) != "9d4e" {
		t.Error("UsageKey must key by creator_id")
	}
}
