package opaque

import (
	"bytes"
	"strings"
	"testing"
)

func TestBase62RoundTrip(t *testing.T) {
	cases := [][]byte{
		{},
		{0},
		{0, 0, 0},
		{0, 0, 1, 2, 3},
		{1},
		{255},
		[]byte("hello world"),
		bytes.Repeat([]byte{0xFF}, 32),
	}
	for _, in := range cases {
		enc := encodeBase62(in)
		out, err := decodeBase62(enc)
		if err != nil {
			t.Fatalf("decode(%q): %v", enc, err)
		}
		if !bytes.Equal(in, out) {
			t.Errorf("round-trip %v -> %q -> %v", in, enc, out)
		}
	}
	if _, err := decodeBase62("ab!cd"); err == nil {
		t.Error("expected error for invalid base62 character")
	}
}

func TestMintRoundTripPlatform(t *testing.T) {
	for _, platform := range []string{"youtube", "twitch", "discord", "mock"} {
		content, err := MintContentID(platform, "chan-123")
		if err != nil {
			t.Fatalf("MintContentID(%s): %v", platform, err)
		}
		author, err := MintAuthorID(platform, "user-456")
		if err != nil {
			t.Fatalf("MintAuthorID(%s): %v", platform, err)
		}
		message, _, err := MintMessageID(platform, "msg-789")
		if err != nil {
			t.Fatalf("MintMessageID(%s): %v", platform, err)
		}
		for _, id := range []string{content, author, message} {
			if len(id) > MaxIDLen {
				t.Errorf("%s id %q is %d chars, exceeds %d", platform, id, len(id), MaxIDLen)
			}
			got, err := Platform(id)
			if err != nil {
				t.Fatalf("Platform(%q): %v", id, err)
			}
			if got != platform {
				t.Errorf("Platform(%q) = %q, want %q", id, got, platform)
			}
		}
	}
}

func TestMintDeterministicAndDistinct(t *testing.T) {
	a1, _ := MintContentID("youtube", "chan-A")
	a2, _ := MintContentID("youtube", "chan-A")
	if a1 != a2 {
		t.Errorf("minting is not deterministic: %q vs %q", a1, a2)
	}
	b, _ := MintContentID("youtube", "chan-B")
	if a1 == b {
		t.Error("distinct channels minted the same content_id")
	}
	crossPlatform, _ := MintContentID("twitch", "chan-A")
	if a1 == crossPlatform {
		t.Error("same native id on different platforms minted the same content_id")
	}
}

func TestMintMessageIDReversible(t *testing.T) {
	id, reversible, err := MintMessageID("youtube", "LCC.abc123")
	if err != nil {
		t.Fatal(err)
	}
	if !reversible {
		t.Fatal("short native id should mint reversibly")
	}
	native, ok := nativeFromMessageID(id)
	if !ok || native != "LCC.abc123" {
		t.Errorf("nativeFromMessageID(%q) = %q, %v", id, native, ok)
	}
}

func TestMintMessageIDHashedFallback(t *testing.T) {
	long := strings.Repeat("x", 200)
	id, reversible, err := MintMessageID("youtube", long)
	if err != nil {
		t.Fatal(err)
	}
	if reversible {
		t.Fatal("200-char native id cannot be reversible under MaxIDLen")
	}
	if len(id) > MaxIDLen {
		t.Errorf("hashed message id %q exceeds %d chars", id, MaxIDLen)
	}
	if p, err := Platform(id); err != nil || p != "youtube" {
		t.Errorf("Platform(%q) = %q, %v", id, p, err)
	}
	// The Minter remembers the native id for hashed fallbacks.
	m := NewMinter()
	minted, err := m.MessageID("youtube", long)
	if err != nil {
		t.Fatal(err)
	}
	if native, ok := m.NativeMessageID(minted); !ok || native != long {
		t.Error("Minter did not resolve hashed message id back to its native id")
	}
}

func TestMinterResolvesContent(t *testing.T) {
	m := NewMinter()
	id, err := m.ContentID("twitch", "44322889")
	if err != nil {
		t.Fatal(err)
	}
	if native, ok := m.NativeContentID(id); !ok || native != "44322889" {
		t.Errorf("NativeContentID(%q) = %q, %v", id, native, ok)
	}
	if _, ok := m.NativeContentID("ct_unknown"); ok {
		t.Error("unknown content id should not resolve")
	}
}

func TestUnknownPlatformAndBadIDs(t *testing.T) {
	if _, err := MintContentID("myspace", "x"); err == nil {
		t.Error("expected error for unknown platform")
	}
	for _, bad := range []string{"", "nonsense", "ct_", "ct_!!!", "9d4e-not-opaque"} {
		if _, err := Platform(bad); err == nil {
			t.Errorf("Platform(%q) should fail", bad)
		}
	}
}
