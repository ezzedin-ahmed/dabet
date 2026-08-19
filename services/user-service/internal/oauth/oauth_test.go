package oauth

import (
	"crypto/sha256"
	"encoding/base64"
	"reflect"
	"testing"
)

func TestLoadProvidersDefaultsAndOverrides(t *testing.T) {
	env := map[string]string{
		"OAUTH_TWITCH_TOKEN_URL": "http://stub:9099/oauth/token",
		"OAUTH_TWITCH_SCOPES":    "a:b, c:d",
		"OAUTH_TWITCH_CLIENT_ID": "cid",
	}
	getenv := func(name, def string) string {
		if v, ok := env[name]; ok {
			return v
		}
		return def
	}

	providers := LoadProviders(getenv, false)
	if _, ok := providers[PlatformMock]; ok {
		t.Error("mock platform present without OAUTH_MOCK_ENABLED")
	}
	for _, p := range []string{PlatformYouTube, PlatformTwitch, PlatformDiscord} {
		if providers[p] == nil {
			t.Fatalf("platform %s missing", p)
		}
	}

	tw := providers[PlatformTwitch]
	if tw.TokenURL != "http://stub:9099/oauth/token" {
		t.Errorf("twitch token URL override lost: %q", tw.TokenURL)
	}
	if !reflect.DeepEqual(tw.Scopes, []string{"a:b", "c:d"}) {
		t.Errorf("scope parsing = %v", tw.Scopes)
	}
	if tw.ClientID != "cid" || tw.ClientSecret != "" {
		t.Errorf("credentials = %q/%q", tw.ClientID, tw.ClientSecret)
	}
	if tw.AuthURL != "https://id.twitch.tv/oauth2/authorize" {
		t.Errorf("unoverridden default lost: %q", tw.AuthURL)
	}
	if tw.RedirectURI != "http://localhost:8080/v1/connections/callback" {
		t.Errorf("redirect URI default = %q", tw.RedirectURI)
	}

	if providers = LoadProviders(getenv, true); providers[PlatformMock] == nil {
		t.Error("mock platform absent with OAUTH_MOCK_ENABLED")
	}
}

func TestChallengeIsS256(t *testing.T) {
	verifier := "some-verifier-value"
	sum := sha256.Sum256([]byte(verifier))
	want := base64.RawURLEncoding.EncodeToString(sum[:])
	if got := Challenge(verifier); got != want {
		t.Errorf("Challenge = %q, want %q", got, want)
	}
}

func TestNewVerifierIsRandomURLSafe(t *testing.T) {
	a, err := NewVerifier()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewVerifier()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Error("verifiers repeat")
	}
	if raw, err := base64.RawURLEncoding.DecodeString(a); err != nil || len(raw) != 32 {
		t.Errorf("verifier %q not 32 bytes base64url", a)
	}
}

func TestMissingScopes(t *testing.T) {
	required := []string{"mod:manage", "chat:read"}
	if m := MissingScopes(required, []string{"chat:read", "mod:manage", "extra"}); len(m) != 0 {
		t.Errorf("missing = %v; want none", m)
	}
	if m := MissingScopes(required, []string{"chat:read"}); !reflect.DeepEqual(m, []string{"mod:manage"}) {
		t.Errorf("missing = %v; want [mod:manage]", m)
	}
}

func TestTokenWireScopeDialects(t *testing.T) {
	arr := &tokenWire{AccessToken: "t", Scope: []byte(`["a","b"]`)}
	if got := arr.token().Scopes; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("array scope = %v", got)
	}
	str := &tokenWire{AccessToken: "t", Scope: []byte(`"a b"`)}
	if got := str.token().Scopes; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("string scope = %v", got)
	}
}

func TestParseUserinfoShapes(t *testing.T) {
	tests := []struct {
		name, body, wantID, wantName string
	}{
		{"google", `{"id":"g-1","name":"Google User"}`, "g-1", "Google User"},
		{"twitch helix", `{"data":[{"id":"44322889","display_name":"somechannel","login":"somechannel"}]}`, "44322889", "somechannel"},
		{"discord", `{"id":"80351110224678912","username":"nelly"}`, "80351110224678912", "nelly"},
		{"numeric id", `{"id":123,"name":"n"}`, "123", "n"},
		{"oidc sub", `{"sub":"s-9"}`, "s-9", "s-9"},
	}
	for _, tc := range tests {
		id, name, ok := parseUserinfo([]byte(tc.body))
		if !ok || id != tc.wantID || name != tc.wantName {
			t.Errorf("%s: parseUserinfo = (%q, %q, %v), want (%q, %q)", tc.name, id, name, ok, tc.wantID, tc.wantName)
		}
	}
	if _, _, ok := parseUserinfo([]byte(`{"name":"anonymous"}`)); ok {
		t.Error("userinfo without id parsed as ok")
	}
}
