// Package oauth implements the §5.5 provider side of connecting a
// platform: per-platform endpoint configuration, PKCE, the authorization
// URL, the code exchange, userinfo, and best-effort revocation.
//
// Every endpoint, credential, and scope list is env-configurable per
// platform (OAUTH_<PLATFORM>_AUTH_URL, _TOKEN_URL, _USERINFO_URL,
// _REVOKE_URL, _CLIENT_ID, _CLIENT_SECRET, _SCOPES, _REDIRECT_URI) with
// real-world defaults, so e2e can point everything at a stub provider.
//
// Per P4, nothing in this package logs tokens, client secrets, or PKCE
// verifiers — errors carry provider status codes, never response bodies
// echoing credentials.
package oauth

import (
	"sort"
	"strings"
)

// Well-known platform names. Mock is only served when OAUTH_MOCK_ENABLED
// is set (documented deviation, for e2e against a stub provider).
const (
	PlatformYouTube = "youtube"
	PlatformTwitch  = "twitch"
	PlatformDiscord = "discord"
	PlatformMock    = "mock"
)

// Provider is one platform's OAuth configuration.
type Provider struct {
	Platform     string
	AuthURL      string
	TokenURL     string
	UserinfoURL  string
	RevokeURL    string // empty: no revocation endpoint, disconnect skips it
	ClientID     string
	ClientSecret string
	// Scopes are requested at authorization and — per §5.5 — must all be
	// granted, or the callback fails with 422 naming the missing scope.
	Scopes []string
	// RedirectURI is fixed per platform and registered with the provider;
	// it is never taken from the request (§5.5).
	RedirectURI string
}

// Getenv is the configuration source, usually config.GetDefault.
type Getenv func(name, def string) string

// defaults per platform. Scopes are the §5.5 moderation scopes (A5 —
// verify against current provider docs; env-overridable for that reason).
var defaults = map[string]Provider{
	PlatformYouTube: {
		AuthURL:     "https://accounts.google.com/o/oauth2/v2/auth",
		TokenURL:    "https://oauth2.googleapis.com/token",
		UserinfoURL: "https://www.googleapis.com/oauth2/v2/userinfo",
		RevokeURL:   "https://oauth2.googleapis.com/revoke",
		Scopes:      []string{"https://www.googleapis.com/auth/youtube.force-ssl"},
	},
	PlatformTwitch: {
		AuthURL:     "https://id.twitch.tv/oauth2/authorize",
		TokenURL:    "https://id.twitch.tv/oauth2/token",
		UserinfoURL: "https://api.twitch.tv/helix/users",
		RevokeURL:   "https://id.twitch.tv/oauth2/revoke",
		Scopes:      []string{"moderator:manage:chat_messages", "user:read:chat", "moderator:read:chatters"},
	},
	PlatformDiscord: {
		AuthURL:     "https://discord.com/oauth2/authorize",
		TokenURL:    "https://discord.com/api/oauth2/token",
		UserinfoURL: "https://discord.com/api/users/@me",
		RevokeURL:   "https://discord.com/api/oauth2/token/revoke",
		Scopes:      []string{"identify", "bot"},
	},
	// The mock platform has no real provider; endpoints must be pointed
	// at a stub via env. Defaults target the local compose stub port.
	PlatformMock: {
		AuthURL:     "http://localhost:9099/oauth/authorize",
		TokenURL:    "http://localhost:9099/oauth/token",
		UserinfoURL: "http://localhost:9099/oauth/userinfo",
		RevokeURL:   "http://localhost:9099/oauth/revoke",
		Scopes:      []string{"mock:moderate"},
	},
}

// LoadProviders builds the provider set from env. mockEnabled gates the
// mock platform (OAUTH_MOCK_ENABLED). Client credentials default to
// empty — fine for local stubs, required for real providers.
func LoadProviders(getenv Getenv, mockEnabled bool) map[string]*Provider {
	out := make(map[string]*Provider, len(defaults))
	for platform, def := range defaults {
		if platform == PlatformMock && !mockEnabled {
			continue
		}
		prefix := "OAUTH_" + strings.ToUpper(platform) + "_"
		p := &Provider{
			Platform:     platform,
			AuthURL:      getenv(prefix+"AUTH_URL", def.AuthURL),
			TokenURL:     getenv(prefix+"TOKEN_URL", def.TokenURL),
			UserinfoURL:  getenv(prefix+"USERINFO_URL", def.UserinfoURL),
			RevokeURL:    getenv(prefix+"REVOKE_URL", def.RevokeURL),
			ClientID:     getenv(prefix+"CLIENT_ID", ""),
			ClientSecret: getenv(prefix+"CLIENT_SECRET", ""),
			Scopes:       splitScopes(getenv(prefix+"SCOPES", strings.Join(def.Scopes, " "))),
			RedirectURI: getenv(prefix+"REDIRECT_URI",
				strings.TrimSuffix(getenv("OAUTH_REDIRECT_BASE", "http://localhost:8080"), "/")+"/v1/connections/callback"),
		}
		out[platform] = p
	}
	return out
}

// splitScopes accepts space- or comma-separated scope lists.
func splitScopes(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool { return r == ' ' || r == ',' })
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

// MissingScopes returns the required scopes absent from granted, sorted.
func MissingScopes(required, granted []string) []string {
	have := make(map[string]bool, len(granted))
	for _, g := range granted {
		have[g] = true
	}
	var missing []string
	for _, r := range required {
		if !have[r] {
			missing = append(missing, r)
		}
	}
	sort.Strings(missing)
	return missing
}
