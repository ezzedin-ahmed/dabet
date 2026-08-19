package oauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client performs the provider-side HTTP calls of the §5.5 flow. All
// errors are provider failures the API layer maps to 502 upstream_error.
type Client struct {
	HTTP *http.Client
}

// NewClient returns a Client with a bounded-timeout http.Client.
func NewClient() *Client {
	return &Client{HTTP: &http.Client{Timeout: 10 * time.Second}}
}

// Token is a provider token response.
type Token struct {
	AccessToken  string
	RefreshToken string
	// ExpiresIn is seconds until access-token expiry; 0 when the
	// provider did not say.
	ExpiresIn int64
	// Scopes are the granted scopes, when reported.
	Scopes []string
}

// tokenWire tolerates the provider dialects: scope may be a
// space-delimited string (Google, Discord) or a JSON array (Twitch).
type tokenWire struct {
	AccessToken  string          `json:"access_token"`
	RefreshToken string          `json:"refresh_token"`
	ExpiresIn    int64           `json:"expires_in"`
	Scope        json.RawMessage `json:"scope"`
	TokenType    string          `json:"token_type"`
}

func (w *tokenWire) token() *Token {
	t := &Token{AccessToken: w.AccessToken, RefreshToken: w.RefreshToken, ExpiresIn: w.ExpiresIn}
	if len(w.Scope) > 0 {
		var arr []string
		var s string
		if json.Unmarshal(w.Scope, &arr) == nil {
			t.Scopes = arr
		} else if json.Unmarshal(w.Scope, &s) == nil {
			t.Scopes = splitScopes(s)
		}
	}
	return t
}

// Exchange redeems an authorization code (with its PKCE verifier) at the
// provider token endpoint.
func (c *Client) Exchange(ctx context.Context, p *Provider, code, verifier string) (*Token, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", p.RedirectURI)
	form.Set("client_id", p.ClientID)
	form.Set("client_secret", p.ClientSecret)
	form.Set("code_verifier", verifier)
	return c.tokenCall(ctx, p, form)
}

func (c *Client) tokenCall(ctx context.Context, p *Provider, form url.Values) (*Token, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("%s token request: %w", p.Platform, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s token endpoint: %w", p.Platform, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("%s token endpoint: %w", p.Platform, err)
	}
	if resp.StatusCode/100 != 2 {
		// Status only — the body may echo credentials (P4).
		return nil, fmt.Errorf("%s token endpoint returned status %d", p.Platform, resp.StatusCode)
	}
	var w tokenWire
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, fmt.Errorf("%s token endpoint: undecodable response", p.Platform)
	}
	if w.AccessToken == "" {
		return nil, fmt.Errorf("%s token endpoint: no access token in response", p.Platform)
	}
	return w.token(), nil
}

// Userinfo fetches the connected account's provider user id and display
// name. The parser tolerates the shapes of the supported providers:
// Google userinfo ({id,name}), Twitch Helix ({data:[{id,display_name}]}),
// Discord ({id,username}), and any stub emitting one of those.
func (c *Client) Userinfo(ctx context.Context, p *Provider, accessToken string) (providerUserID, displayName string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.UserinfoURL, nil)
	if err != nil {
		return "", "", fmt.Errorf("%s userinfo request: %w", p.Platform, err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")
	if p.Platform == PlatformTwitch {
		// Helix requires the app's client id alongside the user token.
		req.Header.Set("Client-Id", p.ClientID)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("%s userinfo endpoint: %w", p.Platform, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", "", fmt.Errorf("%s userinfo endpoint: %w", p.Platform, err)
	}
	if resp.StatusCode/100 != 2 {
		return "", "", fmt.Errorf("%s userinfo endpoint returned status %d", p.Platform, resp.StatusCode)
	}
	id, name, ok := parseUserinfo(body)
	if !ok {
		return "", "", fmt.Errorf("%s userinfo endpoint: no user id in response", p.Platform)
	}
	return id, name, nil
}

// flexString decodes a JSON string or number as a string, since provider
// user ids appear as both across (and within) provider APIs.
type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		*f = flexString(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*f = flexString(n.String())
	return nil
}

type userinfoWire struct {
	ID          flexString `json:"id"`
	Sub         string     `json:"sub"`
	DisplayName string     `json:"display_name"`
	Name        string     `json:"name"`
	Username    string     `json:"username"`
	Login       string     `json:"login"`
	Data        []struct {
		ID          flexString `json:"id"`
		DisplayName string     `json:"display_name"`
		Login       string     `json:"login"`
	} `json:"data"`
}

func parseUserinfo(body []byte) (id, name string, ok bool) {
	var w userinfoWire
	if err := json.Unmarshal(body, &w); err != nil {
		return "", "", false
	}
	if len(w.Data) > 0 { // Twitch Helix envelope
		d := w.Data[0]
		id = string(d.ID)
		return id, firstNonEmpty(d.DisplayName, d.Login, id), id != ""
	}
	id = firstNonEmpty(string(w.ID), w.Sub)
	name = firstNonEmpty(w.DisplayName, w.Name, w.Username, w.Login, id)
	return id, name, id != ""
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// Revoke best-effort revokes a token at the provider. Callers ignore the
// error beyond logging its presence (§5.5: disconnect is best-effort).
func (c *Client) Revoke(ctx context.Context, p *Provider, token string) error {
	if p.RevokeURL == "" {
		return nil
	}
	form := url.Values{}
	form.Set("token", token)
	form.Set("client_id", p.ClientID)
	if p.ClientSecret != "" {
		form.Set("client_secret", p.ClientSecret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.RevokeURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("%s revoke request: %w", p.Platform, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("%s revoke endpoint: %w", p.Platform, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s revoke endpoint returned status %d", p.Platform, resp.StatusCode)
	}
	return nil
}
