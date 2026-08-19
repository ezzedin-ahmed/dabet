package mail

import (
	"context"
	"net/url"
	"strings"
)

// verificationData is the data of TemplateVerification. It carries the
// verification link — and therefore the single-use token — and nothing
// else about the creator beyond a greeting name.
type verificationData struct {
	Name         string
	VerifyURL    string
	ExpiresHours int
}

// connectionExpiredData is the data of TemplateConnectionExpired. No
// tokens: the mail says which channel stopped and where to reconnect it.
type connectionExpiredData struct {
	Name           string
	Platform       string
	DisplayName    string
	ConnectionsURL string
}

// SendVerification queues the §5.4 verification mail. It returns
// immediately: the SMTP conversation happens on a worker, so a dead mail
// server cannot fail — or even slow — a registration (§4.7).
//
// With the mailer disabled it reproduces v1's behaviour exactly: the raw
// token goes to the debug log and nowhere else. That is the documented,
// narrow exception to P4's no-token-logging rule and is invisible at the
// default info level.
func (m *Mailer) SendVerification(_ context.Context, email, fullname, token string) error {
	if !m.Enabled() {
		m.log.Debug("email verification token issued (dev-mode delivery channel)",
			"verification_token", token)
		m.logOnly(TemplateVerification)
		return nil
	}
	return m.enqueue(job{
		template: TemplateVerification,
		resolve:  static(Recipient{Email: email, Name: fullname}),
		data: verificationData{
			Name:         greeting(fullname),
			VerifyURL:    verifyLink(m.cfg.VerifyURL, token),
			ExpiresHours: int(verificationTTLHours),
		},
	})
}

// SendConnectionExpired queues the §5.5/A6 mail: the platform revoked
// our access, the adapter moved the connection to 'expired', and the
// creator has no in-app notification system in v1 to find that out from.
func (m *Mailer) SendConnectionExpired(_ context.Context, email, fullname, platform, displayName string) error {
	if !m.Enabled() {
		m.log.Info("connection expired notification not sent: mailer disabled",
			"template", TemplateConnectionExpired, "platform", platform)
		m.logOnly(TemplateConnectionExpired)
		return nil
	}
	return m.enqueue(job{
		template: TemplateConnectionExpired,
		resolve:  static(Recipient{Email: email, Name: fullname}),
		data: connectionExpiredData{
			Name:           greeting(fullname),
			Platform:       platform,
			DisplayName:    displayName,
			ConnectionsURL: m.cfg.AppConnectionsURL,
		},
	})
}

// verificationTTLHours mirrors api.VerificationTokenTTL. It lives here as
// a number so the mail package does not import the api package.
const verificationTTLHours = 24

// verifyLink appends the token to the configured base as a query
// parameter, whether or not the base already has a query string.
func verifyLink(base, token string) string {
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return base + sep + "token=" + url.QueryEscape(token)
}

// greeting keeps the salutation sensible when a name is missing.
func greeting(name string) string {
	if strings.TrimSpace(name) == "" {
		return "there"
	}
	return name
}
