package mail

import (
	netmail "net/mail"
	"strings"
	"testing"
	"time"
)

// renderer builds a disabled mailer purely to reach render().
func renderer(t *testing.T) *Mailer {
	t.Helper()
	m, err := New(Config{}, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func TestEveryTemplateRenders(t *testing.T) {
	m := renderer(t)
	cases := map[string]any{
		TemplateVerification: verificationData{
			Name: "Ada", VerifyURL: "https://app.dabet.test/verify?token=abc", ExpiresHours: 24,
		},
		TemplateConnectionExpired: connectionExpiredData{
			Name: "Ada", Platform: "youtube", DisplayName: "somechannel",
			ConnectionsURL: "https://app.dabet.test/connections",
		},
	}
	if len(cases) != len(subjects) {
		t.Fatalf("templates = %d, cases = %d: every template needs a rendering test", len(subjects), len(cases))
	}
	for name, data := range cases {
		subject, text, html, err := m.render(name, data)
		if err != nil {
			t.Fatalf("render %s: %v", name, err)
		}
		if subject == "" || !strings.Contains(subject, "Dabet") {
			t.Errorf("%s: subject = %q", name, subject)
		}
		if !strings.Contains(text, "Ada") || !strings.Contains(html, "Ada") {
			t.Errorf("%s: both bodies must address the creator", name)
		}
		if strings.Contains(text, "<div") {
			t.Errorf("%s: the plain-text part carries markup:\n%s", name, text)
		}
		if !strings.Contains(html, "<") {
			t.Errorf("%s: the html part is not HTML:\n%s", name, html)
		}
		if strings.Contains(text, "<no value>") || strings.Contains(html, "<no value>") {
			t.Errorf("%s: unfilled template field", name)
		}
	}
}

// A display name comes from a platform and is therefore attacker-
// controlled. html/template must escape it; the text part must not
// mangle it.
func TestHTMLTemplatesEscapeCreatorData(t *testing.T) {
	m := renderer(t)
	const evil = `<script>alert("x")</script>`
	_, text, html, err := m.render(TemplateConnectionExpired, connectionExpiredData{
		Name: evil, Platform: "twitch", DisplayName: evil,
		ConnectionsURL: "https://app.dabet.test/connections",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if strings.Contains(html, "<script>") {
		t.Errorf("html part is not escaped:\n%s", html)
	}
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Errorf("html part lost the escaped value:\n%s", html)
	}
	if !strings.Contains(text, evil) {
		t.Errorf("text part should carry the literal value:\n%s", text)
	}
}

// A javascript: URL must not survive into an href.
func TestHTMLTemplatesRejectDangerousURLs(t *testing.T) {
	m := renderer(t)
	_, _, html, err := m.render(TemplateVerification, verificationData{
		Name: "Ada", VerifyURL: "javascript:alert(1)", ExpiresHours: 24,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if strings.Contains(html, "javascript:alert") {
		t.Errorf("dangerous URL survived into the html part:\n%s", html)
	}
}

func TestVerifyLinkAppendsToken(t *testing.T) {
	if got := verifyLink("https://app.test/verify", "a b+c"); got != "https://app.test/verify?token=a+b%2Bc" {
		t.Errorf("verifyLink = %q", got)
	}
	if got := verifyLink("https://app.test/v?x=1", "t"); got != "https://app.test/v?x=1&token=t" {
		t.Errorf("verifyLink with existing query = %q", got)
	}
}

// Header injection: a display name carrying CRLF must not be able to add
// headers to the message.
func TestHeaderInjectionIsNeutralised(t *testing.T) {
	raw, err := message{
		From:    "Dabet <no-reply@dabet.test>",
		To:      Recipient{Email: "creator@example.test", Name: "Ada\r\nBcc: attacker@evil.test"},
		Subject: "Subject\r\nX-Injected: yes",
		Text:    "body",
		HTML:    "<p>body</p>",
		Date:    time.Now(),
	}.bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	head, _, _ := strings.Cut(string(raw), "\r\n\r\n")
	for _, line := range strings.Split(head, "\r\n") {
		for _, bad := range []string{"Bcc:", "X-Injected:"} {
			if strings.HasPrefix(line, bad) {
				t.Errorf("injected header %q present in:\n%s", bad, head)
			}
		}
	}
	msg, err := netmail.ReadMessage(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("the encoded message must still parse: %v", err)
	}
	if got := msg.Header.Get("Bcc"); got != "" {
		t.Errorf("Bcc header injected: %q", got)
	}
}

func TestMessageRejectsBadAddresses(t *testing.T) {
	base := message{From: "no-reply@dabet.test", To: Recipient{Email: "creator@example.test"}, Text: "t", HTML: "h"}
	bad := base
	bad.To = Recipient{Email: "not-an-address"}
	if _, err := bad.bytes(); err == nil {
		t.Error("want an error for an invalid recipient")
	}
	bad = base
	bad.From = "also bad"
	if _, err := bad.bytes(); err == nil {
		t.Error("want an error for an invalid sender")
	}
}
