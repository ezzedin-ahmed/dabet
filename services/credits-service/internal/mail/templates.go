package mail

import (
	"embed"
	"fmt"
	htmltemplate "html/template"
	"strings"
	texttemplate "text/template"
)

// Template names. These are also the values of the emails_sent_total
// {template} label, so the set must stay small and fixed (§4.5). They
// match the constants in internal/notify, which decides which of the two
// A8 thresholds was crossed.
const (
	// TemplateCreditsLow is the 20 %-of-last-top-up warning.
	TemplateCreditsLow = "credits_low"
	// TemplateCreditsExhausted is the zero-balance notice: moderation
	// keeps running but is passing messages through unmoderated (§5.8).
	TemplateCreditsExhausted = "credits_exhausted"
)

//go:embed templates/*.tmpl
var templateFS embed.FS

// subjects are fixed per template: they carry no creator data, so
// nothing user-supplied can reach a header.
var subjects = map[string]string{
	TemplateCreditsLow:       "Your Dabet credits are running low",
	TemplateCreditsExhausted: "Your Dabet credits have run out",
}

// templateSet is the plain-text and HTML rendering of one message.
type templateSet struct {
	subject string
	text    *texttemplate.Template
	html    *htmltemplate.Template
}

// loadTemplates parses every embedded template once. A missing or broken
// file is a startup error, not a per-message surprise.
func loadTemplates() (map[string]templateSet, error) {
	out := make(map[string]templateSet, len(subjects))
	for name, subject := range subjects {
		text, err := texttemplate.ParseFS(templateFS, "templates/"+name+".txt.tmpl")
		if err != nil {
			return nil, fmt.Errorf("mail: parse %s text template: %w", name, err)
		}
		html, err := htmltemplate.ParseFS(templateFS, "templates/"+name+".html.tmpl")
		if err != nil {
			return nil, fmt.Errorf("mail: parse %s html template: %w", name, err)
		}
		out[name] = templateSet{subject: subject, text: text, html: html}
	}
	return out, nil
}

// render produces the subject and both bodies for one template. The HTML
// side is html/template, so creator-supplied values (the full name) are
// contextually escaped rather than interpolated raw.
func (m *Mailer) render(name string, data any) (subject, text, html string, err error) {
	set, ok := m.tmpl[name]
	if !ok {
		return "", "", "", fmt.Errorf("mail: unknown template %q", name)
	}
	var textBuf, htmlBuf strings.Builder
	if err := set.text.Execute(&textBuf, data); err != nil {
		return "", "", "", fmt.Errorf("mail: render %s text: %w", name, err)
	}
	if err := set.html.Execute(&htmlBuf, data); err != nil {
		return "", "", "", fmt.Errorf("mail: render %s html: %w", name, err)
	}
	return set.subject, textBuf.String(), htmlBuf.String(), nil
}
