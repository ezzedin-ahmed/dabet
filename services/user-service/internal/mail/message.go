package mail

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	netmail "net/mail"
	"net/textproto"
	"strings"
	"time"
)

// message is one rendered email, ready to encode. Both bodies are always
// present: a multipart/alternative with a plain-text part first is what
// makes the mail readable in a text client and in a webmail alike, and
// text-only clients are still common in the ops mailboxes these
// notifications land in.
type message struct {
	From    string // RFC 5322, may carry a display name
	To      Recipient
	Subject string
	Text    string
	HTML    string
	Date    time.Time
}

// bytes encodes the message as RFC 5322 with CRLF line endings, ready
// for SMTP DATA. Both parts are quoted-printable so non-ASCII names and
// long lines survive intact.
func (m message) bytes() ([]byte, error) {
	if _, err := netmail.ParseAddress(m.From); err != nil {
		return nil, fmt.Errorf("mail: invalid From: %w", err)
	}
	if _, err := netmail.ParseAddress(m.To.Email); err != nil {
		return nil, fmt.Errorf("mail: invalid To: %w", err)
	}

	var body bytes.Buffer
	mp := multipart.NewWriter(&body)
	if err := writePart(mp, "text/plain; charset=utf-8", m.Text); err != nil {
		return nil, err
	}
	if err := writePart(mp, "text/html; charset=utf-8", m.HTML); err != nil {
		return nil, err
	}
	if err := mp.Close(); err != nil {
		return nil, err
	}

	id, err := messageID(m.From)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	headers := [][2]string{
		{"From", m.From},
		{"To", (&netmail.Address{Name: m.To.Name, Address: m.To.Email}).String()},
		{"Subject", mime.QEncoding.Encode("utf-8", m.Subject)},
		{"Date", m.Date.Format(time.RFC1123Z)},
		{"Message-ID", id},
		{"MIME-Version", "1.0"},
		// Transactional mail: tells well-behaved autoresponders not to
		// reply, and marks these as machine-generated.
		{"Auto-Submitted", "auto-generated"},
		{"Content-Type", "multipart/alternative; boundary=" + mp.Boundary()},
	}
	for _, h := range headers {
		fmt.Fprintf(&out, "%s: %s\r\n", h[0], sanitizeHeader(h[1]))
	}
	out.WriteString("\r\n")
	out.Write(body.Bytes())
	return out.Bytes(), nil
}

func writePart(mp *multipart.Writer, contentType, content string) error {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Type", contentType)
	h.Set("Content-Transfer-Encoding", "quoted-printable")
	w, err := mp.CreatePart(h)
	if err != nil {
		return err
	}
	qp := quotedprintable.NewWriter(w)
	if _, err := qp.Write([]byte(normalizeNewlines(content))); err != nil {
		return err
	}
	return qp.Close()
}

// normalizeNewlines makes the body CRLF-terminated as SMTP requires.
func normalizeNewlines(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", "\n"), "\n", "\r\n")
}

// sanitizeHeader strips CR and LF so no rendered value can inject a
// header or terminate the header block. Every value we emit is either
// generated or Q-encoded, but recipient display names come from user
// input, so this is the belt to that braces.
func sanitizeHeader(v string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(v)
}

// messageID builds a unique Message-ID in the sender's domain.
func messageID(from string) (string, error) {
	addr, err := addressOnly(from)
	if err != nil {
		return "", err
	}
	domain := "localhost"
	if i := strings.LastIndex(addr, "@"); i >= 0 && i < len(addr)-1 {
		domain = addr[i+1:]
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("mail: message id: %w", err)
	}
	return "<" + hex.EncodeToString(b[:]) + "@" + domain + ">", nil
}

// addressOnly strips any display name, giving the SMTP envelope address.
func addressOnly(s string) (string, error) {
	a, err := netmail.ParseAddress(s)
	if err != nil {
		return "", err
	}
	return a.Address, nil
}
