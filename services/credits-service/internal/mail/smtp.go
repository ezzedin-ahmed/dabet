package mail

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"time"
)

// smtpSend delivers one message: dial, optionally upgrade to TLS,
// optionally authenticate, then MAIL/RCPT/DATA/QUIT. A fresh connection
// per message keeps the worker stateless — these are a handful of
// messages a minute, not a mail relay.
func (m *Mailer) smtpSend(ctx context.Context, from, to string, msg []byte) error {
	host, _, err := net.SplitHostPort(m.cfg.Addr)
	if err != nil {
		return fmt.Errorf("mail: %s: %w", EnvSMTPAddr, err)
	}

	conn, err := m.dial(ctx, host)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck // best effort; Quit is the clean path

	// One deadline covers the whole conversation: net/smtp has no
	// context support, so the connection deadline is the timeout.
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(m.cfg.Timeout))
	}

	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return fmt.Errorf("mail: smtp handshake: %w", err)
	}
	defer c.Close() //nolint:errcheck

	if err := c.Hello(m.cfg.Helo); err != nil {
		return fmt.Errorf("mail: EHLO: %w", err)
	}
	if m.cfg.TLS == TLSStartTLS {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return fmt.Errorf("mail: server does not offer STARTTLS (set %s=%s to opt out)", EnvTLS, TLSNone)
		}
		if err := c.StartTLS(m.tlsConfig(host)); err != nil {
			return fmt.Errorf("mail: STARTTLS: %w", err)
		}
	}
	if m.cfg.Username != "" {
		// PLAIN only, and net/smtp itself refuses to send it over an
		// unprotected connection — the second half of the "PLAIN over
		// TLS only" rule that Config.validate starts.
		if err := c.Auth(smtp.PlainAuth("", m.cfg.Username, m.cfg.Password, host)); err != nil {
			return fmt.Errorf("mail: AUTH: %w", err)
		}
	}
	if err := c.Mail(from); err != nil {
		return fmt.Errorf("mail: MAIL FROM: %w", err)
	}
	if err := c.Rcpt(to); err != nil {
		return fmt.Errorf("mail: RCPT TO: %w", err)
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("mail: DATA: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("mail: write body: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("mail: end of data: %w", err)
	}
	if err := c.Quit(); err != nil {
		return fmt.Errorf("mail: QUIT: %w", err)
	}
	return nil
}

func (m *Mailer) dial(ctx context.Context, host string) (net.Conn, error) {
	d := &net.Dialer{Timeout: m.cfg.Timeout}
	if m.cfg.TLS == TLSImplicit {
		td := &tls.Dialer{NetDialer: d, Config: m.tlsConfig(host)}
		conn, err := td.DialContext(ctx, "tcp", m.cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("mail: dial %s (TLS): %w", m.cfg.Addr, err)
		}
		return conn, nil
	}
	conn, err := d.DialContext(ctx, "tcp", m.cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("mail: dial %s: %w", m.cfg.Addr, err)
	}
	return conn, nil
}

func (m *Mailer) tlsConfig(host string) *tls.Config {
	if m.cfg.TLSConfig != nil {
		c := m.cfg.TLSConfig.Clone()
		if c.ServerName == "" {
			c.ServerName = host
		}
		return c
	}
	return &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
}
