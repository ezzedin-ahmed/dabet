package mail

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSMTP is an in-process SMTP server speaking enough of RFC 5321 to
// exercise the real net/smtp client: greeting, EHLO with extensions,
// STARTTLS, AUTH PLAIN, MAIL/RCPT/DATA/QUIT and dot-stuffed message
// bodies. Tests assert on what actually crossed the socket rather than
// on a stubbed interface, which is the only way to catch a malformed
// envelope or a broken MIME encoding.
type fakeSMTP struct {
	ln       net.Listener
	tlsCfg   *tls.Config // non-nil: STARTTLS is offered
	implicit bool        // TLS from the first byte (SMTPS)

	mu         sync.Mutex
	deliveries []delivery
	failData   int // reject this many DATA transactions with 451 first
}

// delivery is one accepted message.
type delivery struct {
	from      string
	to        []string
	data      string
	authPlain string
	overTLS   bool
}

type fakeOption func(*fakeSMTP)

// withTLS makes the server offer STARTTLS (or, with implicit, require
// TLS from the first byte) using a throwaway self-signed certificate.
func withTLS(cfg *tls.Config, implicit bool) fakeOption {
	return func(f *fakeSMTP) {
		f.tlsCfg = cfg
		f.implicit = implicit
	}
}

// failFirst makes the server reject the first n DATA transactions with a
// 4xx, so the client's retry path is exercised.
func failFirst(n int) fakeOption {
	return func(f *fakeSMTP) { f.failData = n }
}

func newFakeSMTP(t *testing.T, opts ...fakeOption) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeSMTP{ln: ln}
	for _, o := range opts {
		o(f)
	}
	go f.serve()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *fakeSMTP) addr() string { return f.ln.Addr().String() }

func (f *fakeSMTP) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeSMTP) handle(conn net.Conn) {
	defer conn.Close()

	overTLS := false
	if f.implicit {
		tc := tls.Server(conn, f.tlsCfg)
		if err := tc.Handshake(); err != nil {
			return
		}
		conn = tc
		overTLS = true
	}

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	write := func(lines ...string) {
		for _, l := range lines {
			w.WriteString(l + "\r\n") //nolint:errcheck
		}
		w.Flush() //nolint:errcheck
	}

	write("220 fake.test ESMTP")
	var d delivery
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.TrimRight(line, "\r\n")
		upper := strings.ToUpper(cmd)
		switch {
		case strings.HasPrefix(upper, "EHLO"):
			caps := []string{"250-fake.test"}
			if f.tlsCfg != nil && !overTLS {
				caps = append(caps, "250-STARTTLS")
			}
			if overTLS {
				caps = append(caps, "250-AUTH PLAIN")
			}
			write(append(caps, "250 SIZE 10485760")...)
		case strings.HasPrefix(upper, "HELO"):
			write("250 fake.test")
		case upper == "STARTTLS":
			write("220 2.0.0 Ready to start TLS")
			tc := tls.Server(conn, f.tlsCfg)
			if err := tc.Handshake(); err != nil {
				return
			}
			conn = tc
			overTLS = true
			r = bufio.NewReader(conn)
			w = bufio.NewWriter(conn)
		case strings.HasPrefix(upper, "AUTH PLAIN"):
			d.authPlain = strings.TrimSpace(cmd[len("AUTH PLAIN"):])
			write("235 2.7.0 Authentication successful")
		case strings.HasPrefix(upper, "MAIL FROM:"):
			d.from = angleAddr(cmd)
			write("250 2.1.0 Ok")
		case strings.HasPrefix(upper, "RCPT TO:"):
			d.to = append(d.to, angleAddr(cmd))
			write("250 2.1.5 Ok")
		case upper == "DATA":
			write("354 End data with <CR><LF>.<CR><LF>")
			body, err := readDotted(r)
			if err != nil {
				return
			}
			if f.rejectOnce() {
				write("451 4.3.0 Temporary local problem")
				continue
			}
			d.data = body
			d.overTLS = overTLS
			f.record(d)
			d = delivery{}
			write("250 2.0.0 Ok: queued")
		case upper == "RSET":
			d = delivery{}
			write("250 2.0.0 Ok")
		case upper == "NOOP":
			write("250 2.0.0 Ok")
		case upper == "QUIT":
			write("221 2.0.0 Bye")
			return
		default:
			write("500 5.5.1 Command unrecognized")
		}
	}
}

func (f *fakeSMTP) rejectOnce() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failData > 0 {
		f.failData--
		return true
	}
	return false
}

func (f *fakeSMTP) record(d delivery) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deliveries = append(f.deliveries, d)
}

// wait blocks until n messages have been accepted, or fails the test.
func (f *fakeSMTP) wait(t *testing.T, n int) []delivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		got := len(f.deliveries)
		out := append([]delivery(nil), f.deliveries...)
		f.mu.Unlock()
		if got >= n {
			return out
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d deliveries", n)
	return nil
}

// angleAddr extracts the address from "MAIL FROM:<a@b>" style commands.
func angleAddr(cmd string) string {
	start := strings.Index(cmd, "<")
	end := strings.Index(cmd, ">")
	if start < 0 || end < start {
		return ""
	}
	return cmd[start+1 : end]
}

// readDotted reads a DATA payload, undoing the transparency dot.
func readDotted(r *bufio.Reader) (string, error) {
	var b strings.Builder
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		if line == ".\r\n" || line == ".\n" {
			return b.String(), nil
		}
		if strings.HasPrefix(line, "..") {
			line = line[1:]
		}
		b.WriteString(line)
	}
}

// testCerts mints a self-signed certificate for 127.0.0.1 and returns
// the server and client TLS configurations that trust it. No network,
// no fixtures on disk.
func testCerts(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "dabet-mail-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("key pair: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12},
		&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}
