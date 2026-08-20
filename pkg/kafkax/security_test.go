package kafkax

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

// securityEnv is every variable security.go reads, so a test can prove it
// started from a clean slate rather than from the developer's shell.
var securityEnv = []string{
	EnvTLSEnabled, EnvTLSCAFile, EnvTLSCertFile, EnvTLSKeyFile,
	EnvTLSServerName, EnvTLSSkipVerify,
	EnvSASLMechanism, EnvSASLUsername, EnvSASLPassword, EnvSASLPasswordFile,
	"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
}

func clearSecurityEnv(t *testing.T) {
	t.Helper()
	for _, k := range securityEnv {
		t.Setenv(k, "")
	}
}

// ---------------------------------------------------------------------------
// The compatibility pin.
// ---------------------------------------------------------------------------

// TestUnsetSecurityAddsNothing is the property the Compose profile, `make
// e2e` and the load harness all rest on: with none of the KAFKA_TLS_* or
// KAFKA_SASL_* variables set, this package contributes exactly zero
// franz-go options, so the client it builds is the same client it built
// before transport security existed. A future change that quietly starts
// adding an option here fails this test.
func TestUnsetSecurityAddsNothing(t *testing.T) {
	clearSecurityEnv(t)

	sec, err := DefaultSecurityConfig()
	if err != nil {
		t.Fatal(err)
	}
	if sec != (SecurityConfig{}) {
		t.Fatalf("DefaultSecurityConfig with no env = %+v, want the zero value", sec)
	}
	if sec.Enabled() {
		t.Fatal("Enabled() is true with nothing configured")
	}
	opts, err := sec.Options()
	if err != nil {
		t.Fatal(err)
	}
	if opts != nil {
		t.Fatalf("Options() = %d options, want a nil slice", len(opts))
	}
}

// TestUnsetSecurityLeavesTheConsumerClientUnchanged checks the same thing
// one level up, on the real client: no SASL mechanism, no TLS dialer, and
// the exact option count the consumer had before.
func TestUnsetSecurityLeavesTheConsumerClientUnchanged(t *testing.T) {
	clearSecurityEnv(t)

	c, err := NewConsumer([]string{"127.0.0.1:9092"}, "g", []string{"messages.v1"},
		func(context.Context, *kgo.Record) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if got := c.cl.OptValue(kgo.SASL); got != nil {
		if mechs, ok := got.([]sasl.Mechanism); ok && len(mechs) > 0 {
			t.Fatalf("plaintext consumer has %d SASL mechanisms", len(mechs))
		}
	}
	if got := c.cl.OptValue(kgo.DialTLSConfig); got != nil {
		if cfg, ok := got.(*tls.Config); ok && cfg != nil {
			t.Fatal("plaintext consumer has a TLS dial config")
		}
	}
	// The option list itself: 10 required options, nothing appended.
	if got := len(c.clientOptions([]string{"127.0.0.1:9092"}, "g", []string{"messages.v1"}, nil)); got != 10 {
		t.Fatalf("consumer built %d options, want the 10 it has always built", got)
	}
}

// TestUnsetSecurityLeavesTheProducerClientUnchanged is the producer half.
func TestUnsetSecurityLeavesTheProducerClientUnchanged(t *testing.T) {
	clearSecurityEnv(t)

	p, err := NewProducer([]string{"127.0.0.1:9092"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if mechs, ok := p.cl.OptValue(kgo.SASL).([]sasl.Mechanism); ok && len(mechs) > 0 {
		t.Fatalf("plaintext producer has %d SASL mechanisms", len(mechs))
	}
	if cfg, ok := p.cl.OptValue(kgo.DialTLSConfig).(*tls.Config); ok && cfg != nil {
		t.Fatal("plaintext producer has a TLS dial config")
	}
}

// ---------------------------------------------------------------------------
// Mechanisms.
// ---------------------------------------------------------------------------

// TestSASLMechanisms covers every accepted mechanism and the spellings an
// operator is likely to paste out of an AWS console or a Helm values file.
func TestSASLMechanisms(t *testing.T) {
	cases := []struct {
		env  string
		want string
	}{
		{"SCRAM-SHA-512", "SCRAM-SHA-512"},
		{"scram-sha-512", "SCRAM-SHA-512"},
		{"SCRAM_SHA_512", "SCRAM-SHA-512"},
		{"  scram-sha-512  ", "SCRAM-SHA-512"},
		{"SCRAM-SHA-256", "SCRAM-SHA-256"},
		{"scram_sha_256", "SCRAM-SHA-256"},
		{"PLAIN", "PLAIN"},
		{"plain", "PLAIN"},
		{"AWS_MSK_IAM", "AWS_MSK_IAM"},
		{"aws-msk-iam", "AWS_MSK_IAM"},
	}
	for _, tc := range cases {
		t.Run(strings.TrimSpace(tc.env), func(t *testing.T) {
			sec := SecurityConfig{
				SASLMechanism: tc.env,
				SASLUsername:  "AKIAEXAMPLE",
				SASLPassword:  "s3cret",
			}
			opts, err := sec.Options()
			if err != nil {
				t.Fatal(err)
			}
			if len(opts) != 1 {
				t.Fatalf("got %d options, want just kgo.SASL", len(opts))
			}
			mech := mechanismOf(t, opts)
			if mech.Name() != tc.want {
				t.Fatalf("mechanism = %q, want %q", mech.Name(), tc.want)
			}
		})
	}
}

// TestUnknownMechanismIsAStartupError: §4.4 misconfiguration fails loudly
// at startup, and the message names what is accepted rather than leaving
// the operator to guess.
func TestUnknownMechanismIsAStartupError(t *testing.T) {
	sec := SecurityConfig{SASLMechanism: "GSSAPI", SASLUsername: "u", SASLPassword: "p"}
	_, err := sec.Options()
	if err == nil {
		t.Fatal("an unknown mechanism must be an error, not a silent fallback to plaintext")
	}
	for _, want := range []string{"GSSAPI", SASLScramSHA512, SASLScramSHA256, SASLPlain, SASLAWSMSKIAM} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}

	// And through the environment, which is how it will actually happen.
	clearSecurityEnv(t)
	t.Setenv(EnvSASLMechanism, "GSSAPI")
	t.Setenv(EnvSASLUsername, "u")
	t.Setenv(EnvSASLPassword, "p")
	if _, err := NewProducer([]string{"127.0.0.1:9092"}); err == nil {
		t.Fatal("NewProducer accepted an unknown SASL mechanism")
	}
	if _, err := NewConsumer([]string{"127.0.0.1:9092"}, "g", []string{"t"},
		func(context.Context, *kgo.Record) error { return nil }); err == nil {
		t.Fatal("NewConsumer accepted an unknown SASL mechanism")
	}
}

// TestMechanismRequiresCredentials: a mechanism with no username/password
// is a misconfiguration, not an anonymous connection.
func TestMechanismRequiresCredentials(t *testing.T) {
	for _, mech := range []string{SASLScramSHA512, SASLScramSHA256, SASLPlain} {
		sec := SecurityConfig{SASLMechanism: mech}
		if _, err := sec.Options(); err == nil {
			t.Errorf("%s with no credentials was accepted", mech)
		}
		sec.SASLUsername = "u"
		if _, err := sec.Options(); err == nil {
			t.Errorf("%s with no password was accepted", mech)
		}
	}
}

// TestMSKIAMCredentialSources pins where AWS_MSK_IAM looks: the canonical
// KAFKA_SASL_* pair first, then the standard AWS environment, and an error
// when neither is present.
func TestMSKIAMCredentialSources(t *testing.T) {
	t.Run("kafka vars", func(t *testing.T) {
		clearSecurityEnv(t)
		sec := SecurityConfig{SASLMechanism: SASLAWSMSKIAM, SASLUsername: "AKIA1", SASLPassword: "s1"}
		auth, err := sec.awsAuth()
		if err != nil {
			t.Fatal(err)
		}
		if auth.AccessKey != "AKIA1" || auth.SecretKey != "s1" {
			t.Fatal("KAFKA_SASL_USERNAME/PASSWORD were not used as the AWS key pair")
		}
	})
	t.Run("aws vars", func(t *testing.T) {
		clearSecurityEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA2")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "s2")
		t.Setenv("AWS_SESSION_TOKEN", "tok")
		sec := SecurityConfig{SASLMechanism: SASLAWSMSKIAM}
		auth, err := sec.awsAuth()
		if err != nil {
			t.Fatal(err)
		}
		if auth.AccessKey != "AKIA2" || auth.SecretKey != "s2" || auth.SessionToken != "tok" {
			t.Fatalf("AWS environment not used: %+v", auth.AccessKey)
		}
		opts, err := sec.Options()
		if err != nil {
			t.Fatal(err)
		}
		if got := mechanismOf(t, opts).Name(); got != "AWS_MSK_IAM" {
			t.Fatalf("mechanism = %q", got)
		}
	})
	t.Run("nothing", func(t *testing.T) {
		clearSecurityEnv(t)
		sec := SecurityConfig{SASLMechanism: SASLAWSMSKIAM}
		if _, err := sec.Options(); err == nil {
			t.Fatal("AWS_MSK_IAM with no credentials anywhere must fail at startup")
		}
	})
}

// ---------------------------------------------------------------------------
// TLS.
// ---------------------------------------------------------------------------

// TestTLSCombinations walks the TLS matrix: plain TLS, TLS with a custom
// CA, TLS with skip-verify, TLS with a server name, and mTLS.
func TestTLSCombinations(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client-key.pem")
	writeSelfSigned(t, caPath, certPath, keyPath)

	t.Run("tls only", func(t *testing.T) {
		cfg := tlsConfigOf(t, SecurityConfig{TLSEnabled: true})
		if cfg.RootCAs != nil {
			t.Error("no CA file was given but RootCAs was set")
		}
		if cfg.InsecureSkipVerify {
			t.Error("verification is off by default")
		}
		if cfg.MinVersion != tls.VersionTLS12 {
			t.Errorf("MinVersion = %x, want TLS 1.2", cfg.MinVersion)
		}
		if len(cfg.Certificates) != 0 {
			t.Error("no client certificate was given but one was loaded")
		}
	})

	t.Run("custom ca", func(t *testing.T) {
		cfg := tlsConfigOf(t, SecurityConfig{TLSEnabled: true, CAFile: caPath})
		if cfg.RootCAs == nil {
			t.Fatal("CA file was not loaded")
		}
	})

	t.Run("skip verify", func(t *testing.T) {
		cfg := tlsConfigOf(t, SecurityConfig{TLSEnabled: true, SkipVerify: true})
		if !cfg.InsecureSkipVerify {
			t.Fatal("KAFKA_TLS_SKIP_VERIFY had no effect")
		}
	})

	t.Run("server name", func(t *testing.T) {
		cfg := tlsConfigOf(t, SecurityConfig{TLSEnabled: true, ServerName: "b-1.msk.example"})
		if cfg.ServerName != "b-1.msk.example" {
			t.Fatalf("ServerName = %q", cfg.ServerName)
		}
	})

	t.Run("mtls", func(t *testing.T) {
		cfg := tlsConfigOf(t, SecurityConfig{TLSEnabled: true, CertFile: certPath, KeyFile: keyPath})
		if len(cfg.Certificates) != 1 {
			t.Fatalf("loaded %d client certificates, want 1", len(cfg.Certificates))
		}
	})

	t.Run("tls and sasl together", func(t *testing.T) {
		opts, err := SecurityConfig{
			TLSEnabled:    true,
			CAFile:        caPath,
			SASLMechanism: SASLScramSHA512,
			SASLUsername:  "u",
			SASLPassword:  "p",
		}.Options()
		if err != nil {
			t.Fatal(err)
		}
		if len(opts) != 2 {
			t.Fatalf("got %d options, want DialTLSConfig and SASL", len(opts))
		}
		if got := mechanismOf(t, opts).Name(); got != SASLScramSHA512 {
			t.Fatalf("mechanism = %q", got)
		}
	})
}

// TestTLSMisconfigurationFailsAtStartup: bad paths, half a keypair, and
// TLS material without KAFKA_TLS_ENABLED all fail loudly rather than
// leaving a plaintext connection nobody expected.
func TestTLSMisconfigurationFailsAtStartup(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client-key.pem")
	writeSelfSigned(t, caPath, certPath, keyPath)

	junk := filepath.Join(dir, "junk.pem")
	if err := os.WriteFile(junk, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := map[string]SecurityConfig{
		"missing ca":         {TLSEnabled: true, CAFile: filepath.Join(dir, "nope.pem")},
		"ca with no certs":   {TLSEnabled: true, CAFile: junk},
		"cert without key":   {TLSEnabled: true, CertFile: certPath},
		"key without cert":   {TLSEnabled: true, KeyFile: keyPath},
		"unreadable keypair": {TLSEnabled: true, CertFile: junk, KeyFile: junk},
		"ca without tls":     {CAFile: caPath},
		"skip without tls":   {SkipVerify: true},
	}
	for name, sec := range cases {
		if _, err := sec.Options(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestSecurityFromEnvironment is §4.4 end to end: canonical names, values
// read from the environment, and a bad boolean is an error.
func TestSecurityFromEnvironment(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeSelfSigned(t, caPath, filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem"))
	pwPath := filepath.Join(dir, "password")
	if err := os.WriteFile(pwPath, []byte("from-the-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	clearSecurityEnv(t)
	t.Setenv(EnvTLSEnabled, "true")
	t.Setenv(EnvTLSCAFile, caPath)
	t.Setenv(EnvTLSSkipVerify, "false")
	t.Setenv(EnvTLSServerName, "b-2.msk.example")
	t.Setenv(EnvSASLMechanism, "SCRAM-SHA-512")
	t.Setenv(EnvSASLUsername, "dabet")
	t.Setenv(EnvSASLPassword, "ignored")
	t.Setenv(EnvSASLPasswordFile, pwPath)

	sec, err := DefaultSecurityConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !sec.TLSEnabled || sec.CAFile != caPath || sec.SkipVerify || sec.ServerName != "b-2.msk.example" {
		t.Fatalf("TLS values not read: %+v", sec)
	}
	// The file wins over the inline variable, and its trailing newline is
	// stripped — a secret volume always ends up with one.
	if sec.SASLPassword != "from-the-file" {
		t.Fatalf("password file not used (got %d bytes)", len(sec.SASLPassword))
	}
	if _, err := sec.Options(); err != nil {
		t.Fatal(err)
	}

	t.Setenv(EnvSASLPasswordFile, filepath.Join(dir, "absent"))
	if _, err := DefaultSecurityConfig(); err == nil {
		t.Error("a missing password file must be a startup error")
	}
	t.Setenv(EnvSASLPasswordFile, "")
	t.Setenv(EnvTLSEnabled, "banana")
	if _, err := DefaultSecurityConfig(); err == nil {
		t.Error("an unparseable boolean must be an error, not a silent default")
	}
}

// TestEnvBoolSpellings pins the accepted spellings.
func TestEnvBoolSpellings(t *testing.T) {
	cases := map[string]bool{
		"true": true, "TRUE": true, "1": true, "yes": true, "on": true, "t": true,
		"false": false, "0": false, "no": false, "off": false, "f": false,
	}
	for v, want := range cases {
		t.Setenv(EnvTLSEnabled, v)
		got, err := envBool(EnvTLSEnabled, !want)
		if err != nil {
			t.Fatalf("%q: %v", v, err)
		}
		if got != want {
			t.Errorf("%q = %v, want %v", v, got, want)
		}
	}
	t.Setenv(EnvTLSEnabled, "")
	if got, _ := envBool(EnvTLSEnabled, true); !got {
		t.Error("unset must yield the default")
	}
}

// TestCredentialsAreNeverRendered is P4: neither logging nor formatting a
// SecurityConfig may reveal the password.
func TestCredentialsAreNeverRendered(t *testing.T) {
	sec := SecurityConfig{
		TLSEnabled:    true,
		SASLMechanism: SASLScramSHA512,
		SASLUsername:  "dabet-msk-user",
		SASLPassword:  "hunter2-do-not-log",
	}
	rendered := []string{
		sec.String(),
		renderLogValue(sec.LogValue()),
	}
	for _, s := range rendered {
		for _, secret := range []string{"hunter2-do-not-log", "dabet-msk-user"} {
			if strings.Contains(s, secret) {
				t.Fatalf("rendered form leaked %q: %s", secret, s)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// mechanismOf builds a client from opts and pulls the SASL mechanism back
// out with franz-go's own option introspection, so the test asserts on
// what the client really got rather than on our intent.
func mechanismOf(t *testing.T, opts []kgo.Opt) sasl.Mechanism {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers("127.0.0.1:9092")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	mechs, ok := cl.OptValue(kgo.SASL).([]sasl.Mechanism)
	if !ok || len(mechs) != 1 {
		t.Fatalf("client has %v SASL mechanisms, want exactly 1", cl.OptValue(kgo.SASL))
	}
	return mechs[0]
}

// tlsConfigOf is the same trick for the TLS dial config.
func tlsConfigOf(t *testing.T, sec SecurityConfig) *tls.Config {
	t.Helper()
	opts, err := sec.Options()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers("127.0.0.1:9092")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cfg, _ := cl.OptValue(kgo.DialTLSConfig).(*tls.Config)
	if cfg == nil {
		t.Fatal("no TLS dial config on the client")
	}
	return cfg
}

func renderLogValue(v slog.Value) string {
	var b strings.Builder
	for _, a := range v.Group() {
		b.WriteString(a.Key)
		b.WriteString("=")
		b.WriteString(a.Value.String())
		b.WriteString(" ")
	}
	return b.String()
}

// writeSelfSigned writes a self-signed certificate to caPath and certPath
// and its key to keyPath — enough material for the CA-pool and keypair
// paths without reaching for a real PKI.
func writeSelfSigned(t *testing.T, caPath, certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "dabet-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	for _, p := range []string{caPath, certPath} {
		if err := os.WriteFile(p, certPEM, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
}
