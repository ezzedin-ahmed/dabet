package kafkax

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Transport security for the broker connection (docs §3's managed-cloud
// row: Amazon MSK and every other managed Kafka refuse plaintext). Every
// variable below is unset by default, and with all of them unset this
// package builds exactly the client options it built before they existed —
// so the Compose profile keeps talking plaintext to localhost:9092 with no
// code and no configuration change (docs §3: "topology differences are
// configuration only").
const (
	// EnvTLSEnabled turns on TLS for broker connections ("true"/"1"/"yes").
	EnvTLSEnabled = "KAFKA_TLS_ENABLED"
	// EnvTLSCAFile is a PEM bundle to verify the broker against, replacing
	// the system roots. MSK's public brokers use public CAs, so this is
	// only needed for a private CA.
	EnvTLSCAFile = "KAFKA_TLS_CA_FILE"
	// EnvTLSCertFile and EnvTLSKeyFile are a client certificate for mTLS
	// (MSK's TLS-client-authentication mode). Both or neither.
	EnvTLSCertFile = "KAFKA_TLS_CERT_FILE"
	EnvTLSKeyFile  = "KAFKA_TLS_KEY_FILE"
	// EnvTLSServerName overrides the SNI/verification hostname.
	EnvTLSServerName = "KAFKA_TLS_SERVER_NAME"
	// EnvTLSSkipVerify disables certificate verification. Escape hatch for
	// a self-signed broker in a scratch environment; never for production.
	EnvTLSSkipVerify = "KAFKA_TLS_SKIP_VERIFY"

	// EnvSASLMechanism selects the SASL mechanism. Empty means no SASL.
	// Recognised: SCRAM-SHA-512, SCRAM-SHA-256, PLAIN, AWS_MSK_IAM.
	EnvSASLMechanism = "KAFKA_SASL_MECHANISM"
	// EnvSASLUsername and EnvSASLPassword are the SASL credentials. For
	// AWS_MSK_IAM they are read as the AWS access key id and secret access
	// key; leaving them unset falls back to the standard AWS environment.
	EnvSASLUsername = "KAFKA_SASL_USERNAME"
	EnvSASLPassword = "KAFKA_SASL_PASSWORD"
	// EnvSASLPasswordFile reads the password from a file instead, which is
	// how a Kubernetes secret volume and most secret-manager CSI drivers
	// present it (docs §4.4: secrets come from the environment in v1 and a
	// secret manager in the k8s target). It wins over EnvSASLPassword.
	EnvSASLPasswordFile = "KAFKA_SASL_PASSWORD_FILE"
)

// SASL mechanism names accepted by EnvSASLMechanism, compared
// case-insensitively and ignoring '-'/'_' so "scram_sha_512" also works.
const (
	SASLScramSHA512 = "SCRAM-SHA-512"
	SASLScramSHA256 = "SCRAM-SHA-256"
	SASLPlain       = "PLAIN"
	SASLAWSMSKIAM   = "AWS_MSK_IAM"
)

// SecurityConfig is the broker transport security of docs §4.4: canonical
// KAFKA_* names, every value a default rather than a constant, and secrets
// read from the environment (or a file the secret manager mounts).
//
// The zero value means "plaintext, no SASL" — today's behaviour — and
// produces no franz-go options at all.
type SecurityConfig struct {
	// TLSEnabled wraps broker connections in TLS.
	TLSEnabled bool
	// CAFile is a PEM bundle replacing the system roots.
	CAFile string
	// CertFile and KeyFile are a client certificate for mTLS.
	CertFile string
	KeyFile  string
	// ServerName overrides the SNI / verification hostname.
	ServerName string
	// SkipVerify disables certificate verification.
	SkipVerify bool

	// SASLMechanism is one of the SASL* constants, or "" for no SASL.
	SASLMechanism string
	// SASLUsername and SASLPassword are the credentials. P4: they are
	// never logged — see LogValue.
	SASLUsername string
	SASLPassword string
}

// DefaultSecurityConfig reads the KAFKA_TLS_* and KAFKA_SASL_* variables.
// With none of them set it returns the zero value, i.e. plaintext.
func DefaultSecurityConfig() (SecurityConfig, error) {
	var sec SecurityConfig
	var err error
	if sec.TLSEnabled, err = envBool(EnvTLSEnabled, false); err != nil {
		return SecurityConfig{}, err
	}
	if sec.SkipVerify, err = envBool(EnvTLSSkipVerify, false); err != nil {
		return SecurityConfig{}, err
	}
	sec.CAFile = os.Getenv(EnvTLSCAFile)
	sec.CertFile = os.Getenv(EnvTLSCertFile)
	sec.KeyFile = os.Getenv(EnvTLSKeyFile)
	sec.ServerName = os.Getenv(EnvTLSServerName)
	sec.SASLMechanism = os.Getenv(EnvSASLMechanism)
	sec.SASLUsername = os.Getenv(EnvSASLUsername)
	sec.SASLPassword = os.Getenv(EnvSASLPassword)
	if f := os.Getenv(EnvSASLPasswordFile); f != "" {
		b, rerr := os.ReadFile(f)
		if rerr != nil {
			// The path is safe to name; the contents never are.
			return SecurityConfig{}, fmt.Errorf("environment variable %s: %w", EnvSASLPasswordFile, rerr)
		}
		sec.SASLPassword = strings.TrimRight(string(b), "\r\n")
	}
	return sec, nil
}

// Enabled reports whether anything at all is configured. When it is false
// Options returns no options, which is what keeps the local plaintext
// client byte-identical to the pre-security one.
func (s SecurityConfig) Enabled() bool {
	return s.TLSEnabled || s.SASLMechanism != "" || s.CAFile != "" ||
		s.CertFile != "" || s.KeyFile != "" || s.ServerName != "" || s.SkipVerify
}

// LogValue redacts the credentials (P4: never log secrets). It also makes
// %v and %+v on a SecurityConfig safe, because fmt prefers Stringer.
func (s SecurityConfig) LogValue() slog.Value {
	attrs := []slog.Attr{
		slog.Bool("tls_enabled", s.TLSEnabled),
		slog.Bool("tls_skip_verify", s.SkipVerify),
		slog.String("sasl_mechanism", s.SASLMechanism),
	}
	if s.CAFile != "" {
		attrs = append(attrs, slog.String("tls_ca_file", s.CAFile))
	}
	if s.CertFile != "" {
		attrs = append(attrs, slog.String("tls_cert_file", s.CertFile))
	}
	if s.ServerName != "" {
		attrs = append(attrs, slog.String("tls_server_name", s.ServerName))
	}
	if s.SASLUsername != "" {
		attrs = append(attrs, slog.String("sasl_username", "[redacted]"))
	}
	if s.SASLPassword != "" {
		attrs = append(attrs, slog.String("sasl_password", "[redacted]"))
	}
	return slog.GroupValue(attrs...)
}

// String satisfies fmt.Stringer with the same redaction, so a
// SecurityConfig cannot leak a password through a %v in an error.
func (s SecurityConfig) String() string {
	return fmt.Sprintf("kafka security{tls=%t skip_verify=%t sasl=%q credentials=redacted}",
		s.TLSEnabled, s.SkipVerify, s.SASLMechanism)
}

// Options builds the franz-go options for this configuration. It returns
// nil, nil for the zero value: an unconfigured process appends nothing and
// therefore builds exactly the client it built before this file existed.
//
// Every failure here is a misconfiguration and is returned so the caller
// fails at startup with a clear message (a *runtime* auth failure is a
// different thing entirely and is left to franz-go's retries, per §4.7 —
// consumption must not stop).
func (s SecurityConfig) Options() ([]kgo.Opt, error) {
	if !s.Enabled() {
		return nil, nil
	}
	var opts []kgo.Opt

	if s.TLSEnabled {
		cfg, err := s.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	} else if s.CAFile != "" || s.CertFile != "" || s.KeyFile != "" || s.ServerName != "" || s.SkipVerify {
		return nil, fmt.Errorf("kafka security: %s is set but %s is not; TLS material without TLS would be silently ignored",
			firstSet(map[string]string{
				EnvTLSCAFile: s.CAFile, EnvTLSCertFile: s.CertFile,
				EnvTLSKeyFile: s.KeyFile, EnvTLSServerName: s.ServerName,
			}, EnvTLSSkipVerify), EnvTLSEnabled)
	}

	mech, err := s.mechanism()
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

// tlsConfig assembles the *tls.Config, reading any CA bundle and client
// certificate now rather than on first connect, so a bad path is a startup
// error and not a mysterious dial failure an hour later.
func (s SecurityConfig) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         s.ServerName,
		InsecureSkipVerify: s.SkipVerify, //nolint:gosec // opt-in via KAFKA_TLS_SKIP_VERIFY, documented as non-production
	}
	if s.CAFile != "" {
		pem, err := os.ReadFile(s.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka security: %s: %w", EnvTLSCAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka security: %s %q contains no PEM certificates", EnvTLSCAFile, s.CAFile)
		}
		cfg.RootCAs = pool
	}
	switch {
	case s.CertFile != "" && s.KeyFile != "":
		cert, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafka security: %s/%s: %w", EnvTLSCertFile, EnvTLSKeyFile, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	case s.CertFile != "" || s.KeyFile != "":
		return nil, fmt.Errorf("kafka security: %s and %s must be set together", EnvTLSCertFile, EnvTLSKeyFile)
	}
	return cfg, nil
}

// mechanism resolves KAFKA_SASL_MECHANISM. An unrecognised name is an
// error naming what is accepted, never a silent fallback to no SASL.
func (s SecurityConfig) mechanism() (sasl.Mechanism, error) {
	name := normaliseMechanism(s.SASLMechanism)
	if name == "" {
		return nil, nil
	}
	if name == SASLAWSMSKIAM {
		return s.mskIAMMechanism()
	}
	if s.SASLUsername == "" || s.SASLPassword == "" {
		return nil, fmt.Errorf("kafka security: %s=%s requires %s and %s (or %s)",
			EnvSASLMechanism, s.SASLMechanism, EnvSASLUsername, EnvSASLPassword, EnvSASLPasswordFile)
	}
	switch name {
	case SASLScramSHA512:
		return scram.Auth{User: s.SASLUsername, Pass: s.SASLPassword}.AsSha512Mechanism(), nil
	case SASLScramSHA256:
		return scram.Auth{User: s.SASLUsername, Pass: s.SASLPassword}.AsSha256Mechanism(), nil
	case SASLPlain:
		return plain.Auth{User: s.SASLUsername, Pass: s.SASLPassword}.AsMechanism(), nil
	}
	return nil, fmt.Errorf("kafka security: unknown %s %q; supported: %s, %s, %s, %s",
		EnvSASLMechanism, s.SASLMechanism, SASLScramSHA512, SASLScramSHA256, SASLPlain, SASLAWSMSKIAM)
}

// mskIAMMechanism builds AWS_MSK_IAM. franz-go implements the mechanism in
// pure standard library (pkg/sasl/aws signs the challenge itself), so this
// costs no new dependency — but it is only the signing half. Turning an
// IRSA projected token into an access key/secret/session token is an STS
// AssumeRoleWithWebIdentity call, and doing that properly means pulling in
// aws-sdk-go-v2. We deliberately do not: the credentials are read from the
// environment, so AWS_MSK_IAM here covers an IAM user's keys or creds an
// init container / instance profile has already exported. For a pod that
// only has a projected service-account token, SCRAM-SHA-512 with MSK's
// Secrets Manager integration is the supported path.
//
// The credentials are re-read on every authentication, so a process whose
// environment is refreshed picks up the new session token without a
// restart. Presence is validated here so a typo fails at startup.
func (s SecurityConfig) mskIAMMechanism() (sasl.Mechanism, error) {
	if _, err := s.awsAuth(); err != nil {
		return nil, err
	}
	return aws.ManagedStreamingIAM(func(context.Context) (aws.Auth, error) {
		return s.awsAuth()
	}), nil
}

// awsAuth resolves MSK IAM credentials: the canonical KAFKA_SASL_* pair
// first (so one mechanism switch changes nothing else), then the standard
// AWS environment.
func (s SecurityConfig) awsAuth() (aws.Auth, error) {
	key, secret := s.SASLUsername, s.SASLPassword
	if key == "" || secret == "" {
		key, secret = os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if key == "" || secret == "" {
		return aws.Auth{}, fmt.Errorf("kafka security: %s=%s needs credentials in %s/%s or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY",
			EnvSASLMechanism, SASLAWSMSKIAM, EnvSASLUsername, EnvSASLPassword)
	}
	return aws.Auth{
		AccessKey:    key,
		SecretKey:    secret,
		SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
	}, nil
}

// normaliseMechanism accepts the mechanism name in any of the spellings
// that appear in AWS consoles, Helm values and Kafka docs.
func normaliseMechanism(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ToUpper(s)
	s = strings.ReplaceAll(s, "_", "-")
	switch s {
	case "SCRAM-SHA-512", "SCRAMSHA512", "SCRAM-SHA512":
		return SASLScramSHA512
	case "SCRAM-SHA-256", "SCRAMSHA256", "SCRAM-SHA256":
		return SASLScramSHA256
	case "PLAIN":
		return SASLPlain
	case "AWS-MSK-IAM", "AWSMSKIAM", "MSK-IAM", "OAUTHBEARER-AWS-MSK-IAM":
		return SASLAWSMSKIAM
	}
	return s // unrecognised; mechanism() reports it by name
}

// firstSet names the first variable in vars with a non-empty value, for an
// error message that points at what the operator actually set. Values are
// paths, never secrets.
func firstSet(vars map[string]string, fallback string) string {
	for _, name := range []string{EnvTLSCAFile, EnvTLSCertFile, EnvTLSKeyFile, EnvTLSServerName} {
		if vars[name] != "" {
			return name
		}
	}
	return fallback
}

// envBool parses a boolean environment variable. Unset is def; an
// unparseable value is an error, matching config.GetInt's contract.
func envBool(name string, def bool) (bool, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def, nil
	}
	switch strings.ToLower(v) {
	case "yes", "on":
		return true, nil
	case "no", "off":
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("environment variable %s: %q is not a boolean", name, v)
	}
	return b, nil
}
