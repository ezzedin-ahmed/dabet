package ingest

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"

	"dabet/pkg/config"
)

// Object-store connectivity for §3's two topologies. Locally it is MinIO
// with a static key pair; in the managed target it is S3, where a pod
// assumes an IAM role through IRSA and there is no key pair to hand out at
// all. Everything below is unset by default and resolves to exactly the
// static-credential, path-style MinIO client this service has always
// built (docs §4.4: canonical names, secrets from the environment in v1
// and a secret manager in the k8s target).
const (
	// EnvS3Bucket is the embeddings bucket of §8.4.
	EnvS3Bucket = "S3_BUCKET"
	// EnvS3AccessKey and EnvS3SecretKey are the static credentials.
	// EnvS3SessionToken completes a temporary triple; it was missing
	// entirely before, which is why an assumed role could not be used.
	EnvS3AccessKey    = "S3_ACCESS_KEY"
	EnvS3SecretKey    = "S3_SECRET_KEY"
	EnvS3SessionToken = "S3_SESSION_TOKEN"
	// EnvS3Region is the bucket's region. Empty lets minio-go probe it,
	// which is what MinIO wants; real S3 should be told.
	EnvS3Region = "S3_REGION"
	// EnvS3CredentialsSource selects how credentials are found. See the
	// S3Source constants; the default keeps today's static behaviour.
	EnvS3CredentialsSource = "S3_CREDENTIALS_SOURCE"
	// EnvS3AddressingStyle is "auto" (default), "path" or "virtual".
	// Path style is right for MinIO and virtual-host style for S3, and
	// "auto" picks per endpoint.
	EnvS3AddressingStyle = "S3_ADDRESSING_STYLE"
)

// Credential sources accepted by S3_CREDENTIALS_SOURCE.
const (
	// SourceStatic is S3_ACCESS_KEY/S3_SECRET_KEY (+ optional session
	// token) and nothing else. The default, so local MinIO is unchanged.
	SourceStatic = "static"
	// SourceAuto uses the static keys when they are set, then the
	// web-identity token IRSA projects, then the full chain.
	SourceAuto = "auto"
	// SourceChain is static (if set), the AWS and MinIO environment
	// variables, then the container/instance metadata endpoints.
	SourceChain = "chain"
	// SourceEnv is the AWS and MinIO environment variables only.
	SourceEnv = "env"
	// SourceWebIdentity is IRSA only: AWS_ROLE_ARN plus the projected
	// AWS_WEB_IDENTITY_TOKEN_FILE, exchanged at STS. Missing either is a
	// startup error rather than a silent fall-through.
	SourceWebIdentity = "web-identity"
	// SourceIAM is the container/instance metadata endpoints only (EKS
	// Pod Identity, ECS task roles, EC2 instance profiles).
	SourceIAM = "iam"
)

// Addressing styles accepted by S3_ADDRESSING_STYLE.
const (
	AddressingAuto    = "auto"
	AddressingPath    = "path"
	AddressingVirtual = "virtual"
)

// S3Config is the object-store configuration.
type S3Config struct {
	// Endpoint is a URL such as http://localhost:9000 or
	// https://s3.eu-west-1.amazonaws.com.
	Endpoint string
	// Bucket is the embeddings bucket.
	Bucket string
	// AccessKey, SecretKey and SessionToken are the static credentials.
	// P4: never logged — see LogValue.
	AccessKey    string
	SecretKey    string
	SessionToken string
	// Region is the bucket's region ("" lets minio-go probe).
	Region string
	// CredentialsSource is one of the Source* constants.
	CredentialsSource string
	// AddressingStyle is one of the Addressing* constants.
	AddressingStyle string
}

// DefaultS3Config reads the S3_* environment. The static key defaults are
// applied only for the static source, so switching to IRSA does not leave
// a stale "minioadmin" in front of the credential chain.
func DefaultS3Config() (S3Config, error) {
	source := strings.ToLower(strings.TrimSpace(getenvDefault(EnvS3CredentialsSource, SourceStatic)))
	keyDefault := ""
	if source == SourceStatic {
		keyDefault = "minioadmin"
	}
	cfg := S3Config{
		Endpoint:          getenvDefault(config.EnvS3Endpoint, "http://localhost:9000"),
		Bucket:            getenvDefault(EnvS3Bucket, "embeddings"),
		AccessKey:         getenvDefault(EnvS3AccessKey, keyDefault),
		SecretKey:         getenvDefault(EnvS3SecretKey, keyDefault),
		SessionToken:      os.Getenv(EnvS3SessionToken),
		Region:            os.Getenv(EnvS3Region),
		CredentialsSource: source,
		AddressingStyle:   strings.ToLower(strings.TrimSpace(getenvDefault(EnvS3AddressingStyle, AddressingAuto))),
	}
	return cfg, cfg.validate()
}

func getenvDefault(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// validate rejects a misconfiguration at startup, where it is cheap, and
// names the variable at fault.
func (c S3Config) validate() error {
	switch c.CredentialsSource {
	case SourceStatic, SourceAuto, SourceChain, SourceEnv, SourceWebIdentity, "irsa", SourceIAM:
	default:
		return fmt.Errorf("environment variable %s: unknown source %q; supported: %s, %s, %s, %s, %s, %s",
			EnvS3CredentialsSource, c.CredentialsSource,
			SourceStatic, SourceAuto, SourceChain, SourceEnv, SourceWebIdentity, SourceIAM)
	}
	switch c.AddressingStyle {
	case AddressingAuto, AddressingPath, AddressingVirtual, "dns", "vhost":
	default:
		return fmt.Errorf("environment variable %s: unknown style %q; supported: %s, %s, %s",
			EnvS3AddressingStyle, c.AddressingStyle, AddressingAuto, AddressingPath, AddressingVirtual)
	}
	return nil
}

// LogValue redacts the credentials (P4).
func (c S3Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("endpoint", c.Endpoint),
		slog.String("bucket", c.Bucket),
		slog.String("region", c.Region),
		slog.String("credentials_source", c.CredentialsSource),
		slog.String("addressing_style", c.AddressingStyle),
		slog.Bool("static_keys_present", c.AccessKey != "" && c.SecretKey != ""),
	)
}

// String satisfies fmt.Stringer with the same redaction.
func (c S3Config) String() string {
	return fmt.Sprintf("s3{endpoint=%s bucket=%s region=%s source=%s style=%s credentials=redacted}",
		c.Endpoint, c.Bucket, c.Region, c.CredentialsSource, c.AddressingStyle)
}

// hasStaticKeys reports whether a usable static pair was configured.
func (c S3Config) hasStaticKeys() bool { return c.AccessKey != "" && c.SecretKey != "" }

// hasWebIdentity reports whether the pod has an IRSA projected token. This
// is the pair the EKS admission webhook injects for a service account
// annotated with a role.
func hasWebIdentity() bool {
	return os.Getenv("AWS_ROLE_ARN") != "" && os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != ""
}

// resolvedSource turns "auto" into the source it actually means, given the
// environment the process is running in. Everything else passes through
// (with the "irsa" alias folded in).
func (c S3Config) resolvedSource() string {
	switch c.CredentialsSource {
	case "irsa":
		return SourceWebIdentity
	case SourceAuto:
		switch {
		case c.hasStaticKeys():
			return SourceStatic
		case hasWebIdentity():
			return SourceWebIdentity
		default:
			return SourceChain
		}
	}
	return c.CredentialsSource
}

// providers is the ordered credential provider list. It is pure: building
// it contacts nothing, so a test can assert the selection without a
// metadata endpoint, an STS endpoint or a network at all. Retrieval — and
// therefore any HTTP — happens lazily, on the first signed request.
//
// minio-go v7.3.0's IAM provider is the whole AWS side of the chain in one
// object: given AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN it performs
// AssumeRoleWithWebIdentity against sts.<region>.amazonaws.com (IRSA);
// given AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE + _FULL_URI it does EKS Pod
// Identity; given AWS_CONTAINER_CREDENTIALS_RELATIVE_URI it does ECS task
// roles; otherwise it falls back to IMDSv2 on 169.254.169.254. It also
// refreshes on expiry, which is what makes a rotating role session work.
func (c S3Config) providers() ([]credentials.Provider, error) {
	static := &credentials.Static{Value: credentials.Value{
		AccessKeyID:     c.AccessKey,
		SecretAccessKey: c.SecretKey,
		SessionToken:    c.SessionToken,
		SignerType:      credentials.SignatureV4,
	}}
	iam := &credentials.IAM{Region: c.Region}

	switch c.resolvedSource() {
	case SourceStatic:
		return []credentials.Provider{static}, nil
	case SourceEnv:
		return []credentials.Provider{&credentials.EnvAWS{}, &credentials.EnvMinio{}}, nil
	case SourceWebIdentity:
		if !hasWebIdentity() {
			return nil, fmt.Errorf("%s=%s needs AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE; "+
				"on EKS those are injected for a service account annotated with a role",
				EnvS3CredentialsSource, SourceWebIdentity)
		}
		return []credentials.Provider{iam}, nil
	case SourceIAM:
		return []credentials.Provider{iam}, nil
	case SourceChain:
		providers := []credentials.Provider{}
		if c.hasStaticKeys() {
			providers = append(providers, static)
		}
		return append(providers, &credentials.EnvAWS{}, &credentials.EnvMinio{}, iam), nil
	}
	return nil, fmt.Errorf("environment variable %s: unknown source %q", EnvS3CredentialsSource, c.CredentialsSource)
}

// creds wraps providers in a *credentials.Credentials. A single provider
// is used directly, so the default static path is precisely the
// credentials.NewStaticV4 call it replaces.
func (c S3Config) creds() (*credentials.Credentials, error) {
	providers, err := c.providers()
	if err != nil {
		return nil, err
	}
	if len(providers) == 1 {
		return credentials.New(providers[0]), nil
	}
	return credentials.NewChainCredentials(providers), nil
}

// bucketLookup resolves the addressing style. "auto" is minio-go's own
// rule: virtual-host for S3, Google and Aliyun, path for everything else —
// which is to say path for MinIO, exactly as before.
func (c S3Config) bucketLookup(endpoint *url.URL) minio.BucketLookupType {
	switch c.AddressingStyle {
	case AddressingPath:
		return minio.BucketLookupPath
	case AddressingVirtual, "dns", "vhost":
		return minio.BucketLookupDNS
	}
	if s3utils.IsVirtualHostSupported(*endpoint, c.Bucket) {
		return minio.BucketLookupDNS
	}
	return minio.BucketLookupPath
}

// minioOptions renders the configuration as minio-go options.
func (c S3Config) minioOptions(endpoint *url.URL) (*minio.Options, error) {
	creds, err := c.creds()
	if err != nil {
		return nil, err
	}
	return &minio.Options{
		Creds:        creds,
		Secure:       endpoint.Scheme == "https",
		Region:       c.Region,
		BucketLookup: c.bucketLookup(endpoint),
	}, nil
}

// parseS3Endpoint validates the endpoint URL and returns it.
func parseS3Endpoint(endpoint string) (*url.URL, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3 endpoint: %w", err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("s3 endpoint %q: missing host", endpoint)
	}
	return u, nil
}
