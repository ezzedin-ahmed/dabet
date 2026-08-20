package job

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Nothing in this file contacts a network: the whole point of building the
// provider list separately from retrieving credentials is that the
// selection can be asserted without an STS endpoint, a metadata endpoint
// or an S3 bucket.

// s3Env is every variable s3config.go reads, including the AWS ones the
// IRSA admission webhook injects.
var s3Env = []string{
	"S3_ENDPOINT", EnvS3Bucket, EnvS3AccessKey, EnvS3SecretKey, EnvS3SessionToken,
	EnvS3Region, EnvS3CredentialsSource, EnvS3AddressingStyle,
	"AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE",
	"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION",
}

func clearS3Env(t *testing.T) {
	t.Helper()
	for _, k := range s3Env {
		t.Setenv(k, "")
	}
}

// providerNames renders the selected chain as type names, which is what
// the tests assert on.
func providerNames(t *testing.T, cfg S3Config) []string {
	t.Helper()
	providers, err := cfg.providers()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, p := range providers {
		out = append(out, reflect.TypeOf(p).Elem().Name())
	}
	return out
}

// TestDefaultS3ConfigIsTodaysStaticMinioClient is the compatibility pin:
// with no S3_* variable set, the configuration is the static-credential,
// path-style, minioadmin/minioadmin MinIO client this job has always
// built. The Compose profile and `make e2e` depend on it exactly.
func TestDefaultS3ConfigIsTodaysStaticMinioClient(t *testing.T) {
	clearS3Env(t)

	cfg, err := DefaultS3Config()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoint != "http://localhost:9000" || cfg.Bucket != "embeddings" {
		t.Fatalf("endpoint/bucket defaults changed: %+v", cfg)
	}
	if cfg.AccessKey != "minioadmin" || cfg.SecretKey != "minioadmin" || cfg.SessionToken != "" {
		t.Fatalf("static credential defaults changed: source=%s", cfg.CredentialsSource)
	}
	if cfg.CredentialsSource != SourceStatic {
		t.Fatalf("default credentials source = %q, want %q", cfg.CredentialsSource, SourceStatic)
	}
	if cfg.Region != "" {
		t.Fatalf("region defaulted to %q, want empty", cfg.Region)
	}

	// One static provider, holding exactly what NewStaticV4 would have.
	if got := providerNames(t, cfg); !reflect.DeepEqual(got, []string{"Static"}) {
		t.Fatalf("provider chain = %v, want just Static", got)
	}
	creds, err := cfg.creds()
	if err != nil {
		t.Fatal(err)
	}
	got, err := creds.Get()
	if err != nil {
		t.Fatal(err)
	}
	want, err := credentials.NewStaticV4("minioadmin", "minioadmin", "").Get()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("default credentials are no longer the NewStaticV4 pair this replaced")
	}

	// And path-style addressing against MinIO, as before.
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if lookup := cfg.bucketLookup(u); lookup != minio.BucketLookupPath {
		t.Fatalf("bucket lookup for MinIO = %v, want path style", lookup)
	}
	opts, err := cfg.minioOptions(u)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Secure || opts.Region != "" || opts.BucketLookup != minio.BucketLookupPath {
		t.Fatalf("minio options changed: %+v", struct {
			Secure bool
			Region string
			Lookup minio.BucketLookupType
		}{opts.Secure, opts.Region, opts.BucketLookup})
	}
}

// TestCredentialSourceSelection is the K3 requirement: static when the
// keys are set, web-identity when the IRSA environment is present, the
// full chain otherwise — decided without contacting anything.
func TestCredentialSourceSelection(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("not.a.real.jwt"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		env      map[string]string
		want     []string
		wantSrc  string
		wantFail bool
	}{
		{
			name:    "default static",
			env:     nil,
			want:    []string{"Static"},
			wantSrc: SourceStatic,
		},
		{
			name:    "auto with static keys",
			env:     map[string]string{EnvS3CredentialsSource: SourceAuto, EnvS3AccessKey: "AKIA", EnvS3SecretKey: "sk"},
			want:    []string{"Static"},
			wantSrc: SourceStatic,
		},
		{
			name: "auto with the IRSA environment",
			env: map[string]string{
				EnvS3CredentialsSource:        SourceAuto,
				"AWS_ROLE_ARN":                "arn:aws:iam::123456789012:role/dabet-insights",
				"AWS_WEB_IDENTITY_TOKEN_FILE": tokenFile,
			},
			want:    []string{"IAM"},
			wantSrc: SourceWebIdentity,
		},
		{
			name:    "auto with neither",
			env:     map[string]string{EnvS3CredentialsSource: SourceAuto},
			want:    []string{"EnvAWS", "EnvMinio", "IAM"},
			wantSrc: SourceChain,
		},
		{
			name:    "chain keeps static keys first",
			env:     map[string]string{EnvS3CredentialsSource: SourceChain, EnvS3AccessKey: "AKIA", EnvS3SecretKey: "sk"},
			want:    []string{"Static", "EnvAWS", "EnvMinio", "IAM"},
			wantSrc: SourceChain,
		},
		{
			name:    "chain with no static keys",
			env:     map[string]string{EnvS3CredentialsSource: SourceChain},
			want:    []string{"EnvAWS", "EnvMinio", "IAM"},
			wantSrc: SourceChain,
		},
		{
			name:    "env only",
			env:     map[string]string{EnvS3CredentialsSource: SourceEnv},
			want:    []string{"EnvAWS", "EnvMinio"},
			wantSrc: SourceEnv,
		},
		{
			name:    "iam only",
			env:     map[string]string{EnvS3CredentialsSource: SourceIAM},
			want:    []string{"IAM"},
			wantSrc: SourceIAM,
		},
		{
			name: "explicit web-identity",
			env: map[string]string{
				EnvS3CredentialsSource:        SourceWebIdentity,
				"AWS_ROLE_ARN":                "arn:aws:iam::123456789012:role/dabet-insights",
				"AWS_WEB_IDENTITY_TOKEN_FILE": tokenFile,
			},
			want:    []string{"IAM"},
			wantSrc: SourceWebIdentity,
		},
		{
			name:    "irsa alias",
			env:     map[string]string{EnvS3CredentialsSource: "irsa"},
			wantSrc: SourceWebIdentity,
			// No projected token: a startup error, not a silent fallback
			// to anonymous requests.
			wantFail: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearS3Env(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := DefaultS3Config()
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.resolvedSource(); got != tc.wantSrc {
				t.Fatalf("resolved source = %q, want %q", got, tc.wantSrc)
			}
			if tc.wantFail {
				if _, err := cfg.providers(); err == nil {
					t.Fatal("expected a startup error")
				}
				return
			}
			if got := providerNames(t, cfg); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("provider chain = %v, want %v", got, tc.want)
			}
			// Building the client must still work and must not need a
			// network: no endpoint of any kind is reachable here.
			if _, err := NewS3StoreFromConfig(cfg); err != nil {
				t.Fatalf("NewS3StoreFromConfig: %v", err)
			}
		})
	}
}

// TestWebIdentityCarriesTheRegionToSTS: without AWS_REGION set, minio-go's
// IAM provider needs the region from us or it falls back to the global STS
// endpoint. S3_REGION is the canonical name, so it must reach the provider.
func TestWebIdentityCarriesTheRegionToSTS(t *testing.T) {
	clearS3Env(t)
	t.Setenv(EnvS3CredentialsSource, SourceIAM)
	t.Setenv(EnvS3Region, "eu-west-1")

	cfg, err := DefaultS3Config()
	if err != nil {
		t.Fatal(err)
	}
	providers, err := cfg.providers()
	if err != nil {
		t.Fatal(err)
	}
	iam, ok := providers[0].(*credentials.IAM)
	if !ok {
		t.Fatalf("provider is a %T", providers[0])
	}
	if iam.Region != "eu-west-1" {
		t.Fatalf("IAM provider region = %q, want eu-west-1", iam.Region)
	}
}

// TestAddressingStyle: path style is right for MinIO, virtual-host style
// for S3, "auto" picks per endpoint, and either can be forced.
func TestAddressingStyle(t *testing.T) {
	cases := []struct {
		style    string
		endpoint string
		want     minio.BucketLookupType
	}{
		{AddressingAuto, "http://localhost:9000", minio.BucketLookupPath},
		{AddressingAuto, "http://minio:9000", minio.BucketLookupPath},
		{AddressingAuto, "http://127.0.0.1:41234", minio.BucketLookupPath},
		{AddressingAuto, "https://s3.amazonaws.com", minio.BucketLookupDNS},
		{AddressingAuto, "https://s3.eu-west-1.amazonaws.com", minio.BucketLookupDNS},
		{AddressingPath, "https://s3.eu-west-1.amazonaws.com", minio.BucketLookupPath},
		{AddressingVirtual, "http://localhost:9000", minio.BucketLookupDNS},
		{"vhost", "http://localhost:9000", minio.BucketLookupDNS},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s %s", tc.style, tc.endpoint), func(t *testing.T) {
			cfg := S3Config{
				Endpoint:          tc.endpoint,
				Bucket:            "embeddings",
				CredentialsSource: SourceStatic,
				AddressingStyle:   tc.style,
			}
			u, err := url.Parse(tc.endpoint)
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.bucketLookup(u); got != tc.want {
				t.Fatalf("bucket lookup = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSecureFollowsTheEndpointScheme keeps the https handling honest.
func TestSecureFollowsTheEndpointScheme(t *testing.T) {
	for endpoint, wantSecure := range map[string]bool{
		"http://localhost:9000":              false,
		"https://s3.eu-west-1.amazonaws.com": true,
	} {
		u, err := url.Parse(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		cfg := S3Config{Endpoint: endpoint, Bucket: "embeddings", CredentialsSource: SourceStatic, AddressingStyle: AddressingAuto}
		opts, err := cfg.minioOptions(u)
		if err != nil {
			t.Fatal(err)
		}
		if opts.Secure != wantSecure {
			t.Errorf("%s: Secure = %v, want %v", endpoint, opts.Secure, wantSecure)
		}
	}
}

// TestS3MisconfigurationFailsAtStartup.
func TestS3MisconfigurationFailsAtStartup(t *testing.T) {
	t.Run("unknown credentials source", func(t *testing.T) {
		clearS3Env(t)
		t.Setenv(EnvS3CredentialsSource, "magic")
		_, err := DefaultS3Config()
		if err == nil {
			t.Fatal("an unknown source was accepted")
		}
		for _, want := range []string{"magic", SourceStatic, SourceWebIdentity} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
	})
	t.Run("unknown addressing style", func(t *testing.T) {
		clearS3Env(t)
		t.Setenv(EnvS3AddressingStyle, "sideways")
		if _, err := DefaultS3Config(); err == nil {
			t.Fatal("an unknown addressing style was accepted")
		}
	})
	t.Run("web-identity without the IRSA environment", func(t *testing.T) {
		clearS3Env(t)
		t.Setenv(EnvS3CredentialsSource, SourceWebIdentity)
		cfg, err := DefaultS3Config()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := NewS3StoreFromConfig(cfg); err == nil {
			t.Fatal("web-identity with no AWS_ROLE_ARN / AWS_WEB_IDENTITY_TOKEN_FILE was accepted")
		}
	})
	t.Run("bad endpoint", func(t *testing.T) {
		if _, err := NewS3StoreFromConfig(S3Config{Endpoint: "://not a url", CredentialsSource: SourceStatic}); err == nil {
			t.Fatal("a malformed endpoint was accepted")
		}
		if _, err := NewS3StoreFromConfig(S3Config{Endpoint: "", CredentialsSource: SourceStatic}); err == nil {
			t.Fatal("an empty endpoint was accepted")
		}
	})
}

// TestSessionTokenReachesTheSigner: a static triple is what an operator
// pastes from `aws sts assume-role`, and dropping the token silently would
// produce 403s nobody could explain.
func TestSessionTokenReachesTheSigner(t *testing.T) {
	clearS3Env(t)
	t.Setenv(EnvS3AccessKey, "ASIA123")
	t.Setenv(EnvS3SecretKey, "secret")
	t.Setenv(EnvS3SessionToken, "session-token")

	cfg, err := DefaultS3Config()
	if err != nil {
		t.Fatal(err)
	}
	creds, err := cfg.creds()
	if err != nil {
		t.Fatal(err)
	}
	v, err := creds.Get()
	if err != nil {
		t.Fatal(err)
	}
	if v.SessionToken != "session-token" || v.AccessKeyID != "ASIA123" {
		t.Fatal("the session token did not reach the credentials value")
	}
}

// TestNonStaticSourcesDropTheMinioadminDefault: leaving "minioadmin" in
// front of the chain would mean IRSA never gets a look in.
func TestNonStaticSourcesDropTheMinioadminDefault(t *testing.T) {
	for _, source := range []string{SourceAuto, SourceChain, SourceEnv, SourceIAM} {
		clearS3Env(t)
		t.Setenv(EnvS3CredentialsSource, source)
		cfg, err := DefaultS3Config()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.AccessKey != "" || cfg.SecretKey != "" {
			t.Errorf("source %q kept the local minioadmin default in front of the chain", source)
		}
	}
}

// TestS3ConfigNeverRendersCredentials is P4.
func TestS3ConfigNeverRendersCredentials(t *testing.T) {
	cfg := S3Config{
		Endpoint:          "https://s3.eu-west-1.amazonaws.com",
		Bucket:            "dabet-embeddings",
		AccessKey:         "ASIAEXAMPLEKEY",
		SecretKey:         "super-secret-key",
		SessionToken:      "a-very-long-session-token",
		CredentialsSource: SourceStatic,
	}
	var b strings.Builder
	for _, a := range cfg.LogValue().Group() {
		b.WriteString(a.Key + "=" + a.Value.String() + " ")
	}
	for _, s := range []string{cfg.String(), b.String()} {
		for _, secret := range []string{"ASIAEXAMPLEKEY", "super-secret-key", "a-very-long-session-token"} {
			if strings.Contains(s, secret) {
				t.Fatalf("rendered form leaked %q: %s", secret, s)
			}
		}
	}
}
