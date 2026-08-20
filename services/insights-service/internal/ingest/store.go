package ingest

import (
	"bytes"
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
)

// S3Store implements ObjectStore on MinIO/S3, so the same code runs
// against local MinIO (S3_ENDPOINT, §3) and real S3. Addressing style and
// credentials come from S3Config: path style and a static key pair
// locally, virtual-host style and an assumed role on AWS.
type S3Store struct {
	cl     *minio.Client
	bucket string
}

// NewS3Store connects to the object store at endpoint (a URL such as
// http://localhost:9000), writing into bucket, with a static key pair.
// Kept for callers and tests that hold their own credentials; production
// goes through NewS3StoreFromConfig.
func NewS3Store(endpoint, accessKey, secretKey, bucket string) (*S3Store, error) {
	return NewS3StoreFromConfig(S3Config{
		Endpoint:          endpoint,
		Bucket:            bucket,
		AccessKey:         accessKey,
		SecretKey:         secretKey,
		CredentialsSource: SourceStatic,
		AddressingStyle:   AddressingPath,
	})
}

// NewS3StoreFromConfig connects using the full configuration, including
// the credential chain that lets a pod use IRSA instead of a static IAM
// user key. Nothing here contacts the endpoint or the credential source: a
// misconfiguration fails now, and credentials are fetched lazily on the
// first signed request.
func NewS3StoreFromConfig(cfg S3Config) (*S3Store, error) {
	u, err := parseS3Endpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	opts, err := cfg.minioOptions(u)
	if err != nil {
		return nil, fmt.Errorf("s3 credentials: %w", err)
	}
	cl, err := minio.New(u.Host, opts)
	if err != nil {
		return nil, fmt.Errorf("s3 client: %w", err)
	}
	return &S3Store{cl: cl, bucket: cfg.Bucket}, nil
}

// Put uploads data at key.
func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.cl.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	return err
}
