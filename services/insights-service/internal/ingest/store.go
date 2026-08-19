package ingest

import (
	"bytes"
	"context"
	"fmt"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Store implements ObjectStore on MinIO/S3 with path-style addressing, so
// the same code runs against local MinIO (S3_ENDPOINT, §3) and real S3.
type S3Store struct {
	cl     *minio.Client
	bucket string
}

// NewS3Store connects to the object store at endpoint (a URL such as
// http://localhost:9000), writing into bucket.
func NewS3Store(endpoint, accessKey, secretKey, bucket string) (*S3Store, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3 endpoint: %w", err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("s3 endpoint %q: missing host", endpoint)
	}
	cl, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       u.Scheme == "https",
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 client: %w", err)
	}
	return &S3Store{cl: cl, bucket: bucket}, nil
}

// Put uploads data at key.
func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.cl.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	return err
}
