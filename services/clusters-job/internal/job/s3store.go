package job

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectInfo describes one embeddings object in S3.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// ObjectStore is the object-storage surface the pipeline reads from. The
// production implementation is S3Store; tests use fakes.
type ObjectStore interface {
	// ListCreatorObjects lists the creator's parquet objects whose date
	// partition falls within [from, to] (dates inclusive at both ends —
	// record-level filtering on embedded_at happens in the pipeline).
	ListCreatorObjects(ctx context.Context, creatorID string, from, to time.Time) ([]ObjectInfo, error)
	// Get downloads one object.
	Get(ctx context.Context, key string) ([]byte, error)
}

// S3Store reads insights-service's embeddings bucket over minio-go with
// path-style addressing, so the same code runs against local MinIO
// (S3_ENDPOINT, §3) and real S3.
type S3Store struct {
	cl     *minio.Client
	bucket string
}

// NewS3Store connects to the object store at endpoint (a URL such as
// http://localhost:9000), reading from bucket.
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

// ListCreators enumerates the creator ids present in the bucket from the
// Hive partition prefixes (creator_id=<id>/). Used by the trigger sweep.
func (s *S3Store) ListCreators(ctx context.Context) ([]string, error) {
	var out []string
	for obj := range s.cl.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    "creator_id=",
		Recursive: false,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("s3 list creators: %w", obj.Err)
		}
		key := strings.TrimSuffix(obj.Key, "/")
		id := strings.TrimPrefix(key, "creator_id=")
		if id != "" && id != key {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out, nil
}

// ListCreatorObjects implements ObjectStore: one prefix listing per date
// partition in the window, keys returned in listing (lexicographic — i.e.
// chronological) order.
func (s *S3Store) ListCreatorObjects(ctx context.Context, creatorID string, from, to time.Time) ([]ObjectInfo, error) {
	var out []ObjectInfo
	for day := from.UTC().Truncate(24 * time.Hour); !day.After(to.UTC()); day = day.AddDate(0, 0, 1) {
		prefix := fmt.Sprintf("creator_id=%s/date=%s/", creatorID, day.Format("2006-01-02"))
		for obj := range s.cl.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			if obj.Err != nil {
				return nil, fmt.Errorf("s3 list %s: %w", prefix, obj.Err)
			}
			out = append(out, ObjectInfo{Key: obj.Key, Size: obj.Size, LastModified: obj.LastModified})
		}
	}
	return out, nil
}

// Get implements ObjectStore.
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.cl.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("s3 read %s: %w", key, err)
	}
	return data, nil
}
