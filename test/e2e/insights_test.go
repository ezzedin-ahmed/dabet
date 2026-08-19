//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/parquet-go/parquet-go"
)

// (h) insights-service embedded the messages nobody flagged and rolled
// them to object storage as parquet, partitioned creator_id / date, with
// neither text nor author_id in the file (§8.4, §4.8).
func stepInsights(t *testing.T, s *scenario) {
	ctx := testContext(t)
	mc, err := minio.New(s3Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(s3AccessKey, s3SecretKey, ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatalf("build minio client: %v", err)
	}

	prefix := "creator_id=" + s.creatorID + "/"
	var objects []string
	poll(t, insightsTimeout, "a parquet object under "+prefix, func() error {
		objects = objects[:0]
		for obj := range mc.ListObjects(ctx, s3Bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			if obj.Err != nil {
				return obj.Err
			}
			if strings.HasSuffix(obj.Key, ".parquet") {
				objects = append(objects, obj.Key)
			}
		}
		if len(objects) == 0 {
			return fmt.Errorf("no parquet objects yet")
		}
		return nil
	})
	sort.Strings(objects)

	// §8.4: partitioned creator_id / date.
	for _, key := range objects {
		rest := strings.TrimPrefix(key, prefix)
		datePart, file, ok := strings.Cut(rest, "/")
		if !ok || !strings.HasPrefix(datePart, "date=") || file == "" {
			t.Fatalf("object key %q is not creator_id=…/date=…/<file>.parquet", key)
		}
		if len(strings.TrimPrefix(datePart, "date=")) != len("2006-01-02") {
			t.Fatalf("object key %q does not carry a YYYY-MM-DD date partition", key)
		}
	}

	body := downloadObject(t, mc, objects[0])
	if !bytes.HasPrefix(body, []byte("PAR1")) {
		t.Fatalf("object %s is not a parquet file (no PAR1 magic)", objects[0])
	}

	f, err := parquet.OpenFile(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("open %s as parquet: %v", objects[0], err)
	}
	if f.NumRows() == 0 {
		t.Fatalf("%s has no rows", objects[0])
	}

	var columns []string
	for _, field := range f.Schema().Fields() {
		columns = append(columns, field.Name())
	}
	sort.Strings(columns)
	want := []string{"content_id", "creator_id", "embedded_at", "vector"}
	if strings.Join(columns, ",") != strings.Join(want, ",") {
		t.Fatalf("parquet columns = %v, want exactly %v (no author_id, no text — §4.8)", columns, want)
	}

	// Belt and braces: nothing recognisable from the chat may appear
	// anywhere in the bytes, not just in a declared column.
	for _, secret := range []string{cleanText, dupText, flagmeText, restrictedWord,
		"viewer-clean", "viewer-dup", "viewer-llm", "viewer-rate"} {
		if bytes.Contains(body, []byte(secret)) {
			t.Fatalf("parquet object %s contains %q; embeddings carry no text and no author_id",
				objects[0], secret)
		}
	}
	// The creator and its content are expected — they are the partition
	// key and a stored column.
	if !bytes.Contains(body, []byte(s.creatorID)) {
		t.Fatalf("parquet object %s does not carry the creator_id it is partitioned under", objects[0])
	}
}

func downloadObject(t *testing.T, mc *minio.Client, key string) []byte {
	t.Helper()
	obj, err := mc.GetObject(testContext(t), s3Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer obj.Close()
	body, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return body
}
