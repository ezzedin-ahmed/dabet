package job

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"dabet/pkg/embeddings"
)

// writeParquetFixture encodes records exactly the way insights-service
// does (same struct tags, generic writer).
func writeParquetFixture(t *testing.T, recs []EmbeddingRecord) []byte {
	t.Helper()
	var buf bytes.Buffer
	pw := parquet.NewGenericWriter[EmbeddingRecord](&buf)
	if _, err := pw.Write(recs); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func testVector(seed int) []float32 {
	v := make([]float32, embeddings.Dimensions)
	for i := range v {
		v[i] = float32((seed+i)%7) / 7
	}
	return v
}

func TestParquetRoundTrip(t *testing.T) {
	at := time.Date(2026, 8, 19, 14, 0, 0, 0, time.UTC)
	recs := []EmbeddingRecord{
		{CreatorID: "cr-1", ContentID: "ct-1", EmbeddedAt: at, Vector: testVector(1)},
		{CreatorID: "cr-1", ContentID: "ct-2", EmbeddedAt: at.Add(time.Minute), Vector: testVector(2)},
	}
	got, err := ReadEmbeddings(writeParquetFixture(t, recs))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("read %d records, want 2", len(got))
	}
	for i := range recs {
		if got[i].CreatorID != recs[i].CreatorID || got[i].ContentID != recs[i].ContentID {
			t.Errorf("record %d ids = %s/%s, want %s/%s",
				i, got[i].CreatorID, got[i].ContentID, recs[i].CreatorID, recs[i].ContentID)
		}
		if !got[i].EmbeddedAt.Equal(recs[i].EmbeddedAt) {
			t.Errorf("record %d embedded_at = %v, want %v", i, got[i].EmbeddedAt, recs[i].EmbeddedAt)
		}
		if len(got[i].Vector) != embeddings.Dimensions {
			t.Fatalf("record %d vector has %d dims", i, len(got[i].Vector))
		}
		for d, x := range recs[i].Vector {
			if got[i].Vector[d] != x {
				t.Fatalf("record %d vector[%d] = %v, want %v", i, d, got[i].Vector[d], x)
			}
		}
	}
}

// fakeS3 is a minimal path-style S3 server: ListObjectsV2 (with prefix
// and delimiter) and GetObject, enough for the real minio-go client.
type fakeS3 struct {
	bucket  string
	objects map[string][]byte
}

type listContents struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type listPrefix struct {
	Prefix string `xml:"Prefix"`
}

type listBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	KeyCount       int            `xml:"KeyCount"`
	MaxKeys        int            `xml:"MaxKeys"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []listContents `xml:"Contents"`
	CommonPrefixes []listPrefix   `xml:"CommonPrefixes"`
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Query().Has("location") {
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/"+f.bucket) {
		http.NotFound(w, r)
		return
	}
	rest := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/"+f.bucket), "/")
	if rest == "" { // listing
		prefix := r.URL.Query().Get("prefix")
		delimiter := r.URL.Query().Get("delimiter")
		res := listBucketResult{
			Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:  f.bucket, Prefix: prefix, MaxKeys: 1000, Delimiter: delimiter,
		}
		keys := make([]string, 0, len(f.objects))
		for k := range f.objects {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		seenPrefix := map[string]bool{}
		for _, k := range keys {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			if delimiter != "" {
				if idx := strings.Index(k[len(prefix):], delimiter); idx >= 0 {
					p := k[:len(prefix)+idx+1]
					if !seenPrefix[p] {
						seenPrefix[p] = true
						res.CommonPrefixes = append(res.CommonPrefixes, listPrefix{Prefix: p})
					}
					continue
				}
			}
			res.Contents = append(res.Contents, listContents{
				Key:          k,
				LastModified: "2026-08-19T15:04:05.000Z",
				ETag:         `"0"`,
				Size:         len(f.objects[k]),
				StorageClass: "STANDARD",
			})
		}
		res.KeyCount = len(res.Contents) + len(res.CommonPrefixes)
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, xml.Header)
		_ = xml.NewEncoder(w).Encode(res)
		return
	}
	data, ok := f.objects[rest]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("ETag", `"0"`)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Last-Modified", time.Date(2026, 8, 19, 15, 4, 5, 0, time.UTC).Format(http.TimeFormat))
	_, _ = w.Write(data)
}

func TestS3StoreListAndGet(t *testing.T) {
	at := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	obj1 := writeParquetFixture(t, []EmbeddingRecord{
		{CreatorID: "cr-1", ContentID: "ct-1", EmbeddedAt: at, Vector: testVector(1)},
	})
	obj2 := writeParquetFixture(t, []EmbeddingRecord{
		{CreatorID: "cr-1", ContentID: "ct-2", EmbeddedAt: at.AddDate(0, 0, 1), Vector: testVector(2)},
	})
	fake := &fakeS3{bucket: "embeddings", objects: map[string][]byte{
		"creator_id=cr-1/date=2026-08-19/embeddings-1-1.parquet": obj1,
		"creator_id=cr-1/date=2026-08-20/embeddings-2-1.parquet": obj2,
		"creator_id=cr-2/date=2026-08-19/embeddings-3-1.parquet": obj1,
	}}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	store, err := NewS3Store(srv.URL, "minioadmin", "minioadmin", "embeddings")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	creators, err := store.ListCreators(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(creators) != 2 || creators[0] != "cr-1" || creators[1] != "cr-2" {
		t.Fatalf("ListCreators = %v, want [cr-1 cr-2]", creators)
	}

	// Window spanning both dates finds both objects, in date order.
	objs, err := store.ListCreatorObjects(ctx, "cr-1",
		time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 20, 23, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 {
		t.Fatalf("listed %d objects, want 2: %+v", len(objs), objs)
	}
	if !strings.Contains(objs[0].Key, "date=2026-08-19") || !strings.Contains(objs[1].Key, "date=2026-08-20") {
		t.Errorf("unexpected keys/order: %+v", objs)
	}

	// A one-day window sees only that partition.
	objs, err = store.ListCreatorObjects(ctx, "cr-1",
		time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 20, 23, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 1 || !strings.Contains(objs[0].Key, "date=2026-08-20") {
		t.Fatalf("one-day window listed %+v", objs)
	}

	// Get + decode round-trips the fixture through the S3 path.
	data, err := store.Get(ctx, "creator_id=cr-1/date=2026-08-19/embeddings-1-1.parquet")
	if err != nil {
		t.Fatal(err)
	}
	recs, err := ReadEmbeddings(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].ContentID != "ct-1" {
		t.Fatalf("decoded %+v", recs)
	}
}
