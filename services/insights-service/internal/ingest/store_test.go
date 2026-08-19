package ingest

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// decodeAWSChunked strips aws-chunked framing ("<hex-size>;chunk-signature=…
// \r\n<data>\r\n…") that minio-go's streaming signature-v4 wraps around the
// payload on plain HTTP. Bodies without the framing pass through unchanged.
func decodeAWSChunked(t *testing.T, body []byte) []byte {
	t.Helper()
	if !bytes.Contains(body, []byte(";chunk-signature=")) {
		return body
	}
	var out []byte
	rest := body
	for {
		nl := bytes.Index(rest, []byte("\r\n"))
		if nl < 0 {
			t.Fatalf("malformed aws-chunked body: no header line in %q", rest)
		}
		header := rest[:nl]
		if semi := bytes.IndexByte(header, ';'); semi >= 0 {
			header = header[:semi]
		}
		size, err := strconv.ParseInt(string(header), 16, 64)
		if err != nil {
			t.Fatalf("malformed aws-chunked size %q: %v", header, err)
		}
		rest = rest[nl+2:]
		if size == 0 {
			return out
		}
		out = append(out, rest[:size]...)
		rest = rest[size+2:] // skip data and trailing \r\n
	}
}

// TestS3StorePutPathStyle runs the real minio-go client against an httptest
// fake and asserts the path-style PUT: /<bucket>/<key> with the exact body.
func TestS3StorePutPathStyle(t *testing.T) {
	type capture struct {
		method, path string
		body         []byte
	}
	var got []capture
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = append(got, capture{method: r.Method, path: r.URL.Path, body: body})
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			// Bucket-location probe: answer with a well-formed document.
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
			return
		}
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store, err := NewS3Store(srv.URL, "minioadmin", "minioadmin", "embeddings")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("parquet-bytes")
	key := "creator_id=cr-1/date=2026-08-19/embeddings-1-1.parquet"
	if err := store.Put(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}

	var put *capture
	for i := range got {
		if got[i].method == http.MethodPut {
			put = &got[i]
		}
	}
	if put == nil {
		t.Fatalf("no PUT request seen, requests: %+v", got)
	}
	if want := "/embeddings/" + key; put.path != want {
		t.Fatalf("PUT path = %q, want path-style %q", put.path, want)
	}
	if payload := decodeAWSChunked(t, put.body); !bytes.Equal(payload, data) {
		t.Fatalf("PUT payload = %q, want %q", payload, data)
	}
}

func TestNewS3StoreRejectsBadEndpoint(t *testing.T) {
	if _, err := NewS3Store("://not a url", "a", "b", "embeddings"); err == nil {
		t.Fatal("expected error for malformed endpoint")
	}
	if _, err := NewS3Store("", "a", "b", "embeddings"); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}
