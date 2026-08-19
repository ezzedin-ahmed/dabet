package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dabet/pkg/embeddings"

	"dabet/services/clustering-service/internal/cluster"
)

type captureAssigner struct {
	batches [][]cluster.Record
}

func (c *captureAssigner) AssignBatch(_ context.Context, recs []cluster.Record) {
	c.batches = append(c.batches, recs)
}

func vectorJSON() string {
	return "[1" + strings.Repeat(",0", embeddings.Dimensions-1) + "]"
}

func post(t *testing.T, a Assigner, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	Register(mux, a)
	req := httptest.NewRequest(http.MethodPost, AssignPath, strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestAssignEndpointAcceptsBatch(t *testing.T) {
	a := &captureAssigner{}
	body := `{"records":[
		{"creator_id":"cr-1","content_id":"ct-1","embedded_at":"2026-08-19T13:37:42Z","vector":` + vectorJSON() + `},
		{"creator_id":"cr-2","content_id":"ct-2","embedded_at":"2026-08-19T13:37:43Z","vector":` + vectorJSON() + `}
	]}`
	w := post(t, a, body)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 (body %s)", w.Code, w.Body.String())
	}
	if len(a.batches) != 1 || len(a.batches[0]) != 2 {
		t.Fatalf("expected one batch of 2 records, got %+v", a.batches)
	}
	if a.batches[0][1].CreatorID != "cr-2" || a.batches[0][1].ContentID != "ct-2" {
		t.Fatalf("record fields lost: %+v", a.batches[0][1])
	}
}

func TestAssignEndpointRejectsBadDimensions(t *testing.T) {
	a := &captureAssigner{}
	w := post(t, a, `{"records":[{"creator_id":"cr-1","content_id":"ct-1","embedded_at":"2026-08-19T13:37:42Z","vector":[1,2,3]}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if len(a.batches) != 0 {
		t.Fatal("invalid batch must not reach the assigner")
	}
}

func TestAssignEndpointRejectsUnknownFields(t *testing.T) {
	a := &captureAssigner{}
	w := post(t, a, `{"records":[{"creator_id":"cr-1","text":"never","vector":`+vectorJSON()+`}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}
