// Package httpapi serves the /v1/policies CRUD API (docs §6.6) on the
// shared httpx conventions: error envelope, unknown-field rejection, JWT
// auth, ownership-as-404, and cursor pagination.
//
// P4: handlers log ids and scopes only — never restricted words or
// restricted content text — at info level or above.
package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"dabet/pkg/httpx"

	"dabet/services/policy-service/internal/metrics"
	"dabet/services/policy-service/internal/policy"
	"dabet/services/policy-service/internal/store"
)

// API holds the handler dependencies.
type API struct {
	repo store.Repo
	m    *metrics.Metrics
	log  *slog.Logger
}

// Register mounts the policy routes on mux behind JWT auth.
func Register(mux *http.ServeMux, verifier *httpx.Verifier, repo store.Repo, m *metrics.Metrics, log *slog.Logger) {
	a := &API{repo: repo, m: m, log: log}
	auth := httpx.Auth(verifier)
	mux.Handle("POST /v1/policies", auth(http.HandlerFunc(a.create)))
	mux.Handle("GET /v1/policies", auth(http.HandlerFunc(a.list)))
	mux.Handle("GET /v1/policies/{id}", auth(http.HandlerFunc(a.get)))
	mux.Handle("PUT /v1/policies/{id}", auth(http.HandlerFunc(a.update)))
	mux.Handle("DELETE /v1/policies/{id}", auth(http.HandlerFunc(a.del)))
}

// createRequest is the POST body: the document plus the immutable scope
// pair. Unknown fields are rejected by the decoder.
type createRequest struct {
	Scope   policy.Scope `json:"scope"`
	ScopeID string       `json:"scope_id"`
	policy.Document
}

func writeValidation(w http.ResponseWriter, r *http.Request, verr *policy.ValidationError) {
	httpx.WriteError(w, r, httpx.CodeValidationFailed, verr.Message, verr.Details())
}

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	var req createRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	if verr := policy.ValidateScope(req.Scope, req.ScopeID, creatorID); verr != nil {
		writeValidation(w, r, verr)
		return
	}
	if verr := req.Document.ValidateAndNormalize(); verr != nil {
		writeValidation(w, r, verr)
		return
	}
	now := store.Now()
	p := &policy.Policy{
		ID:        policy.NewID(),
		CreatorID: creatorID,
		Scope:     req.Scope,
		ScopeID:   req.ScopeID,
		Document:  req.Document,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := a.repo.Create(r.Context(), p); err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			httpx.WriteError(w, r, httpx.CodeConflict, "a policy already exists at this scope", map[string]any{
				"scope": string(req.Scope),
			})
			return
		}
		a.internal(w, r, "create policy", err)
		return
	}
	a.m.WritesTotal.WithLabelValues("create", string(p.Scope)).Inc()
	httpx.ContextLogger(r.Context(), a.log).Info("policy created", "policy_id", p.ID, "scope", p.Scope)
	httpx.WriteJSON(w, http.StatusCreated, p)
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	creatorID := httpx.CreatorIDFrom(r.Context())
	limit, err := httpx.ParseLimit(r)
	if err != nil {
		httpx.WriteError(w, r, httpx.CodeValidationFailed, err.Error(), map[string]any{"field": "limit"})
		return
	}
	var f store.ListFilter
	if s := r.URL.Query().Get("scope"); s != "" {
		if !policy.ValidScope(policy.Scope(s)) {
			httpx.WriteError(w, r, httpx.CodeValidationFailed, "scope must be one of creator, platform, content", map[string]any{"field": "scope"})
			return
		}
		f.Scope = policy.Scope(s)
	}
	f.ScopeID = r.URL.Query().Get("scope_id")

	var after *store.Cursor
	if c := r.URL.Query().Get("cursor"); c != "" {
		var cur store.Cursor
		if err := httpx.DecodeCursor(c, &cur); err != nil {
			httpx.WriteError(w, r, httpx.CodeValidationFailed, "invalid cursor", map[string]any{"field": "cursor"})
			return
		}
		after = &cur
	}

	items, err := a.repo.List(r.Context(), creatorID, f, after, limit+1)
	if err != nil {
		a.internal(w, r, "list policies", err)
		return
	}
	resp := struct {
		Items      []*policy.Policy `json:"items"`
		NextCursor string           `json:"next_cursor,omitempty"`
	}{Items: items}
	if len(items) > limit {
		resp.Items = items[:limit]
		last := resp.Items[limit-1]
		cursor, err := httpx.EncodeCursor(store.Cursor{
			CreatedAtUnixNano: last.CreatedAt.UnixNano(),
			ID:                last.ID,
		})
		if err != nil {
			a.internal(w, r, "encode cursor", err)
			return
		}
		resp.NextCursor = cursor
	}
	if resp.Items == nil {
		resp.Items = []*policy.Policy{}
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// owned loads the policy and applies the single authorization rule of
// docs §4.1: absent and owned-by-someone-else are the same 404, so the
// API is not an existence oracle.
func (a *API) owned(w http.ResponseWriter, r *http.Request) *policy.Policy {
	p, err := a.repo.GetByID(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, r, httpx.CodeNotFound, "policy not found", nil)
			return nil
		}
		a.internal(w, r, "load policy", err)
		return nil
	}
	if p.CreatorID != httpx.CreatorIDFrom(r.Context()) {
		httpx.WriteError(w, r, httpx.CodeNotFound, "policy not found", nil)
		return nil
	}
	return p
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	if p := a.owned(w, r); p != nil {
		httpx.WriteJSON(w, http.StatusOK, p)
	}
}

func (a *API) update(w http.ResponseWriter, r *http.Request) {
	p := a.owned(w, r)
	if p == nil {
		return
	}
	// The PUT body is exactly the document: scope and scope_id are not
	// fields of it, so sending them is an unknown-field 400 — which is
	// how immutability is enforced (docs §6.6).
	var doc policy.Document
	if !httpx.Decode(w, r, &doc) {
		return
	}
	if verr := doc.ValidateAndNormalize(); verr != nil {
		writeValidation(w, r, verr)
		return
	}
	// Whole-document replace: the stored document is discarded, never
	// merged (docs §6.2).
	p.Document = doc
	p.UpdatedAt = store.Now()
	if err := a.repo.Update(r.Context(), p); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, r, httpx.CodeNotFound, "policy not found", nil)
			return
		}
		a.internal(w, r, "update policy", err)
		return
	}
	a.m.WritesTotal.WithLabelValues("update", string(p.Scope)).Inc()
	httpx.ContextLogger(r.Context(), a.log).Info("policy updated", "policy_id", p.ID, "scope", p.Scope)
	httpx.WriteJSON(w, http.StatusOK, p)
}

func (a *API) del(w http.ResponseWriter, r *http.Request) {
	p := a.owned(w, r)
	if p == nil {
		return
	}
	if err := a.repo.Delete(r.Context(), p.ID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, r, httpx.CodeNotFound, "policy not found", nil)
			return
		}
		a.internal(w, r, "delete policy", err)
		return
	}
	a.m.WritesTotal.WithLabelValues("delete", string(p.Scope)).Inc()
	httpx.ContextLogger(r.Context(), a.log).Info("policy deleted", "policy_id", p.ID, "scope", p.Scope)
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) internal(w http.ResponseWriter, r *http.Request, op string, err error) {
	httpx.ContextLogger(r.Context(), a.log).Error(op+" failed", "error", err.Error())
	httpx.WriteError(w, r, httpx.CodeInternalError, "internal error", nil)
}
