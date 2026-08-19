// Package grpcapi implements the GetPolicy internal API (docs §6.7) over
// the committed dabet/pkg/policyapi contract. It is the hot-path read
// interface: moderation-service calls it on every local-cache miss, and a
// negative result (found=false) is a first-class, cacheable answer.
package grpcapi

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"dabet/pkg/policyapi"

	"dabet/services/policy-service/internal/policy"
	"dabet/services/policy-service/internal/resolver"
)

// Server implements policyapi.PolicyServiceServer.
type Server struct {
	policyapi.UnimplementedPolicyServiceServer
	res *resolver.Resolver
	log *slog.Logger
}

// New builds the gRPC server over the resolver.
func New(res *resolver.Resolver, log *slog.Logger) *Server {
	return &Server{res: res, log: log}
}

// GetPolicy resolves the effective policy: content → platform → creator →
// none, whole document (docs §6.2). Resolution happens entirely here; the
// caller never sees the candidates and never merges anything.
func (s *Server) GetPolicy(ctx context.Context, req *policyapi.GetPolicyRequest) (*policyapi.GetPolicyResponse, error) {
	if req.GetCreatorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "creator_id is required")
	}
	p, _, err := s.res.Resolve(ctx, req.GetCreatorId(), req.GetPlatform(), req.GetContentId())
	if err != nil {
		// The caller fails open on this (docs §4.7). No policy text in
		// the status message.
		s.log.Error("policy resolve failed", "error", err.Error())
		return nil, status.Error(codes.Unavailable, "policy resolution unavailable")
	}
	if p == nil {
		return &policyapi.GetPolicyResponse{Found: false}, nil
	}
	return &policyapi.GetPolicyResponse{Found: true, Policy: toProto(p)}, nil
}

func toProto(p *policy.Policy) *policyapi.ResolvedPolicy {
	out := &policyapi.ResolvedPolicy{
		PolicyId:                p.ID,
		ResolvedAt:              timestamppb.Now(),
		Spam:                    spamToProto(p.Spam),
		RestrictedWords:         p.RestrictedWords,
		RestrictedContentAction: actionToProto(p.RestrictedContentAction),
	}
	if p.RateLimitMessages != nil {
		v := int32(*p.RateLimitMessages)
		out.RateLimitMessages = &v
	}
	if p.RateLimitSeconds != nil {
		v := int32(*p.RateLimitSeconds)
		out.RateLimitSeconds = &v
	}
	for _, e := range p.RestrictedContent {
		out.RestrictedContent = append(out.RestrictedContent, &policyapi.RestrictedContentEntry{
			Title:       e.Title,
			Description: e.Description,
			Examples:    e.Examples,
		})
	}
	return out
}

func spamToProto(m policy.SpamMode) policyapi.SpamMode {
	switch m {
	case policy.SpamIdentical:
		return policyapi.SpamMode_SPAM_MODE_IDENTICAL
	case policy.SpamSemantic:
		return policyapi.SpamMode_SPAM_MODE_SEMANTIC
	case policy.SpamNone:
		return policyapi.SpamMode_SPAM_MODE_NONE
	default:
		return policyapi.SpamMode_SPAM_MODE_UNSPECIFIED
	}
}

func actionToProto(a policy.RCAction) policyapi.RestrictedContentAction {
	switch a {
	case policy.RCActionReview:
		return policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_REVIEW
	case policy.RCActionAuto:
		return policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO
	default:
		return policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_UNSPECIFIED
	}
}
