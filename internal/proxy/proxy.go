// Package proxy is the shared path's entry point.
//
// At A=on it collects a cohort, seals it, and sends one aggregate envelope. At
// A=off — the level this slice implements — it is a pure pass-through: one
// independent envelope per logical request, no joining, no cohort seal. That
// emptiness is the point. Any collecting here would put formation wait into the
// OFF/OFF baseline and contaminate the aggregation effect (ADR-0001).
//
// The proxy never constructs Triton requests and never chooses compute
// semantics; it cannot, because it does not depend on the packages that could.
package proxy

import (
	"context"
	"fmt"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// Clock reads the proxy's monotonic clock domain.
type Clock func() int64

// Config is what the proxy needs to serve one run.
type Config struct {
	Cell    identity.Cell
	Run     identity.RunID
	TargetB int
}

// Service implements the client-facing proxy service.
type Service struct {
	envelopev1.UnimplementedProxyServer

	cfg     Config
	builder *envelope.Builder
	backend envelopev1.BackendClient
	writer  *events.Writer
	now     Clock
}

// New builds the proxy service.
func New(cfg Config, builder *envelope.Builder, backend envelopev1.BackendClient, writer *events.Writer, now Clock) (*Service, error) {
	switch {
	case builder == nil:
		return nil, fmt.Errorf("proxy: needs an envelope builder")
	case backend == nil:
		return nil, fmt.Errorf("proxy: needs a backend client")
	case writer == nil:
		return nil, fmt.Errorf("proxy: needs an event writer")
	case now == nil:
		return nil, fmt.Errorf("proxy: needs a clock reader")
	}
	if cfg.Cell.AggregatesEnvelopes() {
		return nil, fmt.Errorf("proxy: cell %s is A=on; envelope aggregation arrives in spec 0002", cfg.Cell)
	}
	return &Service{cfg: cfg, builder: builder, backend: backend, writer: writer, now: now}, nil
}

// Submit forwards one logical request and fans its response back.
//
// Nothing is collected, nothing is joined, and no seal is emitted: the load
// generator owns t_cohort_seal at this factor level and records the barrier
// release itself, so the seal never travels the wire (ADR-0001).
func (s *Service) Submit(ctx context.Context, req *envelopev1.ClientRequest) (*envelopev1.ClientResponse, error) {
	recvAt := s.now()

	member := identity.LogicalRequest{
		Cohort:  identity.CohortID(req.GetCohortId()),
		Ordinal: identity.Ordinal(req.GetOrdinal()),
	}
	if got := identity.UID(req.GetUid()); got != member.UID() {
		return nil, fmt.Errorf("proxy: request %v carries uid %d, its identity encodes %d", member, got, member.UID())
	}
	if got := identity.RunID(req.GetRunId()); got != s.cfg.Run {
		return nil, fmt.Errorf("proxy: request is for run %q, proxy is serving %q", got, s.cfg.Run)
	}

	sendAt := s.now()
	env := s.builder.Independent(member, req.GetPayload(), sendAt)

	resp, err := s.backend.Execute(ctx, env)
	respRecvAt := s.now()
	if err != nil {
		s.record(member, req, env, recvAt, sendAt, respRecvAt, s.now(), events.StatusError)
		return nil, fmt.Errorf("proxy: backend rejected envelope %d: %w", env.GetEnvelopeId(), err)
	}

	result, err := memberResult(resp, member)
	if err != nil {
		s.record(member, req, env, recvAt, sendAt, respRecvAt, s.now(), events.StatusError)
		return nil, err
	}

	out := &envelopev1.ClientResponse{
		CohortId:       uint32(member.Cohort),
		Ordinal:        uint32(member.Ordinal),
		Status:         result.GetStatus(),
		Error:          result.GetError(),
		MembershipUids: result.GetMembershipUids(),
		BatchSize:      result.GetBatchSize(),
		DataOutBytes:   result.GetDataOutBytes(),
	}

	fanoutAt := s.now()
	status := events.StatusOK
	if result.GetStatus() != envelopev1.Status_STATUS_OK {
		status = events.StatusError
	}
	s.record(member, req, env, recvAt, sendAt, respRecvAt, fanoutAt, status)
	return out, nil
}

// memberResult finds this member's result, preserving the request→response
// mapping rather than assuming position.
func memberResult(resp *envelopev1.ResponseEnvelope, member identity.LogicalRequest) (*envelopev1.LogicalResult, error) {
	for _, r := range resp.GetResults() {
		if identity.CohortID(r.GetCohortId()) == member.Cohort && identity.Ordinal(r.GetOrdinal()) == member.Ordinal {
			return r, nil
		}
	}
	return nil, fmt.Errorf("proxy: response envelope %d carries no result for %v", resp.GetEnvelopeId(), member)
}

func (s *Service) record(
	member identity.LogicalRequest,
	req *envelopev1.ClientRequest,
	env *envelopev1.RequestEnvelope,
	recvAt, sendAt, respRecvAt, fanoutAt int64,
	status events.Status,
) {
	var rec events.Record
	rec.Emitter = identity.EmitterProxy
	rec.Cohort = member.Cohort
	rec.Ordinal = member.Ordinal
	rec.EnvelopeID = identity.EnvelopeID(env.GetEnvelopeId())
	rec.EnvelopeBytes = uint32(envelope.WireBytes(env))
	rec.LogicalBytes = uint32(len(req.GetPayload()))
	rec.Status = status

	rec.SetStage(events.StageProxyRecv, recvAt)
	rec.SetStage(events.StageProxySend, sendAt)
	rec.SetStage(events.StageProxyRespRecv, respRecvAt)
	rec.SetStage(events.StageProxyFanout, fanoutAt)

	s.writer.Record(&rec)
}
