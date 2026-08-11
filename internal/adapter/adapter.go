// Package adapter terminates the envelope protocol and hands work to an
// executor.
//
// Its defining negative property: at A=off it never joins and never
// synchronizes. Envelopes are dispatched on arrival. An adapter-side cohort
// barrier would inject formation wait into F00 — the OFF/OFF baseline — and
// contaminate the very effect the design is measuring, so the absence of waiting
// here is asserted from records rather than assumed (ADR-0001).
package adapter

import (
	"context"
	"fmt"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/executor"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// Config is what the adapter needs to serve one run.
type Config struct {
	Expectation envelope.Expectation

	// Model is the Triton entry this cell targets.
	Model string
}

// Service implements the backend service.
type Service struct {
	envelopev1.UnimplementedBackendServer

	cfg    Config
	exec   executor.Executor
	writer *events.Writer
	now    executor.Clock
}

// New builds the adapter service.
func New(cfg Config, exec executor.Executor, writer *events.Writer, now executor.Clock) (*Service, error) {
	switch {
	case exec == nil:
		return nil, fmt.Errorf("adapter: needs an executor")
	case writer == nil:
		return nil, fmt.Errorf("adapter: needs an event writer")
	case now == nil:
		return nil, fmt.Errorf("adapter: needs a clock reader")
	case cfg.Model == "":
		return nil, fmt.Errorf("adapter: needs a model entry")
	}
	return &Service{cfg: cfg, exec: exec, writer: writer, now: now}, nil
}

// Execute terminates one envelope and returns its results.
//
// There is no queue, no batching buffer, and nothing to wait on between arrival
// and dispatch. The gap between t_adapter_recv and t_adapter_dispatch is
// therefore unpack cost only, and a contract test reads it back from the records
// to establish that.
func (s *Service) Execute(ctx context.Context, env *envelopev1.RequestEnvelope) (*envelopev1.ResponseEnvelope, error) {
	recvAt := s.now()

	if err := envelope.Validate(env, s.cfg.Expectation); err != nil {
		return nil, err
	}
	members := envelope.Members(env)

	result, evidence, err := s.exec.Execute(ctx, executor.Dispatch{Model: s.cfg.Model, Members: members})
	if err != nil {
		return nil, err
	}

	sendAt := s.now()
	resp := &envelopev1.ResponseEnvelope{
		SchemaVersion: envelope.SchemaVersion,
		RunId:         env.GetRunId(),
		CohortId:      env.GetCohortId(),
		EnvelopeId:    env.GetEnvelopeId(),
		TAdapterRecv:  recvAt,
		TAdapterSend:  sendAt,
		Evidence: &envelopev1.AdapterEvidence{
			Dispatched:        evidence.Dispatched,
			DispatchSkewNanos: evidence.SkewNanos,
			CpuNanos:          evidence.CPUNanos,
			CpuScope:          evidence.CPUScope.String(),
		},
		Results: make([]*envelopev1.LogicalResult, 0, len(result.Members)),
	}

	envelopeBytes := uint32(envelope.WireBytes(env))
	payloads := payloadBytesByMember(env)
	for _, m := range result.Members {
		// A result for a member the envelope never carried has no payload size to
		// attribute, and recording zero would archive a measured-looking zero for a
		// quantity Validate guarantees is non-empty. It is a protocol violation by
		// the executor, so it fails the envelope rather than the member.
		logicalBytes, ok := payloads[m.Member]
		if !ok {
			return nil, fmt.Errorf("adapter: executor returned a result for %v, which envelope %d does not carry",
				m.Member, env.GetEnvelopeId())
		}
		resp.Results = append(resp.Results, logicalResult(m))
		s.record(env, m, evidence, recvAt, sendAt, envelopeBytes, logicalBytes)
	}
	return resp, nil
}

// record writes the adapter's four timestamps for one member, plus the
// membership the execution attested and what the adapter observed about the
// fan-out it was released in. The adapter is where both arrive, so it is where
// they are recorded — until now the dispatch evidence reached the response
// envelope and went no further, so it did not survive the run.
//
// Every member of one dispatch is given the same evidence, because it describes
// the dispatch rather than the member; the envelope id on each record is what
// lets the validator group them back.
func (s *Service) record(
	env *envelopev1.RequestEnvelope,
	m executor.MemberResult,
	evidence executor.Evidence,
	recvAt, sendAt int64,
	envelopeBytes, logicalBytes uint32,
) {
	var rec events.Record
	rec.Emitter = identity.EmitterAdapter
	rec.Cohort = m.Member.Cohort
	rec.Ordinal = m.Member.Ordinal
	rec.EnvelopeID = identity.EnvelopeID(env.GetEnvelopeId())
	rec.EnvelopeBytes = envelopeBytes
	rec.LogicalBytes = logicalBytes
	rec.SetDispatch(evidence)

	rec.SetStage(events.StageAdapterRecv, recvAt)
	rec.SetStage(events.StageAdapterDispatch, m.DispatchedAt)
	rec.SetStage(events.StageAdapterResult, m.ResultAt)
	rec.SetStage(events.StageAdapterSend, sendAt)

	rec.Status = events.StatusOK
	if m.Err != nil {
		rec.Status = events.StatusError
	} else {
		rec.SetMembership(m.Membership)
		rec.BatchSize = uint32(m.BatchSize)
	}
	s.writer.Record(&rec)
}

func logicalResult(m executor.MemberResult) *envelopev1.LogicalResult {
	out := &envelopev1.LogicalResult{
		CohortId:     uint32(m.Member.Cohort),
		Ordinal:      uint32(m.Member.Ordinal),
		Status:       envelopev1.Status_STATUS_OK,
		BatchSize:    uint32(m.BatchSize),
		DataOutBytes: uint64(m.DataOutBytes),
	}
	if m.Err != nil {
		// A failed member is reported as failed; it never disappears from the
		// response, because a member missing from the record is indistinguishable
		// from a member that was never released.
		out.Status = envelopev1.Status_STATUS_ERROR
		out.Error = m.Err.Error()
		return out
	}
	for _, uid := range m.Membership {
		out.MembershipUids = append(out.MembershipUids, int64(uid))
	}
	return out
}

// payloadBytesByMember keys the envelope's payload sizes by identity.
//
// Indexing by position would couple the executor's result order to the
// envelope's member order across a package boundary: at A=off there is one
// member and the coupling is invisible, and at A=on a reordered result would
// attribute one member's payload size to another. Identity is what links them.
func payloadBytesByMember(env *envelopev1.RequestEnvelope) map[identity.LogicalRequest]uint32 {
	out := make(map[identity.LogicalRequest]uint32, len(env.GetRequests()))
	for _, r := range env.GetRequests() {
		member := identity.LogicalRequest{
			Cohort:  identity.CohortID(r.GetCohortId()),
			Ordinal: identity.Ordinal(r.GetOrdinal()),
		}
		out[member] = uint32(len(r.GetPayload()))
	}
	return out
}
