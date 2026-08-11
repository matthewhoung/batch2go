// Package proxy is the shared path's entry point, and the place Factor A is
// realized.
//
// At A=on it holds a cohort while it assembles, seals it with its own clock, and
// sends one aggregate envelope — cohort formation lives in cohort.go. At A=off
// it is a pure pass-through: one independent envelope per logical request, no
// joining, no seal. That emptiness is the point. Any collecting there would put
// formation wait into the OFF/OFF baseline and contaminate the aggregation
// effect (ADR-0001, ADR-0010).
//
// The proxy never constructs Triton requests and never chooses compute
// semantics; it cannot, because it does not depend on the packages that could.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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

	// FormationDeadline bounds how long a partly assembled cohort is held before
	// it is failed whole. It comes from the manifest and is never inferred: it is
	// an experimental quantity, it is treatment-correlated in its consequences —
	// a cohort that cannot form costs one request at A=off and B at A=on — and a
	// code default would be a number nobody declared (ADR-0010).
	FormationDeadline time.Duration
}

// Validate rejects a configuration the proxy could not serve honestly.
func (c Config) Validate() error {
	switch {
	case c.Run == "":
		return fmt.Errorf("proxy: config needs a run id")
	case c.Cell == "":
		return fmt.Errorf("proxy: config needs a cell")
	case c.TargetB <= 0:
		return fmt.Errorf("proxy: config needs a positive target B, got %d", c.TargetB)
	}
	// Formation exists exactly where the proxy aggregates. Both directions are
	// refused, because a deadline at A=off would bound a wait that does not
	// happen and its presence in the record would suggest one did.
	switch {
	case c.Cell.AggregatesEnvelopes() && c.FormationDeadline <= 0:
		return fmt.Errorf("proxy: cell %s aggregates envelopes and needs a formation deadline", c.Cell)
	case !c.Cell.AggregatesEnvelopes() && c.FormationDeadline != 0:
		return fmt.Errorf("proxy: cell %s forms no cohort and must not carry a formation deadline, got %s", c.Cell, c.FormationDeadline)
	}
	return nil
}

// Service implements the client-facing proxy service.
type Service struct {
	envelopev1.UnimplementedProxyServer

	cfg     Config
	builder *envelope.Builder
	backend envelopev1.BackendClient
	writer  *events.Writer
	now     Clock

	// mu guards the registry below and every formation's admission state. One
	// mutex rather than one per cohort: the critical section is a slot write and
	// a counter, the identification regime releases one cohort at a time, and the
	// seal is taken inside it — so what it costs is bounded and what it protects
	// is one rule.
	mu sync.Mutex

	// forming holds the cohorts currently being assembled, plus the ones that
	// failed to form and are now that cohort id's verdict. It stays empty at
	// A=off, where nothing is assembled.
	forming map[identity.CohortID]*formation
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Service{
		cfg:     cfg,
		builder: builder,
		backend: backend,
		writer:  writer,
		now:     now,
		forming: make(map[identity.CohortID]*formation),
	}, nil
}

// Submit takes one logical request and returns that request's own result.
//
// The client protocol is the same at both factor levels: the load generator
// sends one logical request whatever the cell, and whether requests share an
// envelope is the proxy's decision. Which of the two paths below runs is read
// from the cell, never from the shape of what arrived.
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

	if s.cfg.Cell.AggregatesEnvelopes() {
		return s.aggregate(ctx, req, member, recvAt)
	}
	return s.forward(ctx, req, member, recvAt)
}

// forward is the A=off path: one logical request out in its own envelope, and
// its response back.
//
// Nothing is collected, nothing is joined, and no seal is emitted: the load
// generator owns t_cohort_seal at this factor level and records the barrier
// release itself, so the seal never travels the wire (ADR-0001).
func (s *Service) forward(
	ctx context.Context,
	req *envelopev1.ClientRequest,
	member identity.LogicalRequest,
	recvAt int64,
) (*envelopev1.ClientResponse, error) {
	sendAt := s.now()
	env := s.builder.Independent(member, req.GetPayload(), sendAt)

	resp, err := s.backend.Execute(ctx, env)
	respRecvAt := s.now()

	// The envelope is measured here, after the response, so that the measurement
	// falls in the fan-out interval rather than in the transfer it would
	// otherwise inflate. The same holds at A=on, where the envelope is B times
	// the size and the misattribution would be B times as large.
	record := func(fanoutAt int64, status events.Status) {
		var rec events.Record
		rec.EnvelopeID = identity.EnvelopeID(env.GetEnvelopeId())
		rec.EnvelopeBytes = uint32(envelope.WireBytes(env))
		rec.SetStage(events.StageProxyRecv, recvAt)
		rec.SetStage(events.StageProxySend, sendAt)
		rec.SetStage(events.StageProxyRespRecv, respRecvAt)
		rec.SetStage(events.StageProxyFanout, fanoutAt)
		s.write(&rec, member, req, status)
	}

	if err != nil {
		record(s.now(), events.StatusError)
		return nil, fmt.Errorf("proxy: backend rejected envelope %d: %w", env.GetEnvelopeId(), err)
	}
	result, err := memberResult(resp, member)
	if err != nil {
		record(s.now(), events.StatusError)
		return nil, err
	}

	out := clientResponse(member, result)
	record(s.now(), resultStatus(result))
	return out, nil
}

// aggregate is the A=on path: the request joins its cohort, waits for the cohort
// to be sealed and answered, and takes its own member's result out of the one
// response the whole cohort shares.
func (s *Service) aggregate(
	ctx context.Context,
	req *envelopev1.ClientRequest,
	member identity.LogicalRequest,
	recvAt int64,
) (*envelopev1.ClientResponse, error) {
	f, complete, err := s.join(member, req.GetPayload())
	if err != nil {
		// The member arrived and nothing else happened to it. Its record says that
		// and no more: a request the proxy refused must stay in the evidence,
		// because a member missing from the record is indistinguishable from a
		// member that was never released.
		s.recordArrival(member, req, recvAt, events.StatusError)
		return nil, fmt.Errorf("proxy: %v: %w", member, err)
	}
	if complete {
		s.sealAndSend(ctx, f)
	}
	if err := await(ctx, f); err != nil {
		s.recordArrival(member, req, recvAt, departureStatus(err))
		return nil, fmt.Errorf("proxy: %v left its cohort before it was answered: %w", member, err)
	}

	if f.terminal != nil {
		// The same cause the member that exposed it received, worded once at cohort
		// level and reported here against this member's own identity (ADR-0010).
		s.recordArrival(member, req, recvAt, events.StatusError)
		return nil, fmt.Errorf("proxy: %v: %w", member, f.terminal)
	}

	record := func(status events.Status) {
		var rec events.Record
		rec.EnvelopeID = identity.EnvelopeID(f.env.GetEnvelopeId())
		rec.EnvelopeBytes = f.envelopeBytes
		// The member's own arrival and fan-out; the cohort's seal, send and
		// response. The interval from the first pair's arrival to the seal is
		// W_form, which exists here and structurally cannot at A=off (ADR-0010).
		rec.SetStage(events.StageProxyRecv, recvAt)
		rec.SetStage(events.StageCohortSeal, f.sealAt)
		rec.SetStage(events.StageProxySend, f.sendAt)
		rec.SetStage(events.StageProxyRespRecv, f.respRecvAt)
		rec.SetStage(events.StageProxyFanout, s.now())
		s.write(&rec, member, req, status)
	}

	if f.sendErr != nil {
		record(events.StatusError)
		return nil, f.sendErr
	}
	result, err := memberResult(f.resp, member)
	if err != nil {
		record(events.StatusError)
		return nil, err
	}

	out := clientResponse(member, result)
	record(resultStatus(result))
	return out, nil
}

// memberResult finds this member's result, preserving the request→response
// mapping rather than assuming position.
//
// At A=off the two are the same thing, because there is one of each. At A=on a
// response reordered anywhere between the executor and here would hand every
// caller its neighbour's answer while every count, every cardinality and every
// membership set still agreed.
func memberResult(resp *envelopev1.ResponseEnvelope, member identity.LogicalRequest) (*envelopev1.LogicalResult, error) {
	for _, r := range resp.GetResults() {
		if identity.CohortID(r.GetCohortId()) == member.Cohort && identity.Ordinal(r.GetOrdinal()) == member.Ordinal {
			return r, nil
		}
	}
	return nil, fmt.Errorf("proxy: response envelope %d carries no result for %v", resp.GetEnvelopeId(), member)
}

func clientResponse(member identity.LogicalRequest, result *envelopev1.LogicalResult) *envelopev1.ClientResponse {
	return &envelopev1.ClientResponse{
		CohortId:       uint32(member.Cohort),
		Ordinal:        uint32(member.Ordinal),
		Status:         result.GetStatus(),
		Error:          result.GetError(),
		MembershipUids: result.GetMembershipUids(),
		BatchSize:      result.GetBatchSize(),
		DataOutBytes:   result.GetDataOutBytes(),
	}
}

func resultStatus(result *envelopev1.LogicalResult) events.Status {
	if result.GetStatus() != envelopev1.Status_STATUS_OK {
		return events.StatusError
	}
	return events.StatusOK
}

// departureStatus words the outcome of a caller that left before its cohort was
// answered. This member did itself time out, at the proxy, while it was held —
// which is a different fact from the one ADR-0010 records for a cohort the proxy
// fails at its formation deadline, where the held members log an error because
// they did not themselves time out and only the absentee's timeout is a timeout.
// The two stay distinguishable: that path arrives with the deadline and reports
// the cohort's own cause, not this one.
func departureStatus(err error) events.Status {
	if errors.Is(err, context.DeadlineExceeded) {
		return events.StatusTimeout
	}
	return events.StatusError
}

// recordArrival writes the record of a member the proxy received and could not
// answer: it arrived, and nothing after that happened to it.
func (s *Service) recordArrival(
	member identity.LogicalRequest,
	req *envelopev1.ClientRequest,
	recvAt int64,
	status events.Status,
) {
	var rec events.Record
	rec.SetStage(events.StageProxyRecv, recvAt)
	s.write(&rec, member, req, status)
}

// write completes a proxy record with the identity and byte accounting every one
// of them carries, and hands it to the stream.
//
// The caller sets the stages, because which of them exist is the difference
// between the two factor levels and between the outcomes within each: a member
// whose cohort never sealed has no send to report, and SetStage is what keeps a
// timestamp and its presence from drifting apart (ADR-0005).
func (s *Service) write(
	rec *events.Record,
	member identity.LogicalRequest,
	req *envelopev1.ClientRequest,
	status events.Status,
) {
	rec.Emitter = identity.EmitterProxy
	rec.Cohort = member.Cohort
	rec.Ordinal = member.Ordinal
	rec.LogicalBytes = uint32(len(req.GetPayload()))
	rec.Status = status
	s.writer.Record(rec)
}
