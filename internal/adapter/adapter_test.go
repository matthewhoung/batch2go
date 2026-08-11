package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/executor"
	"github.com/matthewhoung/batch2go/internal/identity"
)

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

// fakeExecutor returns results in a controllable order, so that the adapter's
// mapping can be checked against something other than the order it sent.
type fakeExecutor struct {
	reverse bool
	failFor *identity.LogicalRequest
	seen    []executor.Dispatch

	// skew overrides the evidence's dispatch skew. Zero is a legitimate value —
	// it is what one member measures — so it is set explicitly rather than left to
	// the zero value of the struct.
	skew *int64
}

func (f *fakeExecutor) Execute(_ context.Context, d executor.Dispatch) (executor.Result, executor.Evidence, error) {
	f.seen = append(f.seen, d)

	members := append([]identity.LogicalRequest(nil), d.Members...)
	if f.reverse {
		for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
			members[i], members[j] = members[j], members[i]
		}
	}

	var out executor.Result
	for i, m := range members {
		r := executor.MemberResult{
			Member:       m,
			Membership:   []identity.UID{m.UID()},
			BatchSize:    1,
			DataOutBytes: 256,
			DispatchedAt: int64(1000 + i),
			ResultAt:     int64(2000 + i),
		}
		if f.failFor != nil && m == *f.failFor {
			r.Err = fmt.Errorf("backend refused %v", m)
			r.Membership = nil
			r.BatchSize = 0
		}
		out.Members = append(out.Members, r)
	}
	evidence := executor.Evidence{
		Dispatched: uint32(len(members)),
		SkewNanos:  42,
		CPUNanos:   99,
		CPUScope:   events.CPUScopeProcess,
	}
	if f.skew != nil {
		evidence.SkewNanos = *f.skew
	}
	return out, evidence, nil
}

type harness struct {
	service *Service
	stream  *bytes.Buffer
	writer  *events.Writer
	exec    *fakeExecutor
	builder *envelope.Builder
	cell    identity.Cell
	targetB int
}

func newHarness(t *testing.T, cell identity.Cell, targetB int, exec *fakeExecutor) *harness {
	t.Helper()

	cfg := envelope.Config{
		Experiment: "exp", Session: "sess", Run: "run-1",
		Cell: cell, ClockDomain: "cd-test000000000000", TargetB: targetB,
	}
	builder, err := envelope.NewBuilder(cfg)
	if err != nil {
		t.Fatalf("builder: %v", err)
	}

	var buf bytes.Buffer
	writer, err := events.NewWriter(nopCloser{&buf}, events.RunHeader{
		Experiment: "exp", Session: "sess", Run: "run-1",
		Cell: cell, ClockDomain: "cd-test000000000000", WriterID: 3,
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}

	var clock int64
	service, err := New(Config{
		Model: "m_unbatched",
		Expectation: envelope.Expectation{
			Run: "run-1", Cell: cell, ClockDomain: "cd-test000000000000", TargetB: targetB,
		},
	}, exec, writer, func() int64 { clock += 500; return clock })
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}

	return &harness{service: service, stream: &buf, writer: writer, exec: exec, builder: builder, cell: cell, targetB: targetB}
}

func (h *harness) records(t *testing.T) []events.Decoded {
	t.Helper()
	if err := h.writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	decoded, err := events.ReadStream(bytes.NewReader(h.stream.Bytes()))
	if err != nil {
		t.Fatalf("read records: %v", err)
	}
	return decoded
}

func cohortMembers(size int) []identity.LogicalRequest {
	out := make([]identity.LogicalRequest, size)
	for i := range out {
		out[i] = identity.LogicalRequest{Cohort: 3, Ordinal: identity.Ordinal(i)}
	}
	return out
}

func payloads(n, size int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		// Distinct sizes, so a payload attributed to the wrong member is visible.
		out[i] = make([]byte, size+i)
	}
	return out
}

// One envelope carrying B members produces one record per member, and every
// envelope-granularity timestamp is shared by all of them — they describe the
// envelope, not the member.
func TestAggregateEnvelopeWritesOneRecordPerMemberWithSharedEnvelopeStages(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeExecutor{})

	members := cohortMembers(4)
	env, err := h.builder.Aggregate(members, payloads(4, 1024), 900, 1000)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}

	resp, err := h.service.Execute(context.Background(), env)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(resp.GetResults()) != 4 {
		t.Fatalf("response carries %d results, want 4", len(resp.GetResults()))
	}

	records := h.records(t)
	if len(records) != 4 {
		t.Fatalf("wrote %d records, want one per member", len(records))
	}

	var recv, send int64
	seen := map[identity.LogicalRequest]bool{}
	for i, d := range records {
		r := d.Record
		if r.Emitter != identity.EmitterAdapter {
			t.Errorf("record %d written by %v", i, r.Emitter)
		}
		if seen[r.Request()] {
			t.Errorf("%v recorded twice", r.Request())
		}
		seen[r.Request()] = true

		gotRecv, okRecv := r.Stage(events.StageAdapterRecv)
		gotSend, okSend := r.Stage(events.StageAdapterSend)
		if !okRecv || !okSend {
			t.Fatalf("%v is missing an adapter stage", r.Request())
		}
		if i == 0 {
			recv, send = gotRecv, gotSend
			continue
		}
		if gotRecv != recv || gotSend != send {
			t.Errorf("%v carries envelope stages %d/%d, but the envelope's are %d/%d — they describe the envelope",
				r.Request(), gotRecv, gotSend, recv, send)
		}
	}
	if len(seen) != 4 {
		t.Errorf("recorded %d distinct members, want 4", len(seen))
	}
}

// The adapter must attribute each member's payload size by identity. Indexing by
// position couples the executor's result order to the envelope's member order
// across a package boundary — invisible while an envelope carries one member.
func TestPayloadSizeIsAttributedByIdentityNotPosition(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeExecutor{reverse: true})

	members := cohortMembers(4)
	env, err := h.builder.Aggregate(members, payloads(4, 1024), 900, 1000)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if _, err := h.service.Execute(context.Background(), env); err != nil {
		t.Fatalf("execute: %v", err)
	}

	for _, d := range h.records(t) {
		want := uint32(1024 + int(d.Record.Ordinal))
		if d.Record.LogicalBytes != want {
			t.Errorf("%v recorded %d payload bytes, want %d — the executor returned members reversed and the "+
				"attribution followed position instead of identity",
				d.Record.Request(), d.Record.LogicalBytes, want)
		}
	}
}

// A failing member is reported failed and keeps its place; its neighbours are
// unaffected.
func TestFailingMemberIsReportedAndKeepsItsPlace(t *testing.T) {
	members := cohortMembers(4)
	h := newHarness(t, identity.CellF10, 4, &fakeExecutor{failFor: &members[1]})

	env, err := h.builder.Aggregate(members, payloads(4, 512), 900, 1000)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	resp, err := h.service.Execute(context.Background(), env)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	var failures int
	for _, r := range resp.GetResults() {
		if r.GetStatus() == envelopev1.Status_STATUS_ERROR {
			failures++
			if r.GetError() == "" {
				t.Error("a failed member carries no reason")
			}
			if identity.Ordinal(r.GetOrdinal()) != members[1].Ordinal {
				t.Errorf("the wrong member is marked failed: ordinal %d", r.GetOrdinal())
			}
		}
	}
	if failures != 1 {
		t.Errorf("%d members reported failed, want 1", failures)
	}
	if len(resp.GetResults()) != 4 {
		t.Errorf("the failed member vanished: %d results", len(resp.GetResults()))
	}
}

// The adapter's evidence about its own dispatch reaches the response, including
// the scope that says what the CPU number counted.
func TestAdapterReturnsItsDispatchEvidence(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeExecutor{})

	env, err := h.builder.Aggregate(cohortMembers(4), payloads(4, 256), 900, 1000)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	resp, err := h.service.Execute(context.Background(), env)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	ev := resp.GetEvidence()
	if ev == nil {
		t.Fatal("the response carries no adapter evidence")
	}
	if ev.GetDispatched() != 4 || ev.GetDispatchSkewNanos() != 42 {
		t.Errorf("evidence = dispatched %d skew %d, want 4 / 42", ev.GetDispatched(), ev.GetDispatchSkewNanos())
	}
	if ev.GetCpuScope() != events.CPUScopeProcess.String() {
		t.Errorf("cpu scope = %q, want %q", ev.GetCpuScope(), events.CPUScopeProcess)
	}
}

// Evidence that reaches only the response envelope does not survive the run.
// Every member of the dispatch carries it, because it describes the fan-out
// rather than the member, and the envelope id is what groups them back.
func TestDispatchEvidenceReachesEveryMemberRecord(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeExecutor{})

	env, err := h.builder.Aggregate(cohortMembers(4), payloads(4, 256), 900, 1000)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if _, err := h.service.Execute(context.Background(), env); err != nil {
		t.Fatalf("execute: %v", err)
	}

	records := h.records(t)
	if len(records) != 4 {
		t.Fatalf("got %d records, want one per member", len(records))
	}
	for _, d := range records {
		rec := d.Record
		if !rec.HasDispatch {
			t.Errorf("%v carries no dispatch evidence; the adapter measured it and dropped it", rec.Request())
			continue
		}
		if rec.Dispatch.Dispatched != 4 || rec.Dispatch.SkewNanos != 42 || rec.Dispatch.CPUNanos != 99 {
			t.Errorf("%v evidence = %+v, want dispatched 4 / skew 42 / cpu 99", rec.Request(), rec.Dispatch)
		}
		if rec.Dispatch.CPUScope != events.CPUScopeProcess {
			t.Errorf("%v cpu scope = %v, want %v; the number is not interpretable without it",
				rec.Request(), rec.Dispatch.CPUScope, events.CPUScopeProcess)
		}
		if rec.EnvelopeID != identity.EnvelopeID(env.GetEnvelopeId()) {
			t.Errorf("%v carries envelope %d, want %d", rec.Request(), rec.EnvelopeID, env.GetEnvelopeId())
		}
	}
}

// At A=off a dispatch releases one member, so the skew it measures is exactly
// zero — a measurement, not an absence.
func TestSingleMemberDispatchRecordsAMeasuredZeroSkew(t *testing.T) {
	zero := int64(0)
	h := newHarness(t, identity.CellF00, 1, &fakeExecutor{skew: &zero})

	env := h.builder.Independent(identity.LogicalRequest{Cohort: 3, Ordinal: 0}, []byte("payload"), 1000)
	if _, err := h.service.Execute(context.Background(), env); err != nil {
		t.Fatalf("execute: %v", err)
	}

	records := h.records(t)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	rec := records[0].Record
	if !rec.HasDispatch {
		t.Fatal("a skew of zero is what a one-member dispatch measures; the record must say it was measured")
	}
	if rec.Dispatch.Dispatched != 1 || rec.Dispatch.SkewNanos != 0 {
		t.Errorf("evidence = %+v, want dispatched 1 / skew 0", rec.Dispatch)
	}
}

// Protocol drift fails visibly rather than being interpreted leniently.
func TestAdapterRefusesAnEnvelopeThatIsNotItsRun(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeExecutor{})

	other, err := envelope.NewBuilder(envelope.Config{
		Experiment: "exp", Session: "sess", Run: "run-OTHER",
		Cell: identity.CellF10, ClockDomain: "cd-test000000000000", TargetB: 4,
	})
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	env, err := other.Aggregate(cohortMembers(4), payloads(4, 256), 900, 1000)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}

	if _, err := h.service.Execute(context.Background(), env); err == nil {
		t.Fatal("an envelope from another run should be refused")
	}
	if len(h.exec.seen) != 0 {
		t.Error("the adapter dispatched an envelope it should have refused")
	}
}

// At A=off one envelope carries one member, which is the shape the walking
// skeleton runs today — the adapter must keep serving it unchanged.
func TestIndependentEnvelopeStillProducesOneRecord(t *testing.T) {
	h := newHarness(t, identity.CellF00, 4, &fakeExecutor{})

	member := identity.LogicalRequest{Cohort: 3, Ordinal: 2}
	env := h.builder.Independent(member, make([]byte, 777), 1000)

	if _, err := h.service.Execute(context.Background(), env); err != nil {
		t.Fatalf("execute: %v", err)
	}
	records := h.records(t)
	if len(records) != 1 {
		t.Fatalf("wrote %d records for a one-member envelope, want 1", len(records))
	}
	if records[0].Record.Request() != member {
		t.Errorf("record describes %v, want %v", records[0].Record.Request(), member)
	}
	if records[0].Record.LogicalBytes != 777 {
		t.Errorf("payload bytes = %d, want 777", records[0].Record.LogicalBytes)
	}
}

func TestAdapterRejectsIncompleteConstruction(t *testing.T) {
	writerFor := func() *events.Writer {
		w, err := events.NewWriter(nopCloser{io.Discard}, events.RunHeader{
			Experiment: "e", Session: "s", Run: "r", Cell: identity.CellF00, ClockDomain: "cd-x",
		})
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
		return w
	}
	clock := func() int64 { return 0 }

	for name, build := range map[string]func() error{
		"no executor": func() error { _, err := New(Config{Model: "m"}, nil, writerFor(), clock); return err },
		"no writer":   func() error { _, err := New(Config{Model: "m"}, &fakeExecutor{}, nil, clock); return err },
		"no clock":    func() error { _, err := New(Config{Model: "m"}, &fakeExecutor{}, writerFor(), nil); return err },
		"no model":    func() error { _, err := New(Config{}, &fakeExecutor{}, writerFor(), clock); return err },
	} {
		if err := build(); err == nil {
			t.Errorf("%s: should have been refused", name)
		}
	}
}
