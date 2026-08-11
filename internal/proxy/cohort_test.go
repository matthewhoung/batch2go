package proxy

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// The proxy's own concurrency is what this slice introduces, so it is asserted
// at the proxy's seams — a client request in, a client response out, and one
// fake backend behind it — under the race detector and without a GPU. Nothing
// here reads the formation's internal state: a test that did would pass for a
// proxy that recorded the right numbers and shipped the wrong envelope.

// stepClock is a monotonic clock whose every read is a distinct instant. Two
// members reporting the same timestamp can then only have shared one, never
// have taken two reads that happened to agree — which is the whole content of
// the envelope-granularity assertions below.
type stepClock struct{ n atomic.Int64 }

func (c *stepClock) now() int64 { return c.n.Add(1_000) }

// fakeBackend stands in for the adapter. It keeps every envelope it was given,
// so "one envelope per cohort" is a count at the seam rather than a claim.
type fakeBackend struct {
	mu   sync.Mutex
	seen []*envelopev1.RequestEnvelope

	// reverse returns the results in the opposite order to the envelope's
	// members. A proxy that matched a caller to a result by position would then
	// hand every caller somebody else's answer, and every count would still
	// agree.
	reverse bool

	// fail, when set, is returned instead of a response.
	fail error
}

func (b *fakeBackend) Execute(_ context.Context, env *envelopev1.RequestEnvelope, _ ...grpc.CallOption) (*envelopev1.ResponseEnvelope, error) {
	b.mu.Lock()
	b.seen = append(b.seen, env)
	b.mu.Unlock()

	if b.fail != nil {
		return nil, b.fail
	}

	members := env.GetRequests()
	order := make([]int, len(members))
	for i := range order {
		order[i] = i
		if b.reverse {
			order[i] = len(members) - 1 - i
		}
	}

	resp := &envelopev1.ResponseEnvelope{
		SchemaVersion: envelope.SchemaVersion,
		RunId:         env.GetRunId(),
		CohortId:      env.GetCohortId(),
		EnvelopeId:    env.GetEnvelopeId(),
	}
	for _, i := range order {
		m := members[i]
		resp.Results = append(resp.Results, &envelopev1.LogicalResult{
			CohortId:       m.GetCohortId(),
			Ordinal:        m.GetOrdinal(),
			Status:         envelopev1.Status_STATUS_OK,
			MembershipUids: []int64{m.GetUid()},
			BatchSize:      1,
			DataOutBytes:   dataOutFor(identity.Ordinal(m.GetOrdinal())),
		})
	}
	return resp, nil
}

func (b *fakeBackend) envelopes() []*envelopev1.RequestEnvelope {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*envelopev1.RequestEnvelope(nil), b.seen...)
}

// dataOutFor makes every member's result distinguishable, so a result delivered
// to the wrong caller is visible rather than merely possible.
func dataOutFor(ord identity.Ordinal) uint64 { return 4096 + uint64(ord) }

// payloadFor gives each ordinal its own payload size, so a payload attributed
// to the wrong member shows up in the byte accounting.
func payloadFor(ord identity.Ordinal) []byte { return bytes.Repeat([]byte{byte(ord)}, 64+int(ord)) }

type harness struct {
	service *Service
	backend *fakeBackend
	stream  *bytes.Buffer
	writer  *events.Writer
	ctx     context.Context
}

func newHarness(t *testing.T, cell identity.Cell, targetB int, backend *fakeBackend) *harness {
	t.Helper()

	builder, err := envelope.NewBuilder(envelope.Config{
		Experiment: "exp", Session: "sess", Run: "run-1",
		Cell: cell, ClockDomain: "cd-test000000000000", TargetB: targetB,
	})
	if err != nil {
		t.Fatalf("builder: %v", err)
	}

	var stream bytes.Buffer
	writer, err := events.NewWriter(nopCloser{&stream}, events.RunHeader{
		Experiment: "exp", Session: "sess", Run: "run-1",
		Cell: cell, ClockDomain: "cd-test000000000000", WriterID: 2,
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}

	cfg := Config{Cell: cell, Run: "run-1", TargetB: targetB}
	if cell.AggregatesEnvelopes() {
		cfg.FormationDeadline = 5 * time.Second
	}

	clock := &stepClock{}
	service, err := New(cfg, builder, backend, writer, clock.now)
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}

	// A bounded context so that a formation bug fails the test instead of
	// hanging it. Nothing here expects to wait: every cohort below either
	// completes or is judged.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	return &harness{service: service, backend: backend, stream: &stream, writer: writer, ctx: ctx}
}

// answer is one caller's outcome at the proxy's client-facing seam.
type answer struct {
	member identity.LogicalRequest
	resp   *envelopev1.ClientResponse
	err    error
}

// release submits the given ordinals concurrently, the way the load generator's
// barrier releases a cohort, and returns what each caller got back. Duplicates
// are kept in submission order rather than keyed by member, because a cohort
// that received the same ordinal twice is exactly what some of these tests are
// about.
func (h *harness) release(cohort identity.CohortID, ordinals ...identity.Ordinal) []answer {
	answers := make([]answer, len(ordinals))
	var wg sync.WaitGroup
	for i, ord := range ordinals {
		wg.Add(1)
		go func(i int, ord identity.Ordinal) {
			defer wg.Done()
			member := identity.LogicalRequest{Cohort: cohort, Ordinal: ord}
			resp, err := h.service.Submit(h.ctx, &envelopev1.ClientRequest{
				RunId:    "run-1",
				CohortId: uint32(cohort),
				Ordinal:  uint32(ord),
				Uid:      int64(member.UID()),
				Payload:  payloadFor(ord),
			})
			answers[i] = answer{member: member, resp: resp, err: err}
		}(i, ord)
	}
	wg.Wait()
	return answers
}

// submit sends one request from the calling goroutine, for the arrivals whose
// ordering the test needs to be certain of.
func (h *harness) submit(cohort identity.CohortID, ord identity.Ordinal) (*envelopev1.ClientResponse, error) {
	member := identity.LogicalRequest{Cohort: cohort, Ordinal: ord}
	return h.service.Submit(h.ctx, &envelopev1.ClientRequest{
		RunId:    "run-1",
		CohortId: uint32(cohort),
		Ordinal:  uint32(ord),
		Uid:      int64(member.UID()),
		Payload:  payloadFor(ord),
	})
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

func ordinals(n int) []identity.Ordinal {
	out := make([]identity.Ordinal, n)
	for i := range out {
		out[i] = identity.Ordinal(i)
	}
	return out
}

// reversed is the arrival order that makes canonical order a claim rather than
// a coincidence: the members most likely to reach the proxy first are the ones
// that must travel last.
func reversed(n int) []identity.Ordinal {
	out := make([]identity.Ordinal, n)
	for i := range out {
		out[i] = identity.Ordinal(n - 1 - i)
	}
	return out
}

// This is Factor A. B logical requests submitted concurrently become one
// transport envelope, sealed once by the proxy's own clock, carrying the whole
// cohort in canonical order — where at A=off the same B requests become B
// envelopes and no seal at all.
func TestConcurrentSubmissionsBecomeOneSealedEnvelope(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeBackend{})

	for _, a := range h.release(7, reversed(4)...) {
		if a.err != nil {
			t.Fatalf("%v: %v", a.member, a.err)
		}
	}

	envs := h.backend.envelopes()
	if len(envs) != 1 {
		t.Fatalf("the backend saw %d envelopes for one cohort, want exactly 1; that count is Factor A", len(envs))
	}
	env := envs[0]

	if !env.GetAggregate() {
		t.Error("the envelope does not declare itself an aggregate")
	}
	if got := identity.CohortID(env.GetCohortId()); got != 7 {
		t.Errorf("envelope carries cohort %d, want 7", got)
	}
	if got := env.GetExpectedMembers(); got != 4 {
		t.Errorf("envelope declares %d expected members, want 4", got)
	}
	if got := len(env.GetRequests()); got != 4 {
		t.Fatalf("envelope carries %d members, want the whole cohort of 4", got)
	}
	if env.TCohortSeal == nil {
		t.Error("the envelope carries no cohort seal; at A=on the proxy owns it (ADR-0010)")
	}

	// Canonical order, whatever order the members arrived in: the adapter fans a
	// dispatch out in the order the envelope lists, so arrival order here would
	// become dispatch skew there.
	for i, m := range env.GetRequests() {
		if got := identity.Ordinal(m.GetOrdinal()); got != identity.Ordinal(i) {
			t.Errorf("member %d of the envelope is ordinal %d, want %d — members travel in canonical order", i, got, i)
		}
		member := identity.LogicalRequest{Cohort: 7, Ordinal: identity.Ordinal(m.GetOrdinal())}
		if got := identity.UID(m.GetUid()); got != member.UID() {
			t.Errorf("%v carries uid %d, its identity encodes %d", member, got, member.UID())
		}
		if got, want := len(m.GetPayload()), len(payloadFor(member.Ordinal)); got != want {
			t.Errorf("%v carries %d payload bytes, want %d — payloads are attributed by identity", member, got, want)
		}
	}
}

// Cohorts are kept apart by identity, not by arriving one at a time. The
// identification regime releases them singly, but nothing in the proxy depends
// on that, and a registry that let two overlapping cohorts touch would produce
// envelopes whose membership names requests from another cohort entirely.
func TestOverlappingCohortsFormIndependently(t *testing.T) {
	const cohorts, size = 24, 4
	h := newHarness(t, identity.CellF10, size, &fakeBackend{})

	var wg sync.WaitGroup
	for c := 0; c < cohorts; c++ {
		wg.Add(1)
		go func(c identity.CohortID) {
			defer wg.Done()
			for _, a := range h.release(c, reversed(size)...) {
				if a.err != nil {
					t.Errorf("%v: %v", a.member, a.err)
				}
			}
		}(identity.CohortID(c))
	}
	wg.Wait()

	envs := h.backend.envelopes()
	if len(envs) != cohorts {
		t.Fatalf("the backend saw %d envelopes, want one per cohort (%d)", len(envs), cohorts)
	}
	sealed := make(map[identity.CohortID]bool, cohorts)
	for _, env := range envs {
		id := identity.CohortID(env.GetCohortId())
		if sealed[id] {
			t.Errorf("cohort %d was sealed into more than one envelope", id)
		}
		sealed[id] = true
		if got := len(env.GetRequests()); got != size {
			t.Errorf("cohort %d travelled with %d members, want %d", id, got, size)
		}
		for i, m := range env.GetRequests() {
			if got := identity.CohortID(m.GetCohortId()); got != id {
				t.Errorf("the envelope for cohort %d carries a member of cohort %d", id, got)
			}
			if got := identity.Ordinal(m.GetOrdinal()); got != identity.Ordinal(i) {
				t.Errorf("cohort %d: member %d is ordinal %d", id, i, got)
			}
		}
	}
}

// Cohort-granularity quantities are properties of the envelope, not of the
// member, so every member reports the same ones. Disagreement between them is
// how B envelopes wearing F10's label are caught, and it can only be detected
// if agreement is what a correct proxy produces.
func TestEveryMemberObservesTheSameSealAndEnvelopeStages(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeBackend{})

	for _, a := range h.release(7, ordinals(4)...) {
		if a.err != nil {
			t.Fatalf("%v: %v", a.member, a.err)
		}
	}

	records := h.records(t)
	if len(records) != 4 {
		t.Fatalf("the proxy wrote %d records, want one per member", len(records))
	}

	type shared struct{ seal, send, respRecv int64 }
	var want shared
	var wantEnvelope identity.EnvelopeID
	arrivals := make(map[identity.LogicalRequest]int64, 4)

	for i, d := range records {
		r := d.Record
		if r.Emitter != identity.EmitterProxy {
			t.Errorf("%v was recorded by %v; the proxy owns these stages", r.Request(), r.Emitter)
		}

		seal, okSeal := r.Stage(events.StageCohortSeal)
		send, okSend := r.Stage(events.StageProxySend)
		respRecv, okResp := r.Stage(events.StageProxyRespRecv)
		recv, okRecv := r.Stage(events.StageProxyRecv)
		if !okSeal || !okSend || !okResp || !okRecv {
			t.Fatalf("%v is missing a proxy stage: presence %s", r.Request(), r.Presence)
		}
		arrivals[r.Request()] = recv

		// The byte accounting splits the same way the timestamps do: the envelope's
		// size is the cohort's and every member reports it, while the payload size
		// is the member's own and a member reporting its neighbour's would be a
		// misattribution no count would notice.
		if got, want := r.LogicalBytes, uint32(len(payloadFor(r.Ordinal))); got != want {
			t.Errorf("%v reports %d logical bytes, its own payload is %d", r.Request(), got, want)
		}

		got := shared{seal, send, respRecv}
		if i == 0 {
			want, wantEnvelope = got, r.EnvelopeID
			continue
		}
		if got != want {
			t.Errorf("%v reports envelope stages %+v, the cohort's are %+v — they describe the envelope, not the member",
				r.Request(), got, want)
		}
		if r.EnvelopeID != wantEnvelope {
			t.Errorf("%v travelled in envelope %d, the cohort's is %d; B envelopes is A=off wearing F10's label",
				r.Request(), r.EnvelopeID, wantEnvelope)
		}
	}

	if len(arrivals) != 4 {
		t.Fatalf("recorded %d distinct members, want 4", len(arrivals))
	}
	// The seal on the wire and the seal in the archive are one instant. If they
	// could differ, the adapter and the analysis would be reading two numbers.
	envs := h.backend.envelopes()
	if len(envs) != 1 {
		t.Fatalf("the backend saw %d envelopes, want 1", len(envs))
	}
	if got := envs[0].GetTCohortSeal(); got != want.seal {
		t.Errorf("the envelope was sealed at %d, the records say %d", got, want.seal)
	}
	if got := identity.EnvelopeID(envs[0].GetEnvelopeId()); got != wantEnvelope {
		t.Errorf("the envelope is %d, the records say %d", got, wantEnvelope)
	}
}

// Each caller gets its own member's answer. The backend hands the results back
// in the opposite order to the envelope's members, so a proxy that matched by
// position would satisfy every count and every cardinality check while giving
// each caller its neighbour's result.
func TestEachCallerReceivesItsOwnMemberResult(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeBackend{reverse: true})

	for _, a := range h.release(7, ordinals(4)...) {
		if a.err != nil {
			t.Fatalf("%v: %v", a.member, a.err)
		}
		if got := identity.CohortID(a.resp.GetCohortId()); got != a.member.Cohort {
			t.Errorf("%v was answered for cohort %d", a.member, got)
		}
		if got := identity.Ordinal(a.resp.GetOrdinal()); got != a.member.Ordinal {
			t.Errorf("%v was answered with ordinal %d — the response was matched by position, not identity", a.member, got)
		}
		if got, want := a.resp.GetDataOutBytes(), dataOutFor(a.member.Ordinal); got != want {
			t.Errorf("%v received %d output bytes, its own result carried %d", a.member, got, want)
		}
		uids := a.resp.GetMembershipUids()
		if len(uids) != 1 || identity.UID(uids[0]) != a.member.UID() {
			t.Errorf("%v received membership %v, its own uid is %d", a.member, uids, a.member.UID())
		}
	}
}

// Formation wait is the interval from a member's arrival at the proxy to the
// seal. It is per member — the first to arrive waits longest — and it exists
// only because the proxy holds the cohort, which is why it can be measured at
// A=on and structurally cannot be at A=off (ADR-0010).
func TestFormationWaitIsMeasuredPerMember(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeBackend{})

	for _, a := range h.release(7, ordinals(4)...) {
		if a.err != nil {
			t.Fatalf("%v: %v", a.member, a.err)
		}
	}

	waits := make(map[int64]identity.LogicalRequest, 4)
	for _, d := range h.records(t) {
		r := d.Record
		recv, okRecv := r.Stage(events.StageProxyRecv)
		seal, okSeal := r.Stage(events.StageCohortSeal)
		if !okRecv || !okSeal {
			t.Fatalf("%v cannot report a formation wait: presence %s", r.Request(), r.Presence)
		}
		if seal < recv {
			t.Errorf("%v was sealed at %d, before it arrived at %d", r.Request(), seal, recv)
		}
		if other, clash := waits[seal-recv]; clash {
			t.Errorf("%v and %v report the same formation wait %d; the members arrived at different instants",
				r.Request(), other, seal-recv)
		}
		waits[seal-recv] = r.Request()
	}
	if len(waits) != 4 {
		t.Errorf("recorded %d distinct formation waits, want one per member", len(waits))
	}
}

// The seal has exactly one owner at each factor level. At A=on the proxy mints
// it; at A=off the load generator records the barrier release itself and the
// proxy emits none, because there is nothing there to seal (ADR-0001).
func TestTheProxySealsOnlyWhereItAggregates(t *testing.T) {
	for _, tc := range []struct {
		cell     identity.Cell
		wantSeal bool
	}{
		{identity.CellF00, false},
		{identity.CellF10, true},
	} {
		h := newHarness(t, tc.cell, 4, &fakeBackend{})
		for _, a := range h.release(7, ordinals(4)...) {
			if a.err != nil {
				t.Fatalf("%s %v: %v", tc.cell, a.member, a.err)
			}
		}
		for _, d := range h.records(t) {
			r := d.Record
			if got := r.Presence.Has(events.StageCohortSeal); got != tc.wantSeal {
				t.Errorf("%s: %v carries a proxy seal=%v, want %v (owner is %s)",
					tc.cell, r.Request(), got, tc.wantSeal, events.SealOwner(tc.cell))
			}
		}
	}
}

// At A=off the proxy joins nothing: each logical request becomes its own
// envelope of one member, carrying no seal. This is the behaviour F10 must not
// have, asserted at the same seam so that the contrast is one test apart.
func TestAtAOffEachRequestBecomesItsOwnEnvelope(t *testing.T) {
	h := newHarness(t, identity.CellF00, 4, &fakeBackend{})

	for _, a := range h.release(7, ordinals(4)...) {
		if a.err != nil {
			t.Fatalf("%v: %v", a.member, a.err)
		}
	}

	envs := h.backend.envelopes()
	if len(envs) != 4 {
		t.Fatalf("the backend saw %d envelopes, want one per logical request", len(envs))
	}
	seen := make(map[uint64]bool, 4)
	for _, env := range envs {
		if env.GetAggregate() {
			t.Error("an A=off envelope declares itself an aggregate")
		}
		if got := len(env.GetRequests()); got != 1 {
			t.Errorf("an A=off envelope carries %d members, want exactly 1", got)
		}
		if env.TCohortSeal != nil {
			t.Error("an A=off envelope carries a proxy seal; the load generator owns it (ADR-0001)")
		}
		if seen[env.GetEnvelopeId()] {
			t.Errorf("envelope id %d was reused", env.GetEnvelopeId())
		}
		seen[env.GetEnvelopeId()] = true
	}
}

// A member arriving twice fails the cohort rather than being absorbed. Two
// copies of one ordinal are two members by count, and a proxy that counted
// would ship a cohort of B whose attested membership names a request nobody
// released.
func TestAMemberArrivingTwiceFailsTheCohort(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeBackend{})

	// Ordinal 1 twice. Whichever copy is admitted first, the other is the
	// duplicate, so the cohort's fate does not depend on which goroutine wins.
	answers := h.release(7, 0, 1, 1)
	for _, a := range answers {
		if a.err == nil {
			t.Errorf("%v was answered, but its cohort could not form", a.member)
		}
	}
	if !mentions(answers, "arrived twice") {
		t.Errorf("no caller was told what was wrong with the cohort: %v", errorsOf(answers))
	}
	if envs := h.backend.envelopes(); len(envs) != 0 {
		t.Errorf("the backend saw %d envelopes; a cohort that cannot form sends none", len(envs))
	}

	// A member arriving afterwards inherits the same judgement rather than
	// opening a fresh formation on a cohort id that has already been judged.
	_, err := h.submit(7, 2)
	if err == nil {
		t.Fatal("a member arriving after the failure was admitted to a fresh formation")
	}
	if !strings.Contains(err.Error(), "arrived twice") {
		t.Errorf("the late arrival was told %q, not the cohort's own cause", err)
	}

	// Every member the proxy touched is in the record with its failure. A member
	// missing from the record is indistinguishable from one never released.
	records := h.records(t)
	if len(records) != 4 {
		t.Fatalf("the proxy wrote %d records, want one per member it touched", len(records))
	}
	for _, d := range records {
		r := d.Record
		if r.Status != events.StatusError {
			t.Errorf("%v was recorded %s; its cohort could not form", r.Request(), r.Status)
		}
		if r.Presence.Has(events.StageCohortSeal) {
			t.Errorf("%v carries a seal, but its cohort was never sealed", r.Request())
		}
		if !r.Presence.Has(events.StageProxyRecv) {
			t.Errorf("%v records no arrival, and it did arrive", r.Request())
		}
	}
}

// A member arriving twice is refused after the seal as well as before it. The
// two are the same fault seen at different moments — an ordinal that has already
// travelled — and a proxy that only checked the cohorts still forming would take
// the late copy for the first member of a new cohort wearing an id that has
// already been sent, and hold it until its own deadline expired.
func TestAMemberArrivingAfterItsCohortSealedIsRefused(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeBackend{})

	for _, a := range h.release(7, ordinals(4)...) {
		if a.err != nil {
			t.Fatalf("%v: %v", a.member, a.err)
		}
	}

	if _, err := h.submit(7, 2); err == nil {
		t.Fatal("ordinal 2 was admitted again after its cohort had been sealed and sent")
	} else if !strings.Contains(err.Error(), "already sealed") {
		t.Errorf("the late copy was told %q, which does not say the cohort had gone", err)
	}

	if envs := h.backend.envelopes(); len(envs) != 1 {
		t.Errorf("the backend saw %d envelopes; the late copy must not open a second one", len(envs))
	}
}

// An ordinal outside the declared cohort fails the cohort too. B comes from the
// manifest and is never inferred from what arrives, so a member the cohort
// cannot contain is a fault in the cohort, not a member to make room for.
func TestAnOrdinalOutsideTheCohortFailsIt(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeBackend{})

	answers := h.release(7, 0, 1, 4)
	for _, a := range answers {
		if a.err == nil {
			t.Errorf("%v was answered, but its cohort could not form", a.member)
		}
	}
	if !mentions(answers, "outside [0,4)") {
		t.Errorf("no caller was told which ordinal the cohort could not contain: %v", errorsOf(answers))
	}
	if envs := h.backend.envelopes(); len(envs) != 0 {
		t.Errorf("the backend saw %d envelopes; a cohort that cannot form sends none", len(envs))
	}

	if _, err := h.submit(7, 2); err == nil {
		t.Error("a member arriving after the failure was admitted to a fresh formation")
	}
}

// A backend that refuses the envelope fails the whole cohort, because the
// envelope is the whole cohort. Every member is told, and every member is
// recorded — with the envelope's own stages, which did happen.
func TestABackendRefusalReachesEveryMember(t *testing.T) {
	h := newHarness(t, identity.CellF10, 4, &fakeBackend{fail: context.Canceled})

	for _, a := range h.release(7, ordinals(4)...) {
		if a.err == nil {
			t.Errorf("%v was answered, but the backend refused its envelope", a.member)
		}
	}

	records := h.records(t)
	if len(records) != 4 {
		t.Fatalf("the proxy wrote %d records, want one per member", len(records))
	}
	for _, d := range records {
		r := d.Record
		if r.Status != events.StatusError {
			t.Errorf("%v was recorded %s after its envelope was refused", r.Request(), r.Status)
		}
		if !r.Presence.Has(events.StageCohortSeal) {
			t.Errorf("%v records no seal, and its cohort was sealed before the send", r.Request())
		}
	}
}

func errorsOf(answers []answer) []error {
	out := make([]error, 0, len(answers))
	for _, a := range answers {
		out = append(out, a.err)
	}
	return out
}

func mentions(answers []answer, phrase string) bool {
	for _, a := range answers {
		if a.err != nil && strings.Contains(a.err.Error(), phrase) {
			return true
		}
	}
	return false
}
