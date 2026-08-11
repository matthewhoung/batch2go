// Package testkit builds the evidence the live stack cannot produce.
//
// Two kinds of fixture live here, and both exist because the validator is the
// thing being tested, not the thing doing the testing:
//
//   - Injected-delay bundles, where every named stage was given a duration
//     chosen in advance. The validator must recover each one. Until it does, no
//     live conservation number means anything, because nothing has established
//     that the decomposition attributes time to the right stage.
//
//   - Defective bundles, each carrying exactly one planted fault. The validator
//     must fail each of them and name the fault. A validator that passes
//     everything is indistinguishable from no validator at all.
package testkit

import (
	"fmt"
	"time"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/validate"
)

// Spec describes a synthetic run to build evidence for.
type Spec struct {
	Cell        identity.Cell
	Run         identity.RunID
	ClockDomain identity.ClockDomainID

	CohortSize    int
	CohortCount   int
	FirstCohortID identity.CohortID

	// BaseInstant is where the synthetic monotonic clock starts.
	BaseInstant int64

	// StageDelays gives each named stage of the cell's chain its duration. A
	// stage without an entry gets DefaultStageDelay.
	StageDelays map[string]time.Duration

	// CohortGap separates consecutive cohorts, and MemberStagger offsets members
	// within one cohort so that their in-flight intervals overlap the way real
	// concurrent members do.
	CohortGap     time.Duration
	MemberStagger time.Duration

	// InterExecutionGap separates consecutive model executions within a cohort.
	//
	// A V=off cohort's B executions serialize on the single model instance —
	// request i waits behind requests 0..i-1, and that wait is what the cycle
	// model books in Q_backend (M1 §2.2). A fixture that let the executions run in
	// parallel would model a machine this design does not have, and every offline
	// assertion about backend timing would be checked against evidence no run
	// could produce.
	InterExecutionGap time.Duration

	// DispatchStagger is how far apart the adapter submitted consecutive members
	// of one fan-out. Zero is a concurrent release, which is what a correct run
	// performs and what makes its reported skew a measured zero.
	//
	// It is one knob because it is one physical quantity, and the two ways a
	// fan-out goes wrong are two values of it rather than two mechanisms: a small
	// stagger is a release whose members still overlap but reached the backend
	// further apart than the bound allows, and a stagger wider than a member's
	// whole downstream is a sequence of releases wearing the shape of one call.
	DispatchStagger time.Duration

	// ReportedSkewNanos overrides what the adapter claims its skew was. Left nil,
	// the adapter reports what its own dispatch timestamps say — which is what a
	// correct one does, since both come from the same clock readings. Setting it
	// builds the fixture where the adapter's arithmetic and its own evidence
	// disagree, and it is a pointer because zero is a legitimate claim.
	ReportedSkewNanos *int64

	ToleranceFraction float64
}

// dispatchedPerFanOut is how many members the adapter releases in one call: one
// per envelope at A=off, the whole cohort at A=on.
func (s Spec) dispatchedPerFanOut() int {
	if s.Cell.AggregatesEnvelopes() {
		return s.CohortSize
	}
	return 1
}

// DefaultStageDelay is used for any stage the spec does not name.
const DefaultStageDelay = 100 * time.Microsecond

// defaultStageDelays give a well-formed fixture a realistically shaped path:
// compute dominates, transfers are small, and the intervals the cycle model does
// not name are smaller still.
//
// The shape matters, not the values. A fixture where every interval had the same
// duration would put the unnamed intervals at a large fraction of the path, and
// a correct run would fail the conservation tolerance for no reason but the
// fixture's own arithmetic.
func defaultStageDelays() map[string]time.Duration {
	return map[string]time.Duration{
		"W_form":          30 * time.Microsecond,
		"barrier_wait":    30 * time.Microsecond,
		"A_pack":          150 * time.Microsecond,
		"X_req":           200 * time.Microsecond,
		"X_req_hop1":      200 * time.Microsecond,
		"X_req_hop2":      200 * time.Microsecond,
		"X_req_hop3":      100 * time.Microsecond,
		"Q_backend":       1000 * time.Microsecond,
		"S_comp":          5000 * time.Microsecond,
		"X_resp":          200 * time.Microsecond,
		"X_resp_hop1":     100 * time.Microsecond,
		"X_resp_hop2":     200 * time.Microsecond,
		"X_resp_hop3":     200 * time.Microsecond,
		"F_fanout":        150 * time.Microsecond,
		"release_to_send": 20 * time.Microsecond,
		"adapter_unpack":  20 * time.Microsecond,
		"response_pack":   20 * time.Microsecond,
	}
}

// NewSpec returns a spec for a well-formed run of the given cell.
func NewSpec(cell identity.Cell) Spec {
	return Spec{
		Cell:              cell,
		Run:               identity.RunID("run-fixture-" + string(cell)),
		ClockDomain:       "cd-fixture000000000000",
		CohortSize:        4,
		CohortCount:       5,
		FirstCohortID:     10,
		BaseInstant:       1_000_000_000_000,
		StageDelays:       defaultStageDelays(),
		CohortGap:         50 * time.Millisecond,
		MemberStagger:     200 * time.Microsecond,
		InterExecutionGap: 100 * time.Microsecond,
		ToleranceFraction: 0.05,
	}
}

// WithDelay sets one named stage's injected duration.
func (s Spec) WithDelay(stage string, d time.Duration) Spec {
	delays := make(map[string]time.Duration, len(s.StageDelays)+1)
	for k, v := range s.StageDelays {
		delays[k] = v
	}
	delays[stage] = d
	s.StageDelays = delays
	return s
}

// Delay reports the duration a stage will be given.
func (s Spec) Delay(stage string) time.Duration {
	if d, ok := s.StageDelays[stage]; ok {
		return d
	}
	return DefaultStageDelay
}

// Bundle is a built fixture: the records a run would have produced, and the
// expectation a validator should judge them against.
type Bundle struct {
	Spec        Spec
	Records     []events.Decoded
	Expectation validate.Expectation
}

// Build produces the evidence for a well-formed run of the spec.
//
// Records are split across emitters exactly as the real path splits them — each
// process writes only the stages it owns — so the validator has to perform the
// same offline join it performs on a live bundle. A fixture that handed it one
// pre-joined record per request would not exercise the join at all.
func (s Spec) Build() (Bundle, error) {
	spans, err := validate.Chain(s.Cell)
	if err != nil {
		return Bundle{}, err
	}
	batchSize, executionsPerCohort := s.evidenceShape()

	var records []events.Decoded
	var nextEnvelope identity.EnvelopeID
	for c := 0; c < s.CohortCount; c++ {
		cohortID := s.FirstCohortID + identity.CohortID(c)
		cohortStart := s.BaseInstant + int64(c)*int64(s.CohortGap)

		var stamps []map[events.Stage]int64
		var envelopes []identity.EnvelopeID
		if s.Cell.AggregatesEnvelopes() {
			stamps = s.aggregateCohort(cohortStart, batchSize)
			// One envelope carries the whole cohort, so its id is the cohort's and
			// every member reports it. A fixture that minted one per member would be
			// describing B envelopes wearing an A=on label — which is exactly the
			// fake the cardinality check exists to catch, and it would pass.
			nextEnvelope++
			envelopes = repeatEnvelope(nextEnvelope, s.CohortSize)
		} else {
			stamps = s.independentCohort(spans, cohortStart, batchSize)
			for o := 0; o < s.CohortSize; o++ {
				nextEnvelope++
				envelopes = append(envelopes, nextEnvelope)
			}
		}

		// The adapter reports what its own dispatch timestamps span. At A=off each
		// envelope is a release of one and there is nothing to skew.
		skew := int64(0)
		if s.Cell.AggregatesEnvelopes() {
			skew = lastOf(stamps, events.StageAdapterDispatch) - firstOf(stamps, events.StageAdapterDispatch)
		}

		for o := 0; o < s.CohortSize; o++ {
			req := identity.LogicalRequest{Cohort: cohortID, Ordinal: identity.Ordinal(o)}
			membership := s.membership(req, batchSize)
			records = append(records, s.splitByEmitter(req, stamps[o], envelopes[o], skew, membership, batchSize)...)
		}
	}

	executions := s.CohortCount * executionsPerCohort
	bundle := Bundle{
		Spec:    s,
		Records: records,
		Expectation: validate.Expectation{
			Run:                         s.Run,
			Cell:                        s.Cell,
			ClockDomain:                 s.ClockDomain,
			CohortSize:                  s.CohortSize,
			CohortCount:                 s.CohortCount,
			FirstCohortID:               s.FirstCohortID,
			ExecutionsPerCohort:         executionsPerCohort,
			BatchSize:                   batchSize,
			Executions:                  executions,
			ToleranceFraction:           s.ToleranceFraction,
			MaxAdapterDispatchWaitNanos: int64(4 * s.Delay("adapter_unpack")),
			// Well above a concurrent release, which skews by nothing, and far below
			// one execution's service time — the claim the bound stands for is that
			// the members reached the backend together, so a bound that a serial
			// fan-out could satisfy would assert nothing.
			MaxDispatchSkewNanos: int64(2 * s.Delay(validate.StageAdapterUnpack)),
			ExecutionCountDelta:  uint64(executions),
			InferenceCountDelta:  uint64(s.CohortCount * s.CohortSize),
			BatchSizeHistogram:   map[uint64]uint64{uint64(batchSize): uint64(executions)},
		},
	}
	// The builder checks its own output. An A=on fixture that gave each member its
	// own seal would satisfy every F10 assertion in the validator while describing
	// evidence no proxy could emit, and the assertions would then be tested
	// against a fiction. This has happened once already: the first fixtures
	// modelled the backend as fully parallel, contradicting the serialization the
	// design declares, and it went unnoticed until a check was written that could
	// fail.
	if err := bundle.checkGranularity(); err != nil {
		return Bundle{}, err
	}
	return bundle, nil
}

// repeatEnvelope gives every member of a cohort the same envelope id.
func repeatEnvelope(id identity.EnvelopeID, n int) []identity.EnvelopeID {
	out := make([]identity.EnvelopeID, n)
	for i := range out {
		out[i] = id
	}
	return out
}

// independentCohort is the A=off construction: every member walks the whole
// chain on its own, because nothing joins them anywhere on the path.
func (s Spec) independentCohort(spans []validate.Span, cohortStart int64, batchSize int) []map[events.Stage]int64 {
	out := make([]map[events.Stage]int64, s.CohortSize)

	// prevComputeEnd carries the serialization forward: member i cannot begin
	// executing until member i-1 has finished on the one model instance.
	var prevComputeEnd int64
	for o := 0; o < s.CohortSize; o++ {
		// Walk the chain, giving each named stage its injected duration.
		ts := map[events.Stage]int64{}
		now := cohortStart + int64(o)*int64(s.MemberStagger)
		ts[spans[0].Start] = now
		for _, span := range spans {
			now += int64(s.Delay(span.Name))
			ts[span.End] = now
		}

		if batchSize == 1 && o > 0 {
			// Push this member's execution behind the previous one, absorbing the
			// wait into Q_backend and leaving every other stage's duration
			// untouched — which is exactly where M1 §2.2 books it.
			if wait := prevComputeEnd + int64(s.InterExecutionGap) - ts[events.StageComputeStart]; wait > 0 {
				shiftFrom(ts, spans, events.StageComputeStart, wait)
			}
		}
		prevComputeEnd = ts[events.StageComputeEnd]
		out[o] = ts
	}
	return out
}

// aggregateCohort is the A=on construction: the cohort is assembled, sealed,
// sent, executed and answered as one object, and only the parts that really are
// per member vary between members.
//
// It cannot be a per-member walk. Walking the chain independently would give
// every member its own seal, its own envelope timestamps and its own response —
// evidence no proxy could emit, and evidence against which every F10 assertion
// would pass. What varies per member is where the physics says it does: when a
// member reached the proxy, when its own execution ran, and when its own answer
// went back.
func (s Spec) aggregateCohort(cohortStart int64, batchSize int) []map[events.Stage]int64 {
	d := func(stage string) int64 { return int64(s.Delay(stage)) }

	ts := make([]map[events.Stage]int64, s.CohortSize)
	for o := range ts {
		ts[o] = map[events.Stage]int64{}
	}

	// The members arrive on their own schedules; nothing has joined them yet.
	for o := range ts {
		ts[o][events.StageSched] = cohortStart + int64(o)*int64(s.MemberStagger)
		ts[o][events.StageClientSend] = ts[o][events.StageSched] + d(validate.StageReleaseToSend)
		ts[o][events.StageProxyRecv] = ts[o][events.StageClientSend] + d(validate.StageXReqHop1)
	}

	// Formation ends when the cohort is whole, so the seal follows the LAST
	// arrival. Every member's W_form is measured back to its own arrival from
	// that one instant, which is why the first to arrive waits longest and why
	// the injected W_form is the floor of the cohort's spread rather than its
	// median.
	seal := lastOf(ts, events.StageProxyRecv) + d(validate.StageWForm)
	send := seal + d(validate.StageAPack)
	adapterRecv := send + d(validate.StageXReqHop2)

	// One unpack, then the fan-out. Its submissions are simultaneous by default:
	// a skew of zero is the measurement a concurrent release produces, and a
	// fixture that staggered them as a matter of course would make the serial
	// fan-out differ from a correct run by degree rather than by kind.
	dispatch := adapterRecv + d(validate.StageAdapterUnpack)

	if batchSize == 1 {
		// V=off: B executions serializing on the single model instance.
		var prevComputeEnd int64
		for o := range ts {
			submitted := dispatch + int64(o)*int64(s.DispatchStagger)
			ts[o][events.StageAdapterDispatch] = submitted
			ts[o][events.StageQueueStart] = submitted + d(validate.StageXReqHop3)
			computeStart := ts[o][events.StageQueueStart] + d(validate.StageQBackend)
			if o > 0 {
				if wait := prevComputeEnd + int64(s.InterExecutionGap); wait > computeStart {
					computeStart = wait
				}
			}
			ts[o][events.StageComputeStart] = computeStart
			ts[o][events.StageComputeEnd] = computeStart + d(validate.StageSComp)
			prevComputeEnd = ts[o][events.StageComputeEnd]
			ts[o][events.StageAdapterResult] = ts[o][events.StageComputeEnd] + d(validate.StageXRespHop1)
		}
	} else {
		// V=on: the cohort is one execution, so its members share one window
		// rather than each having their own. A fixture that gave them separate
		// windows would describe B executions wearing a V=on label.
		queueStart := dispatch + d(validate.StageXReqHop3)
		computeStart := queueStart + d(validate.StageQBackend)
		computeEnd := computeStart + d(validate.StageSComp)
		for o := range ts {
			ts[o][events.StageAdapterDispatch] = dispatch
			ts[o][events.StageQueueStart] = queueStart
			ts[o][events.StageComputeStart] = computeStart
			ts[o][events.StageComputeEnd] = computeEnd
			ts[o][events.StageAdapterResult] = computeEnd + d(validate.StageXRespHop1)
		}
	}

	// One response, packed after the last member's result and carried back once.
	adapterSend := lastOf(ts, events.StageAdapterResult) + d(validate.StageResponsePack)
	respRecv := adapterSend + d(validate.StageXRespHop2)

	for o := range ts {
		ts[o][events.StageCohortSeal] = seal
		ts[o][events.StageProxySend] = send
		ts[o][events.StageAdapterRecv] = adapterRecv
		ts[o][events.StageAdapterSend] = adapterSend
		ts[o][events.StageProxyRespRecv] = respRecv
		// The fan-out back is the member's own stage, and the fixture gives each
		// the same duration rather than modelling the wake order: what makes it a
		// member's stage is who owns it, not that its value has to differ.
		ts[o][events.StageProxyFanout] = respRecv + d(validate.StageFFanout)
		ts[o][events.StageClientRecv] = ts[o][events.StageProxyFanout] + d(validate.StageXRespHop3)
	}
	return ts
}

// firstOf is the earliest value a cohort recorded for one stage — when the
// cohort as a whole began arriving, or began being released.
func firstOf(ts []map[events.Stage]int64, stage events.Stage) int64 {
	var first int64
	var seen bool
	for _, m := range ts {
		if v, ok := m[stage]; ok && (!seen || v < first) {
			first, seen = v, true
		}
	}
	return first
}

// lastOf is the latest value a cohort recorded for one stage — when the cohort
// as a whole finished arriving, or finished executing.
func lastOf(ts []map[events.Stage]int64, stage events.Stage) int64 {
	var last int64
	for _, m := range ts {
		if v, ok := m[stage]; ok && v > last {
			last = v
		}
	}
	return last
}

// shiftFrom moves a stage and everything after it in TRAVERSAL order later by
// delta, so the interval ending at that stage stretches and every later interval
// keeps its duration.
//
// The order comes from the cell's chain, not from the schema's numbering. Those
// are different: t_cohort_seal is timestamp 4 but at A=off it is emitted before
// the client send, so comparing stage numbers would shift the wrong set. The
// single call site happens to be safe under either, which is precisely why the
// wrong one would have gone unnoticed.
//
// V=on cells are not handled here: a vectorized cohort executes once, so all B
// members share one execution window rather than serializing. Building that is
// owned by the testkit work of spec 0002; until then no V=on cell runs, and the
// serialization check skips them.
func shiftFrom(ts map[events.Stage]int64, spans []validate.Span, from events.Stage, delta int64) {
	order := validate.ChainStages(spans)
	at := -1
	for i, stage := range order {
		if stage == from {
			at = i
			break
		}
	}
	if at < 0 {
		return
	}
	for _, stage := range order[at:] {
		if _, ok := ts[stage]; ok {
			ts[stage] += delta
		}
	}
}

// MustBuild is Build for tests that have no meaningful way to handle failure.
func (s Spec) MustBuild() Bundle {
	b, err := s.Build()
	if err != nil {
		panic(err)
	}
	return b
}

func (s Spec) evidenceShape() (batchSize, executionsPerCohort int) {
	if s.Cell.VectorizesCompute() {
		return s.CohortSize, 1
	}
	return 1, s.CohortSize
}

// membership is what the model would attest: at V=off an execution has one
// member, at V=on it has the whole cohort.
func (s Spec) membership(req identity.LogicalRequest, batchSize int) []identity.UID {
	if batchSize == 1 {
		return []identity.UID{req.UID()}
	}
	out := make([]identity.UID, 0, batchSize)
	for o := 0; o < batchSize; o++ {
		out = append(out, identity.LogicalRequest{Cohort: req.Cohort, Ordinal: identity.Ordinal(o)}.UID())
	}
	return out
}

// splitByEmitter distributes one request's timestamps to the processes that own
// them, producing the same multi-process evidence a real run produces.
func (s Spec) splitByEmitter(
	req identity.LogicalRequest,
	ts map[events.Stage]int64,
	envelope identity.EnvelopeID,
	observedSkew int64,
	membership []identity.UID,
	batchSize int,
) []events.Decoded {
	emitters := []identity.Emitter{
		identity.EmitterLoadGen, identity.EmitterProxy,
		identity.EmitterAdapter, identity.EmitterTriton,
	}

	var out []events.Decoded
	for i, emitter := range emitters {
		owned := events.OwnedStages(s.Cell, emitter)
		if owned == 0 {
			continue
		}

		rec := events.Record{
			Emitter: emitter,
			Seq:     uint64(i + 1),
			Cohort:  req.Cohort,
			Ordinal: req.Ordinal,
			Status:  events.StatusOK,
		}
		// The envelope id rides with the processes that handled the envelope. It
		// is what the cardinality check counts: one per cohort at A=on, one per
		// member at A=off, and a fixture that left it zero would make those two
		// indistinguishable.
		if emitter == identity.EmitterProxy || emitter == identity.EmitterAdapter {
			rec.EnvelopeID = envelope
		}
		// The adapter is where the fan-out is observed, so it is the only emitter
		// that carries dispatch evidence — the same values for every member of one
		// release, because the evidence describes the release and not the member.
		if emitter == identity.EmitterAdapter && s.Cell.UsesProxy() {
			reported := observedSkew
			if s.ReportedSkewNanos != nil {
				reported = *s.ReportedSkewNanos
			}
			rec.SetDispatch(events.DispatchEvidence{
				Dispatched: uint32(s.dispatchedPerFanOut()),
				SkewNanos:  reported,
				CPUNanos:   int64(s.Delay(validate.StageAdapterUnpack)),
				// Whole-process CPU sampled around a dispatch. It is archived with its
				// scope because the number alone is not interpretable, and at this
				// scope it may not cross a Factor A level at all.
				CPUScope: events.CPUScopeProcess,
			})
		}
		var carried bool
		for _, stage := range owned.Stages() {
			if v, ok := ts[stage]; ok {
				rec.SetStage(stage, v)
				carried = true
			}
		}
		if !carried {
			continue
		}
		// Membership rides with the processes that actually received the model's
		// attestation: the load generator on every path, and the adapter as well
		// on the shared one. Recording both is what gives the validator two
		// independent observations to compare.
		if emitter == identity.EmitterLoadGen || (emitter == identity.EmitterAdapter && s.Cell.UsesProxy()) {
			rec.SetMembership(membership)
			rec.BatchSize = uint32(batchSize)
		}

		out = append(out, events.Decoded{
			SchemaVersion: events.SchemaVersion,
			Header: events.RunHeader{
				Experiment:  "exp-fixture",
				Session:     "sess-fixture",
				Run:         s.Run,
				Cell:        s.Cell,
				ClockDomain: s.ClockDomain,
				WriterID:    identity.WriterID(i + 1),
			},
			Record: rec,
		})
	}
	return out
}

// checkGranularity verifies that the bundle carries evidence at the granularity
// the cell's own path would have produced it at.
//
// It runs on every fixture the builder produces, before any test sees one. The
// assertions that judge an A=on bundle are only as good as the evidence they are
// tested against, and a fixture that quietly gave each member its own seal would
// let all of them pass while describing a run no proxy could have performed. The
// check is deliberately about what a cohort shares rather than about the values
// themselves: it is the shape that carries the claim.
func (b Bundle) checkGranularity() error {
	if !b.Spec.Cell.AggregatesEnvelopes() {
		return nil
	}

	shared := validate.EnvelopeStages().Stages()
	for c := 0; c < b.Spec.CohortCount; c++ {
		cohort := b.Spec.FirstCohortID + identity.CohortID(c)

		envelopes := map[identity.EnvelopeID]bool{}
		values := map[events.Stage]map[int64]bool{}
		var windows map[[2]int64]bool
		if b.Spec.Cell.VectorizesCompute() {
			windows = map[[2]int64]bool{}
		}

		for _, d := range b.Records {
			rec := d.Record
			if rec.Cohort != cohort {
				continue
			}
			if rec.EnvelopeID != 0 {
				envelopes[rec.EnvelopeID] = true
			}
			for _, stage := range shared {
				if v, ok := rec.Stage(stage); ok {
					if values[stage] == nil {
						values[stage] = map[int64]bool{}
					}
					values[stage][v] = true
				}
			}
			if windows != nil {
				start, okStart := rec.Stage(events.StageComputeStart)
				end, okEnd := rec.Stage(events.StageComputeEnd)
				if okStart && okEnd {
					windows[[2]int64{start, end}] = true
				}
			}
		}

		if len(envelopes) != 1 {
			return fmt.Errorf(
				"testkit: cohort %d travelled in %d envelopes; at A=on one envelope carries the cohort, and a fixture of B envelopes is the fake the cardinality check exists to catch",
				cohort, len(envelopes))
		}
		for _, stage := range shared {
			if n := len(values[stage]); n > 1 {
				return fmt.Errorf(
					"testkit: cohort %d carries %d distinct values of %s; it describes the envelope, so its members share one",
					cohort, n, stage)
			}
		}
		if windows != nil && len(windows) != 1 {
			return fmt.Errorf(
				"testkit: cohort %d has %d distinct execution windows; at V=on the cohort is one execution and its members share one window",
				cohort, len(windows))
		}
	}
	return nil
}

// Clone returns a deep copy so a defect can be planted without disturbing the
// bundle it was derived from.
func (b Bundle) Clone() Bundle {
	records := make([]events.Decoded, len(b.Records))
	copy(records, b.Records)

	exp := b.Expectation
	histogram := make(map[uint64]uint64, len(exp.BatchSizeHistogram))
	for k, v := range exp.BatchSizeHistogram {
		histogram[k] = v
	}
	exp.BatchSizeHistogram = histogram

	return Bundle{Spec: b.Spec, Records: records, Expectation: exp}
}

// find locates the record a defect should be planted in.
func (b *Bundle) find(emitter identity.Emitter, req identity.LogicalRequest) (int, error) {
	for i, d := range b.Records {
		if d.Record.Emitter == emitter && d.Record.Request() == req {
			return i, nil
		}
	}
	return 0, fmt.Errorf("testkit: no %s record for %v", emitter, req)
}

// FirstRequest is the request defects are planted in by default.
func (b Bundle) FirstRequest() identity.LogicalRequest {
	return identity.LogicalRequest{Cohort: b.Spec.FirstCohortID, Ordinal: 0}
}
