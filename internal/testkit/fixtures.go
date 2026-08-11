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

	ToleranceFraction float64
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
	for c := 0; c < s.CohortCount; c++ {
		cohortID := s.FirstCohortID + identity.CohortID(c)
		cohortStart := s.BaseInstant + int64(c)*int64(s.CohortGap)

		// prevComputeEnd carries the serialization forward: member i cannot begin
		// executing until member i-1 has finished on the one model instance.
		var prevComputeEnd int64
		for o := 0; o < s.CohortSize; o++ {
			req := identity.LogicalRequest{Cohort: cohortID, Ordinal: identity.Ordinal(o)}

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

			membership := s.membership(req, batchSize)
			records = append(records, s.splitByEmitter(req, ts, membership, batchSize)...)
		}
	}

	executions := s.CohortCount * executionsPerCohort
	return Bundle{
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
			ExecutionCountDelta:         uint64(executions),
			InferenceCountDelta:         uint64(s.CohortCount * s.CohortSize),
			BatchSizeHistogram:          map[uint64]uint64{uint64(batchSize): uint64(executions)},
		},
	}, nil
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
