package validate

import (
	"fmt"
	"sort"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// ConservationReport is the per-topology accounting of where a request's time
// went. Residuals are reported signed and are never relabeled as a stage: an
// unaccounted duration is a fact about the instrument, and renaming it would
// destroy the only signal that the decomposition is incomplete (M1 §4).
type ConservationReport struct {
	Cell     identity.Cell `json:"cell"`
	Additive bool          `json:"additive"`

	// Requests is the per-request accounting.
	Requests []RequestConservation `json:"requests"`

	// Stages summarizes each named stage across the run, which is what the
	// injected-delay self-test reads to check that the validator attributes a
	// known delay to the stage it was injected into.
	Stages []StageSummary `json:"stages"`

	// Cohorts is the interval accounting for multi-RPC cells, where stages of
	// different requests legitimately overlap and are never summed.
	Cohorts []CohortConservation `json:"cohorts"`

	MaxAbsResidualNanos      int64   `json:"max_abs_residual_nanos"`
	MaxAbsResidualFraction   float64 `json:"max_abs_residual_fraction"`
	ToleranceFraction        float64 `json:"tolerance_fraction"`
	MaxUncoveredMakespanFrac float64 `json:"max_uncovered_makespan_fraction"`
}

// RequestConservation is one request's decomposition.
type RequestConservation struct {
	Request        identity.LogicalRequest `json:"request"`
	EndToEndNanos  int64                   `json:"end_to_end_nanos"`
	AccountedNanos int64                   `json:"accounted_nanos"`

	// UnaccountedNanos is the time inside the path that the cycle model does not
	// name — the adapter's own unpack and response-pack cost, the load
	// generator's dispatch. It is measured, not estimated.
	UnaccountedNanos int64 `json:"unaccounted_nanos"`

	// ResidualNanos is end-to-end minus the cycle model's stages, signed.
	ResidualNanos    int64            `json:"residual_nanos"`
	ResidualFraction float64          `json:"residual_fraction"`
	Stages           map[string]int64 `json:"stages"`
}

// StageSummary aggregates one named stage across the run.
type StageSummary struct {
	Name        string `json:"name"`
	Count       int    `json:"count"`
	TotalNanos  int64  `json:"total_nanos"`
	MinNanos    int64  `json:"min_nanos"`
	MaxNanos    int64  `json:"max_nanos"`
	MedianNanos int64  `json:"median_nanos"`
}

// CohortConservation is the interval accounting for one cohort.
//
// For multi-RPC cells the per-cohort cycle is first release to last completion,
// and the B requests genuinely overlap — so their durations are never summed.
// What is checked instead is coverage: the union of the members' in-flight
// intervals must account for the cohort's makespan, and any uncovered stretch is
// time when the cohort was in flight and nothing was recorded as happening.
type CohortConservation struct {
	Cohort          identity.CohortID `json:"cohort_id"`
	MakespanNanos   int64             `json:"makespan_nanos"`
	CoveredNanos    int64             `json:"covered_nanos"`
	UncoveredNanos  int64             `json:"uncovered_nanos"`
	UncoveredFrac   float64           `json:"uncovered_fraction"`
	SumOfSpansNanos int64             `json:"sum_of_member_spans_nanos"`

	// Stages is the cohort's own decomposition, present only where its members
	// share an envelope. Each named stage appears once, whatever its granularity:
	// a cost the cohort paid once is counted once, where the per-member sum of an
	// envelope-granularity stage would report it B times.
	Stages map[string]int64 `json:"stages,omitempty"`

	AccountedNanos   int64 `json:"accounted_nanos,omitempty"`
	UnaccountedNanos int64 `json:"unaccounted_nanos,omitempty"`

	// ResidualNanos is the cohort's makespan minus the stages the cycle model
	// names, signed. Where a cohort shares an envelope this is the residual the
	// run is gated on, because a member's own residual there is not a measure of
	// unexplained time — see checkConservation.
	ResidualNanos    int64   `json:"residual_nanos,omitempty"`
	ResidualFraction float64 `json:"residual_fraction,omitempty"`

	// FormationWaitNanos is W_form at cohort granularity: the earliest arrival to
	// the seal. It is the part of formation that lies on the cohort's critical
	// path, because the cohort's clock has been running since its first member
	// got there.
	//
	// It is NOT an independent observation of the per-member values summarized
	// elsewhere. It is identically the largest of them — the member that arrived
	// first is the member that waited longest — so the two agreeing establishes
	// nothing that either one did not already say, and a reader who took their
	// agreement for corroboration would be counting one measurement twice.
	FormationWaitNanos int64 `json:"formation_wait_nanos,omitempty"`
}

func checkConservation(exp Expectation, joined map[identity.LogicalRequest]*Joined) (ConservationReport, Check) {
	report := ConservationReport{
		Cell:              exp.Cell,
		Additive:          AdditiveConservation(exp.Cell),
		ToleranceFraction: exp.ToleranceFraction,
	}

	spans, err := Chain(exp.Cell)
	if err != nil {
		return report, Check{Name: "conservation", Passed: false, Detail: err.Error()}
	}
	first, last := ConservedSpan()

	var defects []Defect
	samples := map[string][]int64{}

	for _, req := range sortedRequests(joined) {
		j := joined[req]
		start, okStart := j.Stage(first)
		end, okEnd := j.Stage(last)
		if !okStart || !okEnd {
			continue // the presence check already named the missing timestamp
		}

		rc := RequestConservation{
			Request:       req,
			EndToEndNanos: end - start,
			Stages:        make(map[string]int64, len(spans)),
		}
		complete := true
		for i, span := range spans {
			a, okA := j.Stage(span.Start)
			b, okB := j.Stage(span.End)
			if !okA || !okB {
				complete = false
				continue
			}
			d := b - a
			rc.Stages[span.Name] = d
			samples[span.Name] = append(samples[span.Name], d)

			// Stages before the client send are measured and reported but are not
			// part of the identity, which is stated over t15 − t2.
			if PreCycle(spans, i) {
				continue
			}
			if span.Accounted {
				rc.AccountedNanos += d
			} else {
				rc.UnaccountedNanos += d
			}
		}
		if !complete {
			continue
		}

		rc.ResidualNanos = rc.EndToEndNanos - rc.AccountedNanos
		if rc.EndToEndNanos > 0 {
			rc.ResidualFraction = float64(rc.ResidualNanos) / float64(rc.EndToEndNanos)
		}
		report.Requests = append(report.Requests, rc)

		if abs64(rc.ResidualNanos) > report.MaxAbsResidualNanos {
			report.MaxAbsResidualNanos = abs64(rc.ResidualNanos)
		}
		if absF(rc.ResidualFraction) > report.MaxAbsResidualFraction {
			report.MaxAbsResidualFraction = absF(rc.ResidualFraction)
		}
		// Where a cohort shares an envelope, a member's own residual is not a
		// measure of unexplained time and is reported without being gated. A member
		// that finished early waits for the rest of its cohort before the one
		// response comes back, and that wait lands in its response-pack interval —
		// time the cohort really spent, already accounted at cohort level as
		// another member's execution. Gating it per member would fail every correct
		// F10 run by a margin that grows with B. The cohort residual below is what
		// this cell is judged on (ADR-0009).
		if gateOnMembers(exp.Cell) && absF(rc.ResidualFraction) > exp.ToleranceFraction {
			defects = append(defects, Defect{
				Kind:    DefectConservation,
				Request: requestPtr(req),
				Message: fmt.Sprintf("unaccounted %+dns is %.2f%% of a %dns path, tolerance %.2f%%",
					rc.ResidualNanos, rc.ResidualFraction*100, rc.EndToEndNanos, exp.ToleranceFraction*100),
			})
		}
	}

	report.Stages = summarize(spans, samples)

	// Cohort intervals are measured and reported, but coverage is NOT a pass
	// criterion. Under fixed-cohort release the members' in-flight intervals
	// always overlap, so their union is one contiguous span identically equal to
	// the makespan: uncovered time was zero in every cohort of every run ever
	// made, whatever the input. The cohort-level test that can fail is the
	// execution-serialization check; what is reported here is the overlap
	// structure itself, which is informative and makes no claim.
	report.Cohorts = cohortCoverage(exp, joined, first, last)
	for _, c := range report.Cohorts {
		if c.UncoveredFrac > report.MaxUncoveredMakespanFrac {
			report.MaxUncoveredMakespanFrac = c.UncoveredFrac
		}
	}

	detail := fmt.Sprintf("max |residual| %.4f%% of path, tolerance %.2f%% (cohort intervals reported, not gated)",
		report.MaxAbsResidualFraction*100, exp.ToleranceFraction*100)

	if !gateOnMembers(exp.Cell) {
		var maxCohortFrac float64
		decomposeCohorts(exp, joined, spans, report.Cohorts)
		for i := range report.Cohorts {
			c := &report.Cohorts[i]
			if absF(c.ResidualFraction) > maxCohortFrac {
				maxCohortFrac = absF(c.ResidualFraction)
			}
			if absF(c.ResidualFraction) > exp.ToleranceFraction {
				defects = append(defects, Defect{
					Kind: DefectConservation,
					Message: fmt.Sprintf("cohort %d: unaccounted %+dns is %.2f%% of a %dns makespan, tolerance %.2f%%",
						c.Cohort, c.ResidualNanos, c.ResidualFraction*100, c.MakespanNanos, exp.ToleranceFraction*100),
				})
			}
		}
		detail = fmt.Sprintf(
			"max |cohort residual| %.4f%% of makespan, tolerance %.2f%% (per-member residuals reported, not gated: a member's wait for its cohort is another member's execution)",
			maxCohortFrac*100, exp.ToleranceFraction*100)
	}
	return report, fail("conservation", defects, detail)
}

// gateOnMembers reports whether a cell's conservation identity holds per request.
//
// It does wherever a request's own path is the whole story: at A=off each member
// has its own envelope end to end, so the time between its send and its
// completion is time spent on its behalf. Where a cohort shares one envelope the
// identity moves to the cohort, because the members' paths merge at the seal and
// again at the response — and between those two points a member is waiting for
// work being done for somebody else (ADR-0009).
func gateOnMembers(cell identity.Cell) bool { return !cell.AggregatesEnvelopes() }

// decomposeCohorts fills in each cohort's own decomposition, in place.
//
// The cohort's chain is the same chain a member walks, evaluated at the instants
// that lie on the cohort's critical path rather than on any one member's. Those
// instants tile the makespan exactly, so what is left over is the time the cycle
// model does not name — and nothing else.
func decomposeCohorts(
	exp Expectation,
	joined map[identity.LogicalRequest]*Joined,
	spans []Span,
	cohorts []CohortConservation,
) {
	members := map[identity.CohortID][]*Joined{}
	for _, req := range sortedRequests(joined) {
		members[req.Cohort] = append(members[req.Cohort], joined[req])
	}

	for i := range cohorts {
		c := &cohorts[i]
		instants, ok := cohortInstants(members[c.Cohort])
		if !ok {
			continue // the presence check already named what is missing
		}

		c.Stages = make(map[string]int64, len(spans))
		for j, span := range spans {
			start, okStart := instants[span.Start]
			end, okEnd := instants[span.End]
			if !okStart || !okEnd {
				continue
			}
			d := end - start
			c.Stages[span.Name] = d
			if span.Name == StageWForm {
				c.FormationWaitNanos = d
			}
			if PreCycle(spans, j) {
				continue
			}
			if span.Accounted {
				c.AccountedNanos += d
			} else {
				c.UnaccountedNanos += d
			}
		}

		c.ResidualNanos = c.MakespanNanos - c.AccountedNanos
		if c.MakespanNanos > 0 {
			c.ResidualFraction = float64(c.ResidualNanos) / float64(c.MakespanNanos)
		}
	}
}

// cohortInstants picks, for each stage, the member observation that lies on the
// cohort's own critical path.
//
// Before the backend the cohort is paced by its earliest member: its clock has
// been running since the first request was sent, formation begins at the first
// arrival (ADR-0010), and the fan-out begins at the first submission. From the
// end of execution onward it is paced by its latest: the backend is done when
// the last execution is done, and the cohort is complete when its last member
// is. The stages the whole cohort shares have one value and are unaffected by
// the choice.
//
// The two rules meet inside the execution window, which is exactly where a
// cohort's work fans out into B paths and then reconverges. So the cohort's
// S_comp is the whole window the backend was busy for, and the queueing of later
// members behind earlier ones lies inside it rather than beside it — which is
// what it means for this cell to be interval-accounted (ADR-0009).
func cohortInstants(members []*Joined) (map[events.Stage]int64, bool) {
	if len(members) == 0 {
		return nil, false
	}
	out := make(map[events.Stage]int64, events.StageCount)
	for _, stage := range events.AllStages() {
		var value int64
		var seen bool
		for _, m := range members {
			v, ok := m.Stage(stage)
			if !ok {
				continue
			}
			switch {
			case !seen:
				value, seen = v, true
			case pacedByLastMember(stage) && v > value:
				value = v
			case !pacedByLastMember(stage) && v < value:
				value = v
			}
		}
		if seen {
			out[stage] = value
		}
	}
	return out, true
}

// pacedByLastMember reports whether a cohort reaches a stage when its last
// member does, rather than when its first does.
func pacedByLastMember(s events.Stage) bool {
	switch s {
	case events.StageComputeEnd, events.StageAdapterResult,
		events.StageAdapterSend, events.StageProxyRespRecv,
		events.StageProxyFanout, events.StageClientRecv:
		return true
	default:
		return false
	}
}

// cohortCoverage computes the interval accounting for each cohort.
func cohortCoverage(
	exp Expectation,
	joined map[identity.LogicalRequest]*Joined,
	first, last events.Stage,
) []CohortConservation {
	type interval struct{ start, end int64 }
	byCohort := map[identity.CohortID][]interval{}

	for _, req := range sortedRequests(joined) {
		j := joined[req]
		start, okStart := j.Stage(first)
		end, okEnd := j.Stage(last)
		if !okStart || !okEnd || end < start {
			continue
		}
		byCohort[req.Cohort] = append(byCohort[req.Cohort], interval{start, end})
	}

	cohorts := make([]identity.CohortID, 0, len(byCohort))
	for c := range byCohort {
		cohorts = append(cohorts, c)
	}
	sort.Slice(cohorts, func(i, j int) bool { return cohorts[i] < cohorts[j] })

	out := make([]CohortConservation, 0, len(cohorts))
	for _, cohort := range cohorts {
		spans := byCohort[cohort]
		sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })

		makespanStart, makespanEnd := spans[0].start, spans[0].end
		var sumOfSpans int64
		for _, s := range spans {
			if s.end > makespanEnd {
				makespanEnd = s.end
			}
			sumOfSpans += s.end - s.start
		}

		// Union of overlapping intervals: this is the step that refuses to
		// double-count concurrent members.
		var covered int64
		curStart, curEnd := spans[0].start, spans[0].end
		for _, s := range spans[1:] {
			if s.start > curEnd {
				covered += curEnd - curStart
				curStart, curEnd = s.start, s.end
				continue
			}
			if s.end > curEnd {
				curEnd = s.end
			}
		}
		covered += curEnd - curStart

		c := CohortConservation{
			Cohort:          cohort,
			MakespanNanos:   makespanEnd - makespanStart,
			CoveredNanos:    covered,
			SumOfSpansNanos: sumOfSpans,
		}
		c.UncoveredNanos = c.MakespanNanos - c.CoveredNanos
		if c.MakespanNanos > 0 {
			c.UncoveredFrac = float64(c.UncoveredNanos) / float64(c.MakespanNanos)
		}
		out = append(out, c)
	}
	return out
}

func summarize(spans []Span, samples map[string][]int64) []StageSummary {
	out := make([]StageSummary, 0, len(spans))
	for _, span := range spans {
		values := samples[span.Name]
		if len(values) == 0 {
			out = append(out, StageSummary{Name: span.Name})
			continue
		}
		sorted := append([]int64(nil), values...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		s := StageSummary{
			Name:        span.Name,
			Count:       len(sorted),
			MinNanos:    sorted[0],
			MaxNanos:    sorted[len(sorted)-1],
			MedianNanos: sorted[len(sorted)/2],
		}
		for _, v := range sorted {
			s.TotalNanos += v
		}
		out = append(out, s)
	}
	return out
}

// StageSummary looks up one stage's summary by name.
func (r ConservationReport) StageSummary(name string) (StageSummary, bool) {
	for _, s := range r.Stages {
		if s.Name == name {
			return s, true
		}
	}
	return StageSummary{}, false
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func absF(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
