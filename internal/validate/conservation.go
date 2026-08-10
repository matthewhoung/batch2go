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
		if absF(rc.ResidualFraction) > exp.ToleranceFraction {
			defects = append(defects, Defect{
				Kind:    DefectConservation,
				Request: requestPtr(req),
				Message: fmt.Sprintf("unaccounted %+dns is %.2f%% of a %dns path, tolerance %.2f%%",
					rc.ResidualNanos, rc.ResidualFraction*100, rc.EndToEndNanos, exp.ToleranceFraction*100),
			})
		}
	}

	report.Stages = summarize(spans, samples)
	report.Cohorts = cohortCoverage(exp, joined, first, last)
	for _, c := range report.Cohorts {
		if c.UncoveredFrac > report.MaxUncoveredMakespanFrac {
			report.MaxUncoveredMakespanFrac = c.UncoveredFrac
		}
		if c.UncoveredFrac > exp.ToleranceFraction {
			defects = append(defects, Defect{
				Kind: DefectConservation,
				Message: fmt.Sprintf("cohort %d: %dns of a %dns makespan (%.2f%%) is covered by no member's in-flight interval",
					c.Cohort, c.UncoveredNanos, c.MakespanNanos, c.UncoveredFrac*100),
			})
		}
	}

	detail := fmt.Sprintf("max |residual| %.4f%% of path, max uncovered makespan %.4f%%, tolerance %.2f%%",
		report.MaxAbsResidualFraction*100, report.MaxUncoveredMakespanFrac*100, exp.ToleranceFraction*100)
	return report, fail("conservation", defects, detail)
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
