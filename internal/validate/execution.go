package validate

import (
	"fmt"
	"sort"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// ExecutionReport is the cohort-level account of how a cohort's model executions
// were laid out in time.
type ExecutionReport struct {
	// Serialized is whether the cell declares that its executions run one at a
	// time on a single model instance. Only then is the overlap check meaningful.
	Serialized bool `json:"serialized_by_declaration"`

	Cohorts []CohortExecutions `json:"cohorts"`

	// MinGapNanos is the smallest interval between consecutive executions across
	// the run, and OverlapCount how many consecutive pairs overlapped at all.
	// GapsMeasured says whether any non-overlapping pair existed to measure: when
	// every pair overlaps there is no smallest gap, and reporting one would be
	// inventing a number on exactly the path where the check has failed.
	MinGapNanos    int64 `json:"min_gap_nanos"`
	GapsMeasured   int   `json:"gaps_measured"`
	OverlapCount   int   `json:"overlap_count"`
	ExecutionPairs int   `json:"execution_pairs"`
}

// CohortExecutions is one cohort's execution layout.
type CohortExecutions struct {
	Cohort       identity.CohortID `json:"cohort_id"`
	Executions   int               `json:"executions"`
	Overlaps     int               `json:"overlapping_pairs"`
	GapsMeasured int               `json:"gaps_measured"`
	MinGapNanos  int64             `json:"min_gap_nanos"`
	SpanNanos    int64             `json:"span_nanos"`
	ComputeNanos int64             `json:"total_compute_nanos"`
}

// checkExecutionSerialization is the cohort-level check that can actually fail.
//
// It replaces an interval-coverage test that could not. That test unioned each
// member's in-flight interval and compared the union against the cohort's
// makespan; under fixed-cohort release those intervals always overlap, so the
// union is one contiguous span identically equal to the makespan, and uncovered
// time was zero for every cohort of every run — a check that passes whatever it
// is shown is not a check.
//
// What this tests instead is the cell's own declared mechanism. M1 §2.2 states
// that in the B-execution cells the executions "serialize on the single model
// instance: requests 2..B genuinely wait behind earlier executions", and that is
// what licenses booking the wait in Q_backend. It is a falsifiable claim: a
// second model instance, or a scheduler that coalesced, would make executions
// overlap, and the cycle model's queue accounting would be describing something
// that did not happen.
func checkExecutionSerialization(exp Expectation, joined map[identity.LogicalRequest]*Joined) (ExecutionReport, Check) {
	report := ExecutionReport{Serialized: !exp.Cell.VectorizesCompute()}

	if !report.Serialized {
		// A V=on cohort executes once; there is nothing to serialize.
		return report, pass("execution_serialization",
			fmt.Sprintf("%s executes each cohort once; serialization does not apply", exp.Cell))
	}

	type window struct {
		req        identity.LogicalRequest
		start, end int64
	}
	byCohort := map[identity.CohortID][]window{}
	for _, req := range sortedRequests(joined) {
		j := joined[req]
		start, okStart := j.Stage(events.StageComputeStart)
		end, okEnd := j.Stage(events.StageComputeEnd)
		if !okStart || !okEnd {
			continue // the presence check already named this
		}
		byCohort[req.Cohort] = append(byCohort[req.Cohort], window{req, start, end})
	}

	cohorts := make([]identity.CohortID, 0, len(byCohort))
	for c := range byCohort {
		cohorts = append(cohorts, c)
	}
	sort.Slice(cohorts, func(i, j int) bool { return cohorts[i] < cohorts[j] })

	var defects []Defect

	for _, cohort := range cohorts {
		windows := byCohort[cohort]
		sort.Slice(windows, func(i, j int) bool { return windows[i].start < windows[j].start })

		ce := CohortExecutions{Cohort: cohort, Executions: len(windows)}
		for _, w := range windows {
			ce.ComputeNanos += w.end - w.start
		}
		if len(windows) > 0 {
			ce.SpanNanos = windows[len(windows)-1].end - windows[0].start
		}

		for i := 1; i < len(windows); i++ {
			prev, cur := windows[i-1], windows[i]
			gap := cur.start - prev.end
			report.ExecutionPairs++

			if gap < 0 {
				ce.Overlaps++
				report.OverlapCount++
				defects = append(defects, Defect{
					Kind:    DefectOverlappingExecutions,
					Request: requestPtr(cur.req),
					Message: fmt.Sprintf(
						"its execution began %dns before %v's ended; %s declares one model instance running executions one at a time, and the queue accounting books the wait on that basis",
						-gap, prev.req, exp.Cell),
				})
				continue
			}
			if ce.GapsMeasured == 0 || gap < ce.MinGapNanos {
				ce.MinGapNanos = gap
			}
			ce.GapsMeasured++
			if report.GapsMeasured == 0 || gap < report.MinGapNanos {
				report.MinGapNanos = gap
			}
			report.GapsMeasured++
		}
		report.Cohorts = append(report.Cohorts, ce)
	}

	smallest := "no non-overlapping pair"
	if report.GapsMeasured > 0 {
		smallest = fmt.Sprintf("smallest gap %dns", report.MinGapNanos)
	}
	detail := fmt.Sprintf("%d consecutive execution pairs, %d overlapping, %s",
		report.ExecutionPairs, report.OverlapCount, smallest)
	return report, fail("execution_serialization", defects, detail)
}
