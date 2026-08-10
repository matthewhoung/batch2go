package validate

import (
	"fmt"
	"sort"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// DispatchReport is the adapter's release behavior, measured per cohort.
type DispatchReport struct {
	// MaxWaitNanos is the largest gap any member spent between arriving at the
	// adapter and being dispatched.
	MaxWaitNanos    int64 `json:"max_wait_nanos"`
	MedianWaitNanos int64 `json:"median_wait_nanos"`

	Cohorts []CohortDispatch `json:"cohorts"`
}

// CohortDispatch compares how a cohort's members arrived at the adapter with how
// they left it.
type CohortDispatch struct {
	Cohort              identity.CohortID `json:"cohort_id"`
	ArrivalSpreadNanos  int64             `json:"arrival_spread_nanos"`
	DispatchSpreadNanos int64             `json:"dispatch_spread_nanos"`
	MaxWaitNanos        int64             `json:"max_wait_nanos"`
}

// checkAdapterDispatch establishes that the adapter did not wait at A=off.
//
// The absence of adapter-side joining is load-bearing: a cohort barrier here
// would inject formation wait into the OFF/OFF baseline and contaminate the
// aggregation effect, so the design forbids it (ADR-0001). "We did not implement
// one" is not evidence, so this reads the behavior back out of the records.
//
// The test that actually discriminates is the spread comparison. If the adapter
// dispatched each envelope on arrival, the members leave as unevenly as they
// arrived, and the dispatch spread tracks the arrival spread. If it joined them,
// they all leave together the moment the last one arrives — the dispatch spread
// collapses toward zero while the arrival spread stays whatever it was. A bound
// on the per-member wait alone would not catch that, because a join over
// closely-spaced arrivals produces short waits too.
func checkAdapterDispatch(exp Expectation, joined map[identity.LogicalRequest]*Joined) Check {
	topology, err := events.TopologyMask(exp.Cell)
	if err != nil {
		return Check{Name: "adapter_dispatch_on_arrival", Passed: false, Detail: err.Error()}
	}
	if !topology.Has(events.StageAdapterRecv) {
		return pass("adapter_dispatch_on_arrival", fmt.Sprintf("%s has no adapter", exp.Cell))
	}
	if exp.Cell.AggregatesEnvelopes() {
		// At A=on the proxy already sealed the cohort, so the adapter receives one
		// envelope and there is no arrival spread to compare against.
		return pass("adapter_dispatch_on_arrival", fmt.Sprintf("%s is A=on; the proxy owns cohort formation", exp.Cell))
	}

	byCohort := map[identity.CohortID][]*Joined{}
	var waits []int64
	var defects []Defect

	for _, req := range sortedRequests(joined) {
		j := joined[req]
		recv, okRecv := j.Stage(events.StageAdapterRecv)
		dispatch, okDispatch := j.Stage(events.StageAdapterDispatch)
		if !okRecv || !okDispatch {
			continue // the presence check already named this
		}
		wait := dispatch - recv
		waits = append(waits, wait)
		byCohort[req.Cohort] = append(byCohort[req.Cohort], j)

		if exp.MaxAdapterDispatchWaitNanos > 0 && wait > exp.MaxAdapterDispatchWaitNanos {
			defects = append(defects, Defect{
				Kind:    DefectAdapterWaiting,
				Request: requestPtr(req),
				Message: fmt.Sprintf("waited %dns between arriving at the adapter and being dispatched, bound is %dns",
					wait, exp.MaxAdapterDispatchWaitNanos),
			})
		}
	}

	report := DispatchReport{}
	if len(waits) > 0 {
		sorted := append([]int64(nil), waits...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		report.MedianWaitNanos = sorted[len(sorted)/2]
		report.MaxWaitNanos = sorted[len(sorted)-1]
	}

	cohorts := make([]identity.CohortID, 0, len(byCohort))
	for c := range byCohort {
		cohorts = append(cohorts, c)
	}
	sort.Slice(cohorts, func(i, j int) bool { return cohorts[i] < cohorts[j] })

	for _, cohort := range cohorts {
		members := byCohort[cohort]
		if len(members) < 2 {
			continue
		}
		cd := CohortDispatch{Cohort: cohort}
		cd.ArrivalSpreadNanos = spread(members, events.StageAdapterRecv)
		cd.DispatchSpreadNanos = spread(members, events.StageAdapterDispatch)
		for _, m := range members {
			recv, _ := m.Stage(events.StageAdapterRecv)
			dispatch, _ := m.Stage(events.StageAdapterDispatch)
			if w := dispatch - recv; w > cd.MaxWaitNanos {
				cd.MaxWaitNanos = w
			}
		}
		report.Cohorts = append(report.Cohorts, cd)

		// Only meaningful when the members really did arrive apart; below that the
		// two spreads are indistinguishable and the check would be noise.
		if cd.ArrivalSpreadNanos < minArrivalSpreadNanos {
			continue
		}
		if float64(cd.DispatchSpreadNanos) < collapsedSpreadFraction*float64(cd.ArrivalSpreadNanos) {
			defects = append(defects, Defect{
				Kind: DefectAdapterWaiting,
				Message: fmt.Sprintf(
					"cohort %d arrived over %dns but dispatched within %dns: the members left together, which is what joining looks like",
					cohort, cd.ArrivalSpreadNanos, cd.DispatchSpreadNanos),
			})
		}
	}

	check := fail("adapter_dispatch_on_arrival", defects,
		fmt.Sprintf("median wait %dns, max %dns over %d cohorts",
			report.MedianWaitNanos, report.MaxWaitNanos, len(report.Cohorts)))
	return check
}

const (
	// minArrivalSpreadNanos is the arrival spread below which the comparison
	// carries no information.
	minArrivalSpreadNanos = 20_000

	// collapsedSpreadFraction is how far the dispatch spread may fall below the
	// arrival spread before it reads as the members having been released together.
	collapsedSpreadFraction = 0.25
)

func spread(members []*Joined, stage events.Stage) int64 {
	var lo, hi int64
	var seen bool
	for _, m := range members {
		ts, ok := m.Stage(stage)
		if !ok {
			continue
		}
		if !seen {
			lo, hi, seen = ts, ts, true
			continue
		}
		if ts < lo {
			lo = ts
		}
		if ts > hi {
			hi = ts
		}
	}
	if !seen {
		return 0
	}
	return hi - lo
}
