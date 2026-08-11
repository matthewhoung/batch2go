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

// FanOutReport is what the adapter's releases looked like, read back from the
// archive rather than taken from the adapter's word for it.
type FanOutReport struct {
	Releases []Release `json:"releases,omitempty"`

	// MaxSkewNanos is the widest observed first-to-last submit across the run,
	// and BoundNanos what the manifest declared. Both are here so a reader sees
	// the margin rather than only the verdict on it.
	MaxSkewNanos int64 `json:"max_skew_nanos"`
	BoundNanos   int64 `json:"bound_nanos"`
}

// Release is one fan-out: the members the adapter submitted in a single call.
type Release struct {
	Envelope identity.EnvelopeID `json:"envelope_id"`
	Members  int                 `json:"members"`

	// ObservedSkewNanos is first-to-last submit computed from the dispatch
	// timestamps in the records, and ReportedSkewNanos is what the adapter said
	// it was. They come from the same instants in a correct run, so a
	// disagreement means one of the two is not describing this release.
	ObservedSkewNanos int64 `json:"observed_skew_nanos"`
	ReportedSkewNanos int64 `json:"reported_skew_nanos"`

	// DeclaredMembers is the count the adapter attached to the evidence.
	DeclaredMembers uint32 `json:"declared_members"`

	// Overlapped says every member was still in flight when the next was
	// submitted. A release whose members did not overlap was a sequence of
	// submissions wearing the shape of one call.
	Overlapped bool `json:"overlapped"`

	// CPUScope is what the adapter's CPU number counted. It travels with the
	// release because the value is not interpretable without it.
	CPUScope string `json:"cpu_scope,omitempty"`
	CPUNanos int64  `json:"cpu_nanos,omitempty"`
}

// checkFanOut judges the release the adapter says it performed.
//
// The adapter's own account is evidence, not testimony to be taken on trust: it
// reports a skew, and the same dispatch instants that produced that number are
// in the records beside it. So the number is checked against them. An adapter
// whose arithmetic — or whose clock — disagreed with its own timestamps would
// otherwise be believed, and the one quantity that separates a concurrent
// release from a serial one would be self-certified.
//
// Grouping is by envelope, not by cohort. One envelope is one call into the
// executor, which is what a fan-out is; at A=off a cohort is B separate calls,
// and grouping those by cohort would compute a "skew" across releases that never
// happened together.
func checkFanOut(exp Expectation, joined map[identity.LogicalRequest]*Joined) (FanOutReport, Check) {
	report := FanOutReport{BoundNanos: exp.MaxDispatchSkewNanos}

	topology, err := events.TopologyMask(exp.Cell)
	if err != nil {
		return report, Check{Name: "fan_out", Passed: false, Detail: err.Error()}
	}
	if !topology.Has(events.StageAdapterDispatch) {
		return report, pass("fan_out", fmt.Sprintf("%s has no adapter", exp.Cell))
	}

	// The bound is a manifest constant with no code default, and the manifest
	// already refuses an A=on cell that does not declare one. A zero arriving here
	// therefore means the bound was lost between the manifest and the validator,
	// and skipping the check would be the silent pass this whole package exists to
	// prevent — so it is a defect rather than a reason to stop looking.
	if exp.Cell.AggregatesEnvelopes() && exp.MaxDispatchSkewNanos <= 0 {
		return report, fail("fan_out", []Defect{{
			Kind: DefectDispatchSkew,
			Message: fmt.Sprintf(
				"%s releases its cohort in one fan-out but no skew bound reached the validator; the manifest declares one, so this run cannot be judged on the quantity that separates a concurrent release from a serial one",
				exp.Cell),
		}}, "no bound")
	}

	byEnvelope := map[identity.EnvelopeID][]*Joined{}
	var defects []Defect
	for _, req := range sortedRequests(joined) {
		j := joined[req]
		if j.EnvelopeID == 0 {
			continue // no envelope to attribute a release to
		}
		// Only the adapter observes a fan-out. Anything else claiming one is
		// reporting on a call it was not in.
		for emitter := range j.DispatchBySource {
			if emitter != identity.EmitterAdapter {
				defects = append(defects, Defect{
					Kind:    DefectDispatchEvidence,
					Request: requestPtr(req),
					Message: fmt.Sprintf("%s carries fan-out evidence; only the adapter performs one", emitter),
				})
			}
		}
		byEnvelope[j.EnvelopeID] = append(byEnvelope[j.EnvelopeID], j)
	}

	envelopes := make([]identity.EnvelopeID, 0, len(byEnvelope))
	for id := range byEnvelope {
		envelopes = append(envelopes, id)
	}
	sort.Slice(envelopes, func(i, j int) bool { return envelopes[i] < envelopes[j] })

	for _, id := range envelopes {
		members := byEnvelope[id]
		rel, relDefects := judgeRelease(exp, id, members)
		report.Releases = append(report.Releases, rel)
		defects = append(defects, relDefects...)
		if rel.ObservedSkewNanos > report.MaxSkewNanos {
			report.MaxSkewNanos = rel.ObservedSkewNanos
		}
	}

	detail := fmt.Sprintf("%d releases, widest observed skew %dns against a %dns bound",
		len(report.Releases), report.MaxSkewNanos, report.BoundNanos)
	return report, fail("fan_out", defects, detail)
}

// judgeRelease reads one fan-out out of its members' records.
func judgeRelease(exp Expectation, id identity.EnvelopeID, members []*Joined) (Release, []Defect) {
	rel := Release{Envelope: id, Members: len(members)}
	var defects []Defect

	var evidence events.DispatchEvidence
	var haveEvidence bool
	for _, m := range members {
		if e, ok := m.DispatchBySource[identity.EmitterAdapter]; ok {
			evidence, haveEvidence = e, true
			break
		}
	}
	if haveEvidence {
		rel.ReportedSkewNanos = evidence.SkewNanos
		rel.DeclaredMembers = evidence.Dispatched
		rel.CPUNanos = evidence.CPUNanos
		rel.CPUScope = evidence.CPUScope.String()
	}

	rel.ObservedSkewNanos = spread(members, events.StageAdapterDispatch)
	rel.Overlapped = submissionsOverlapped(members)

	if !haveEvidence {
		defects = append(defects, Defect{
			Kind:    DefectDispatchEvidence,
			Message: fmt.Sprintf("envelope %d carries no fan-out evidence; the adapter records it for every member it releases", id),
		})
		return rel, defects
	}

	if int(rel.DeclaredMembers) != len(members) {
		defects = append(defects, Defect{
			Kind: DefectDispatchEvidence,
			Message: fmt.Sprintf("envelope %d: the adapter declared it released %d members, %d carry its evidence",
				id, rel.DeclaredMembers, len(members)),
		})
	}

	// The adapter's skew and the recorded dispatch instants are the same numbers
	// in a correct run — the executor takes one clock reading per member and both
	// come from it. A difference is not a rounding artifact; it means one of the
	// two is describing something else.
	if rel.ReportedSkewNanos != rel.ObservedSkewNanos {
		defects = append(defects, Defect{
			Kind: DefectDispatchEvidence,
			Message: fmt.Sprintf(
				"envelope %d: the adapter reported a skew of %dns, its own dispatch timestamps span %dns; both come from the same instants, so they cannot disagree",
				id, rel.ReportedSkewNanos, rel.ObservedSkewNanos),
		})
	}

	if exp.MaxDispatchSkewNanos > 0 && rel.ObservedSkewNanos > exp.MaxDispatchSkewNanos {
		defects = append(defects, Defect{
			Kind: DefectDispatchSkew,
			Message: fmt.Sprintf(
				"envelope %d: its %d members were submitted over %dns, bound is %dns; the claim the bound stands for is that they reached the backend together",
				id, len(members), rel.ObservedSkewNanos, exp.MaxDispatchSkewNanos),
		})
	}

	// A skew inside the bound is not the same finding as members that overlapped.
	// A release could submit its members quickly and still have each one complete
	// before the next began, which is a sequence of submissions wearing the shape
	// of one call — and it would satisfy a skew bound set generously enough.
	if len(members) > 1 && !rel.Overlapped {
		defects = append(defects, Defect{
			Kind: DefectFanOutSerial,
			Message: fmt.Sprintf(
				"envelope %d: its %d members did not overlap — each was submitted only after the previous one had returned, which is a sequence of releases and not a fan-out",
				id, len(members)),
		})
	}
	return rel, defects
}

// submissionsOverlapped reports whether the members of one release were in
// flight at the same time.
//
// A member's window runs from its submission to its result. Sorted by
// submission, a release overlapped when every member was submitted before its
// predecessor came back. This is the same walk the execution-serialization check
// performs, at the opposite polarity: there, overlap is the fault, because B
// executions share one model instance; here the absence of overlap is, because
// the whole point of a fan-out is that its members go together.
func submissionsOverlapped(members []*Joined) bool {
	type window struct{ start, end int64 }
	windows := make([]window, 0, len(members))
	for _, m := range members {
		start, okStart := m.Stage(events.StageAdapterDispatch)
		end, okEnd := m.Stage(events.StageAdapterResult)
		if !okStart || !okEnd {
			continue
		}
		windows = append(windows, window{start, end})
	}
	if len(windows) < 2 {
		// One member is a release of one, and there is nothing to overlap with. It
		// is reported as overlapped because the alternative — calling it serial —
		// would make every A=off envelope a defect.
		return true
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i].start < windows[j].start })
	for i := 1; i < len(windows); i++ {
		if windows[i].start >= windows[i-1].end {
			return false
		}
	}
	return true
}

// AdapterCPUAcrossFactorA is the comparison the adapter's CPU numbers are NOT
// admitted to, expressed as the function that refuses it.
//
// The restriction is real and it is easy to forget, because the two numbers are
// both called "the adapter's CPU for a dispatch" and subtracting them looks like
// measuring what aggregation cost. At the process scope it is not: a cohort's B
// concurrent dispatches at A=off each attribute the whole process's CPU over
// their own overlapping windows, so the same work is counted B times, while at
// A=on one dispatch attributes it once. The difference would move with B for
// reasons that have nothing to do with envelope aggregation — exactly the
// treatment-correlated artifact this project measures GC and tracing overhead in
// order to bound.
//
// It lives here as a function rather than a comment because a comment cannot be
// called by mistake. Anything wanting the contrast has to come through this, and
// this refuses until a scope exists that counts one dispatch's work
// (events.CPUScopeDispatch), at which point the same call starts succeeding
// without any caller changing.
func AdapterCPUAcrossFactorA(a, b FanOutReport) (int64, error) {
	scopeA, err := scopeOf(a)
	if err != nil {
		return 0, err
	}
	scopeB, err := scopeOf(b)
	if err != nil {
		return 0, err
	}
	for _, s := range []events.CPUScope{scopeA, scopeB} {
		if !s.ComparableAcrossFactorA() {
			return 0, fmt.Errorf(
				"validate: adapter CPU at the %s scope may not cross a Factor A level: it counts the same work once per concurrent dispatch, so the difference would move with B for reasons unrelated to aggregation",
				s)
		}
	}
	return totalCPU(a) - totalCPU(b), nil
}

// scopeOf is the one scope a report's releases were measured at, refusing a
// report that mixes them — a run whose CPU numbers changed definition partway
// through is not a measurement of anything.
func scopeOf(r FanOutReport) (events.CPUScope, error) {
	var found events.CPUScope
	var seen bool
	for _, rel := range r.Releases {
		scope, err := events.ParseCPUScope(rel.CPUScope)
		if err != nil {
			return events.CPUScopeUnspecified, err
		}
		if seen && scope != found {
			return events.CPUScopeUnspecified, fmt.Errorf(
				"validate: this run's adapter CPU was measured at two scopes, %s and %s; a quantity whose definition changed mid-run is not a measurement of the run",
				found, scope)
		}
		found, seen = scope, true
	}
	if !seen {
		return events.CPUScopeUnspecified, fmt.Errorf("validate: no fan-out carried a CPU scope")
	}
	return found, nil
}

func totalCPU(r FanOutReport) int64 {
	var total int64
	for _, rel := range r.Releases {
		total += rel.CPUNanos
	}
	return total
}
