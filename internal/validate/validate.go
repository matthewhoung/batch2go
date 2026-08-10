package validate

import (
	"fmt"
	"sort"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// DefectKind names what went wrong. A verdict that fails must say which of these
// it found: "the run failed" is not a usable finding.
type DefectKind string

const (
	DefectMissingTimestamp      DefectKind = "missing_timestamp"
	DefectUnexpectedStage       DefectKind = "unexpected_stage_for_topology"
	DefectNonMonotonic          DefectKind = "non_monotonic_timestamps"
	DefectCrossClockDomain      DefectKind = "cross_clock_domain_subtraction"
	DefectMembershipMismatch    DefectKind = "membership_mismatch"
	DefectOwnUIDEcho            DefectKind = "own_uid_echo"
	DefectTruncatedEvidence     DefectKind = "truncated_membership_evidence"
	DefectCoalescedSingles      DefectKind = "coalesced_singles"
	DefectAdapterWaiting        DefectKind = "adapter_waiting_at_a_off"
	DefectStageOwnership        DefectKind = "stage_written_by_wrong_emitter"
	DefectOverlappingExecutions DefectKind = "executions_overlapped_on_one_instance"
	DefectMembershipDisagree    DefectKind = "membership_sources_disagree"
	DefectExecutionCount        DefectKind = "execution_count_mismatch"
	DefectDroppedRecords        DefectKind = "dropped_records"
	DefectMissingRequest        DefectKind = "missing_request"
	DefectDuplicateRecord       DefectKind = "duplicate_record"
	DefectConservation          DefectKind = "conservation_residual_exceeds_tolerance"
	DefectSchemaVersion         DefectKind = "schema_version_mismatch"
	DefectRequestFailed         DefectKind = "request_failed"
)

// Defect is one named finding, attached to the request it concerns where there
// is one.
type Defect struct {
	Kind    DefectKind               `json:"kind"`
	Request *identity.LogicalRequest `json:"request,omitempty"`
	Stage   string                   `json:"stage,omitempty"`
	Message string                   `json:"message"`
}

func (d Defect) String() string {
	if d.Request != nil {
		return fmt.Sprintf("%s [%v]: %s", d.Kind, *d.Request, d.Message)
	}
	return fmt.Sprintf("%s: %s", d.Kind, d.Message)
}

// Check is one named assertion's outcome.
type Check struct {
	Name    string   `json:"name"`
	Passed  bool     `json:"passed"`
	Detail  string   `json:"detail,omitempty"`
	Defects []Defect `json:"defects,omitempty"`
}

// Verdict is the validator's complete finding for one run. It is reproducible
// from an archived bundle alone, so re-running the validator on the archive must
// yield the same verdict.
type Verdict struct {
	Run    identity.RunID `json:"run_id"`
	Cell   identity.Cell  `json:"cell"`
	Passed bool           `json:"passed"`
	Checks []Check        `json:"checks"`

	// Conservation carries the residuals whether or not they passed. A residual
	// is reported signed and never relabeled (M1 §4).
	Conservation ConservationReport `json:"conservation"`

	// Executions is the cohort-level account of how the model executions were laid
	// out in time — the check that can fail where interval coverage could not.
	Executions ExecutionReport `json:"executions"`
}

// Defects flattens every finding, most structural first.
func (v Verdict) Defects() []Defect {
	var out []Defect
	for _, c := range v.Checks {
		out = append(out, c.Defects...)
	}
	return out
}

// HasDefect reports whether the verdict names a defect of this kind. It is how a
// defect fixture asserts that the validator caught the specific thing it planted.
func (v Verdict) HasDefect(kind DefectKind) bool {
	for _, d := range v.Defects() {
		if d.Kind == kind {
			return true
		}
	}
	return false
}

// Expectation is what the run was supposed to produce, as plain data. The caller
// translates a manifest and bundle into it; the validator never reads either.
type Expectation struct {
	Run         identity.RunID         `json:"run_id"`
	Cell        identity.Cell          `json:"cell"`
	ClockDomain identity.ClockDomainID `json:"clock_domain_id"`

	CohortSize    int               `json:"cohort_size"`
	CohortCount   int               `json:"cohort_count"`
	FirstCohortID identity.CohortID `json:"first_cohort_id"`

	ExecutionsPerCohort int `json:"executions_per_cohort"`
	BatchSize           int `json:"batch_size"`
	Executions          int `json:"executions"`

	ToleranceFraction float64 `json:"tolerance_fraction"`

	// MaxAdapterDispatchWaitNanos bounds how long a member may sit at the adapter
	// before dispatch. At A=off the adapter must forward on arrival (ADR-0001).
	MaxAdapterDispatchWaitNanos int64 `json:"max_adapter_dispatch_wait_nanos"`

	// Backend accounting, copied verbatim from the bundle. The validator judges
	// it; it never fetches it.
	ExecutionCountDelta uint64            `json:"execution_count_delta"`
	InferenceCountDelta uint64            `json:"inference_count_delta"`
	BatchSizeHistogram  map[uint64]uint64 `json:"batch_size_histogram"`

	DroppedRecords uint64 `json:"dropped_records"`
}

// Requests lists every logical request the run should have produced.
func (e Expectation) Requests() []identity.LogicalRequest {
	out := make([]identity.LogicalRequest, 0, e.CohortCount*e.CohortSize)
	for c := 0; c < e.CohortCount; c++ {
		for o := 0; o < e.CohortSize; o++ {
			out = append(out, identity.LogicalRequest{
				Cohort:  e.FirstCohortID + identity.CohortID(c),
				Ordinal: identity.Ordinal(o),
			})
		}
	}
	return out
}

// Joined is one logical request's path, assembled from every process's records.
//
// The join is on identity — (run, cohort, ordinal) — because that is the only
// thing that actually links the records. Timestamp proximity is never identity.
type Joined struct {
	Request  identity.LogicalRequest
	Presence events.StageMask
	TS       [events.StageCount + 1]int64
	Status   events.Status

	Domains  map[identity.ClockDomainID]bool
	Emitters map[identity.Emitter]int

	// StageEmitter records which process wrote each timestamp. Ownership is part
	// of the evidence, not an implementation detail: t_cohort_seal belongs to the
	// load generator at A=off and to the proxy at A=on, and a record that claims
	// the wrong one is a defect even when the value looks right (ADR-0001).
	StageEmitter    [events.StageCount + 1]identity.Emitter
	Membership      []identity.UID
	MembershipCount uint32

	// MembershipBySource keeps each process's copy of the attestation rather than
	// collapsing them. On the shared path the client and the adapter observe the
	// same uid set independently, and a fan-out that mapped a set to the wrong
	// member would agree with itself but not with the other source.
	MembershipBySource map[identity.Emitter][]identity.UID

	// StatusBySource keeps each process's view of the outcome. Status is resolved
	// deterministically below, but the raw observations survive: two processes
	// disagreeing about how a request ended is itself evidence.
	StatusBySource map[identity.Emitter]events.Status
	BatchSize      uint32
	SchemaVersions map[uint32]bool
}

// Stage returns a joined timestamp and whether it is present.
func (j *Joined) Stage(s events.Stage) (int64, bool) {
	if !s.Valid() || !j.Presence.Has(s) {
		return 0, false
	}
	return j.TS[s], true
}

// Join assembles per-request paths from the records of every process.
func Join(records []events.Decoded) map[identity.LogicalRequest]*Joined {
	out := make(map[identity.LogicalRequest]*Joined)
	for _, d := range records {
		req := d.Record.Request()
		j, ok := out[req]
		if !ok {
			j = &Joined{
				Request:            req,
				Domains:            map[identity.ClockDomainID]bool{},
				Emitters:           map[identity.Emitter]int{},
				SchemaVersions:     map[uint32]bool{},
				MembershipBySource: map[identity.Emitter][]identity.UID{},
				StatusBySource:     map[identity.Emitter]events.Status{},
			}
			out[req] = j
		}
		j.Domains[d.Header.ClockDomain] = true
		j.Emitters[d.Record.Emitter]++
		j.SchemaVersions[d.SchemaVersion] = true

		for _, s := range events.AllStages() {
			if ts, ok := d.Record.Stage(s); ok {
				j.TS[s] = ts
				j.Presence = j.Presence.With(s)
				j.StageEmitter[s] = d.Record.Emitter
			}
		}
		if d.Record.Status != events.StatusUnspecified {
			j.StatusBySource[d.Record.Emitter] = d.Record.Status
			// Resolution is by rank, not by arrival. A request can carry two
			// different non-OK statuses — the load generator recording a timeout
			// while the adapter records an error for the same member — and
			// last-one-wins would let the order records happened to be read in
			// decide which reached the archive. Rank keeps the more specific
			// diagnosis: a timeout says how it failed, an error only that it did.
			if statusRank(d.Record.Status) > statusRank(j.Status) {
				j.Status = d.Record.Status
			}
		}
		if len(d.Record.MembershipUIDs()) > 0 {
			attested := append([]identity.UID(nil), d.Record.MembershipUIDs()...)
			j.MembershipBySource[d.Record.Emitter] = attested
			j.Membership = attested
			j.MembershipCount = d.Record.MembershipCount
		}
		if d.Record.BatchSize > 0 {
			j.BatchSize = d.Record.BatchSize
		}
	}
	return out
}

// statusRank orders outcomes from least to most diagnostic, so that joining a
// request's records is deterministic regardless of the order they are read in.
func statusRank(s events.Status) int {
	switch s {
	case events.StatusTimeout:
		return 3
	case events.StatusError:
		return 2
	case events.StatusOK:
		return 1
	default:
		return 0
	}
}

// Validate is the whole judgment. It is a pure function: same bundle, same
// verdict, no network, no live state.
func Validate(exp Expectation, records []events.Decoded) Verdict {
	v := Verdict{Run: exp.Run, Cell: exp.Cell}
	joined := Join(records)

	v.Checks = append(v.Checks,
		checkRecordIntegrity(exp, joined),
		checkClockDomain(exp, joined),
		checkPresence(exp, joined),
		checkStageOwnership(exp, joined),
		checkOrdering(exp, joined),
		checkMembership(exp, joined),
		checkContamination(exp),
		checkAdapterDispatch(exp, joined),
	)

	conservation, check := checkConservation(exp, joined)
	v.Conservation = conservation
	v.Checks = append(v.Checks, check)

	executions, execCheck := checkExecutionSerialization(exp, joined)
	v.Executions = executions
	v.Checks = append(v.Checks, execCheck)

	v.Passed = true
	for _, c := range v.Checks {
		if !c.Passed {
			v.Passed = false
		}
	}
	return v
}

func pass(name, detail string) Check {
	return Check{Name: name, Passed: true, Detail: detail}
}

func fail(name string, defects []Defect, detail string) Check {
	return Check{Name: name, Passed: len(defects) == 0, Detail: detail, Defects: defects}
}

func requestPtr(r identity.LogicalRequest) *identity.LogicalRequest { return &r }

// checkRecordIntegrity verifies the run recorded what it claims: nothing
// dropped, every request present exactly once per emitter, one schema version.
func checkRecordIntegrity(exp Expectation, joined map[identity.LogicalRequest]*Joined) Check {
	var defects []Defect

	if exp.DroppedRecords > 0 {
		defects = append(defects, Defect{
			Kind:    DefectDroppedRecords,
			Message: fmt.Sprintf("%d event records were dropped; the run's evidence is incomplete", exp.DroppedRecords),
		})
	}

	for _, req := range exp.Requests() {
		j, ok := joined[req]
		if !ok {
			defects = append(defects, Defect{
				Kind:    DefectMissingRequest,
				Request: requestPtr(req),
				Message: "no event records at all for this logical request",
			})
			continue
		}
		for emitter, n := range j.Emitters {
			if n > 1 {
				defects = append(defects, Defect{
					Kind:    DefectDuplicateRecord,
					Request: requestPtr(req),
					Message: fmt.Sprintf("%s wrote %d records for one request", emitter, n),
				})
			}
		}
		if len(j.SchemaVersions) > 1 {
			defects = append(defects, Defect{
				Kind:    DefectSchemaVersion,
				Request: requestPtr(req),
				Message: "records for one request carry different schema versions",
			})
		}
		if j.Status != events.StatusOK {
			defects = append(defects, Defect{
				Kind:    DefectRequestFailed,
				Request: requestPtr(req),
				Message: fmt.Sprintf("terminal status %s", j.Status),
			})
		}
	}

	// Requests nobody expected are as much a defect as requests that went missing.
	expected := make(map[identity.LogicalRequest]bool, len(exp.Requests()))
	for _, r := range exp.Requests() {
		expected[r] = true
	}
	for req := range joined {
		if !expected[req] {
			defects = append(defects, Defect{
				Kind:    DefectDuplicateRecord,
				Request: requestPtr(req),
				Message: "records for a request the manifest did not schedule",
			})
		}
	}

	return fail("record_integrity", defects,
		fmt.Sprintf("%d of %d expected requests recorded", len(joined), len(exp.Requests())))
}

// checkClockDomain refuses any subtraction across clock domains.
//
// CLOCK_MONOTONIC restarts at boot and is only comparable within one kernel's
// boot, so timestamps from two domains are unrelated numbers that would subtract
// into a plausible-looking duration. This is the check that stops that.
func checkClockDomain(exp Expectation, joined map[identity.LogicalRequest]*Joined) Check {
	var defects []Defect
	for _, req := range sortedRequests(joined) {
		j := joined[req]
		if len(j.Domains) > 1 {
			defects = append(defects, Defect{
				Kind:    DefectCrossClockDomain,
				Request: requestPtr(req),
				Message: fmt.Sprintf("records span %d clock domains (%v); their timestamps cannot be subtracted",
					len(j.Domains), sortedDomains(j.Domains)),
			})
			continue
		}
		if exp.ClockDomain == "" {
			continue
		}
		for domain := range j.Domains {
			if domain != exp.ClockDomain {
				defects = append(defects, Defect{
					Kind:    DefectCrossClockDomain,
					Request: requestPtr(req),
					Message: fmt.Sprintf("records are in clock domain %q, the run declares %q", domain, exp.ClockDomain),
				})
			}
		}
	}
	return fail("clock_domain", defects, fmt.Sprintf("run clock domain %q", exp.ClockDomain))
}

// checkPresence is where absence is typed.
//
// A stage outside the cell's topology is absent by design — D0 has no proxy, so
// t_proxy_recv is not missing, it does not exist. A stage inside the topology but
// not carried by any record is a missing timestamp, and that is a failure. The
// two are different findings and the validator names them differently (ADR-0005).
func checkPresence(exp Expectation, joined map[identity.LogicalRequest]*Joined) Check {
	topology, err := events.TopologyMask(exp.Cell)
	if err != nil {
		return Check{Name: "presence_mask", Passed: false, Detail: err.Error()}
	}

	var defects []Defect
	for _, req := range sortedRequests(joined) {
		j := joined[req]
		if missing := topology &^ j.Presence; missing != 0 {
			for _, s := range missing.Stages() {
				defects = append(defects, Defect{
					Kind:    DefectMissingTimestamp,
					Request: requestPtr(req),
					Stage:   s.String(),
					Message: fmt.Sprintf("%s is in %s's topology but no record carries it", s, exp.Cell),
				})
			}
		}
		if extra := j.Presence &^ topology; extra != 0 {
			for _, s := range extra.Stages() {
				defects = append(defects, Defect{
					Kind:    DefectUnexpectedStage,
					Request: requestPtr(req),
					Stage:   s.String(),
					Message: fmt.Sprintf("%s is absent by topology in %s, but a record carries it", s, exp.Cell),
				})
			}
		}
	}
	return fail("presence_mask", defects, fmt.Sprintf("%s topology %v", exp.Cell, topology))
}

// checkStageOwnership verifies each timestamp was written by the process that
// owns it.
//
// This is what makes t_cohort_seal's conditional ownership checkable rather than
// merely intended. At A=off the load generator emits the seal at barrier release
// and the proxy emits none — because a proxy that sealed anything at A=off would
// be joining, and joining at the OFF/OFF baseline is what ADR-0001 forbids. A
// value in the right column written by the wrong process would otherwise pass
// every other check in this file.
func checkStageOwnership(exp Expectation, joined map[identity.LogicalRequest]*Joined) Check {
	var defects []Defect
	for _, req := range sortedRequests(joined) {
		j := joined[req]
		for _, s := range j.Presence.Stages() {
			emitter := j.StageEmitter[s]
			owned := events.OwnedStages(exp.Cell, emitter)
			if owned.Has(s) {
				continue
			}
			defects = append(defects, Defect{
				Kind:    DefectStageOwnership,
				Request: requestPtr(req),
				Stage:   s.String(),
				Message: fmt.Sprintf("written by %s, which does not own it in %s", emitter, exp.Cell),
			})
		}
	}
	return fail("stage_ownership", defects,
		fmt.Sprintf("%s seal owner is %s", exp.Cell, events.SealOwner(exp.Cell)))
}

// checkOrdering verifies the path's timestamps do not go backwards. A negative
// stage duration is not a small number: it means the schema's ownership or the
// clock assumption is wrong.
func checkOrdering(exp Expectation, joined map[identity.LogicalRequest]*Joined) Check {
	spans, err := Chain(exp.Cell)
	if err != nil {
		return Check{Name: "ordering", Passed: false, Detail: err.Error()}
	}

	var defects []Defect
	for _, req := range sortedRequests(joined) {
		j := joined[req]
		for _, span := range spans {
			start, okStart := j.Stage(span.Start)
			end, okEnd := j.Stage(span.End)
			if !okStart || !okEnd {
				continue // already reported by the presence check
			}
			if end < start {
				defects = append(defects, Defect{
					Kind:    DefectNonMonotonic,
					Request: requestPtr(req),
					Stage:   span.Name,
					Message: fmt.Sprintf("%s ends %dns before it starts (%s=%d, %s=%d)",
						span.Name, start-end, span.Start, start, span.End, end),
				})
			}
		}
	}
	return fail("ordering", defects, fmt.Sprintf("%d spans per request", len(spans)))
}

// checkMembership judges the self-attesting evidence (ADR-0007).
//
// The failure this exists to catch is silent: Triton scatters a batched output
// back per request, so a model that echoes its uid input returns each request
// its own uid — evidence-shaped and attesting nothing. At V=on that shows up as
// an attested set of one where the cohort should be, which is why own-uid echo
// is named as its own defect rather than folded into a size mismatch.
func checkMembership(exp Expectation, joined map[identity.LogicalRequest]*Joined) Check {
	var defects []Defect
	for _, req := range sortedRequests(joined) {
		j := joined[req]

		// Where two processes observed the attestation independently, they must
		// agree. On the shared path the proxy maps results back to members, so a
		// mismatched mapping shows up here and nowhere else — each source is
		// internally consistent, and only the comparison catches it.
		if len(j.MembershipBySource) > 1 {
			if a, b, ok := disagreeingSources(j.MembershipBySource); ok {
				defects = append(defects, Defect{
					Kind:    DefectMembershipDisagree,
					Request: requestPtr(req),
					Message: fmt.Sprintf("%s attested %v but %s attested %v for the same execution",
						a.emitter, a.uids, b.emitter, b.uids),
				})
			}
		}

		if len(j.Membership) == 0 {
			defects = append(defects, Defect{
				Kind:    DefectMembershipMismatch,
				Request: requestPtr(req),
				Message: "no membership evidence: the execution attested nothing",
			})
			continue
		}
		if int(j.MembershipCount) != len(j.Membership) {
			defects = append(defects, Defect{
				Kind:    DefectTruncatedEvidence,
				Request: requestPtr(req),
				Message: fmt.Sprintf("execution claimed %d members but only %d were recorded",
					j.MembershipCount, len(j.Membership)),
			})
		}

		self := req.UID()
		var containsSelf bool
		for _, uid := range j.Membership {
			if uid == self {
				containsSelf = true
				break
			}
		}
		if !containsSelf {
			defects = append(defects, Defect{
				Kind:    DefectMembershipMismatch,
				Request: requestPtr(req),
				Message: fmt.Sprintf("attested set %v does not contain the request's own uid %d", j.Membership, self),
			})
		}

		if len(j.Membership) != exp.BatchSize {
			kind := DefectMembershipMismatch
			if exp.BatchSize > 1 && len(j.Membership) == 1 && containsSelf {
				// Exactly the naive-echo signature: each request got its own uid back
				// and nothing else, where a batch of B was expected.
				kind = DefectOwnUIDEcho
			}
			defects = append(defects, Defect{
				Kind:    kind,
				Request: requestPtr(req),
				Message: fmt.Sprintf("execution attested %d members, the cell expects batch size %d",
					len(j.Membership), exp.BatchSize),
			})
			continue
		}
		if int(j.BatchSize) != exp.BatchSize {
			defects = append(defects, Defect{
				Kind:    DefectMembershipMismatch,
				Request: requestPtr(req),
				Message: fmt.Sprintf("record reports batch size %d, the cell expects %d", j.BatchSize, exp.BatchSize),
			})
		}

		// Every attested uid must resolve to a logical request of this run's
		// cohorts, and at batch size B they must all share one cohort — otherwise
		// the execution mixed cohorts, which is a different finding from a wrong
		// count.
		for _, uid := range j.Membership {
			member := uid.LogicalRequest()
			if member.Cohort != req.Cohort {
				defects = append(defects, Defect{
					Kind:    DefectMembershipMismatch,
					Request: requestPtr(req),
					Message: fmt.Sprintf("attested member %v belongs to cohort %d, not %d",
						member, member.Cohort, req.Cohort),
				})
				continue
			}
			if int(member.Ordinal) >= exp.CohortSize {
				defects = append(defects, Defect{
					Kind:    DefectMembershipMismatch,
					Request: requestPtr(req),
					Message: fmt.Sprintf("attested member %v has an ordinal outside a cohort of %d", member, exp.CohortSize),
				})
			}
		}
	}
	return fail("membership", defects, fmt.Sprintf("expected batch size %d", exp.BatchSize))
}

// checkContamination judges the backend's own accounting.
//
// On the unbatched entry the scheduler must never coalesce singles. That is not
// a warning to be noted: a V=off cell that quietly ran a batched execution would
// report a factor level it did not realize, so it fails the run (M1 §2.1).
func checkContamination(exp Expectation) Check {
	var defects []Defect

	if exp.ExecutionCountDelta != uint64(exp.Executions) {
		defects = append(defects, Defect{
			Kind: DefectExecutionCount,
			Message: fmt.Sprintf("backend reports %d executions, the cell expects %d",
				exp.ExecutionCountDelta, exp.Executions),
		})
	}
	expectedInferences := uint64(exp.CohortCount * exp.CohortSize)
	if exp.InferenceCountDelta != expectedInferences {
		defects = append(defects, Defect{
			Kind: DefectExecutionCount,
			Message: fmt.Sprintf("backend reports %d inferences, the run released %d logical requests",
				exp.InferenceCountDelta, expectedInferences),
		})
	}

	for size, count := range exp.BatchSizeHistogram {
		if size != uint64(exp.BatchSize) {
			defects = append(defects, Defect{
				Kind: DefectCoalescedSingles,
				Message: fmt.Sprintf("batch-size histogram has %d executions at size %d; the cell expects only size %d",
					count, size, exp.BatchSize),
			})
		}
	}
	if len(exp.BatchSizeHistogram) == 0 && exp.Executions > 0 {
		defects = append(defects, Defect{
			Kind:    DefectExecutionCount,
			Message: "the backend reported no batch-size histogram; the contamination check has nothing to judge",
		})
	}

	return fail("contamination", defects,
		fmt.Sprintf("%d executions, histogram %v", exp.ExecutionCountDelta, sortedHistogram(exp.BatchSizeHistogram)))
}

// source pairs an emitter with what it attested.
type source struct {
	emitter identity.Emitter
	uids    []identity.UID
}

// disagreeingSources reports the first pair of sources whose attested sets
// differ. Order is not significant — one execution's membership is a set — so
// the comparison is order-insensitive.
func disagreeingSources(bySource map[identity.Emitter][]identity.UID) (source, source, bool) {
	emitters := make([]identity.Emitter, 0, len(bySource))
	for e := range bySource {
		emitters = append(emitters, e)
	}
	sort.Slice(emitters, func(i, j int) bool { return emitters[i] < emitters[j] })

	for i := 0; i < len(emitters); i++ {
		for j := i + 1; j < len(emitters); j++ {
			a, b := bySource[emitters[i]], bySource[emitters[j]]
			if !sameUIDSet(a, b) {
				return source{emitters[i], a}, source{emitters[j], b}, true
			}
		}
	}
	return source{}, source{}, false
}

func sameUIDSet(a, b []identity.UID) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[identity.UID]int, len(a))
	for _, uid := range a {
		counts[uid]++
	}
	for _, uid := range b {
		counts[uid]--
		if counts[uid] < 0 {
			return false
		}
	}
	return true
}

func sortedRequests(joined map[identity.LogicalRequest]*Joined) []identity.LogicalRequest {
	out := make([]identity.LogicalRequest, 0, len(joined))
	for req := range joined {
		out = append(out, req)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Cohort != out[j].Cohort {
			return out[i].Cohort < out[j].Cohort
		}
		return out[i].Ordinal < out[j].Ordinal
	})
	return out
}

func sortedDomains(m map[identity.ClockDomainID]bool) []string {
	out := make([]string, 0, len(m))
	for d := range m {
		out = append(out, string(d))
	}
	sort.Strings(out)
	return out
}

func sortedHistogram(h map[uint64]uint64) string {
	sizes := make([]uint64, 0, len(h))
	for s := range h {
		sizes = append(sizes, s)
	}
	sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })

	out := "{"
	for i, s := range sizes {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%d:%d", s, h[s])
	}
	return out + "}"
}
