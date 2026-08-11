package validate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// The manipulation check for Factor A.
//
// Every other check in this package would pass against a proxy that sealed each
// member separately and sent B envelopes: it would produce n=B executions, J=B
// distinct ones, every execution of shape [1,…], a clean coalescing histogram,
// correct self-attested membership, a matching presence mask and a conservation
// residual within tolerance. It would be F00's behaviour wearing F10's label,
// and the aggregation contrast would be a comparison of one implementation
// against itself.
//
// What separates them is cardinality and agreement: one envelope per cohort,
// carrying that cohort's members and no others, with one shared value for every
// timestamp that describes the envelope rather than a member.

// AggregationReport is what the validator observed about a run's envelopes. It
// is evidence: the counts and the disagreements, not a conclusion about them.
type AggregationReport struct {
	// Cohorts is the per-cohort envelope accounting, present only for cells whose
	// members share an envelope.
	Cohorts []CohortEnvelopes `json:"cohorts,omitempty"`

	// FailedFormations are the cohorts that never assembled. They are named here
	// so that the diagnosis is a finding rather than a pattern a reader has to
	// spot in a list of missing timestamps.
	FailedFormations []FormationFailure `json:"failed_formations,omitempty"`
}

// CohortEnvelopes is one cohort's envelope evidence.
type CohortEnvelopes struct {
	Cohort identity.CohortID `json:"cohort_id"`

	// Envelopes is how many distinct envelope ids the cohort's members reported.
	// One is the claim F10 makes; B is what F00 does.
	Envelopes int `json:"envelope_count"`

	// Members is how many logical requests reported that envelope.
	Members int `json:"member_count"`
}

// FormationFailure is a cohort the proxy never assembled.
type FormationFailure struct {
	Cohort identity.CohortID `json:"cohort_id"`

	// Held are the members that reached the proxy and were failed with it, and
	// Absent the members that never got there. The distinction is the whole
	// diagnostic content of the failure: a held member records an error because it
	// did not itself time out, and an absent one keeps whatever status the load
	// generator gave it (ADR-0010).
	Held   []identity.Ordinal `json:"held"`
	Absent []identity.Ordinal `json:"absent"`
}

// FailedCohorts is the set of cohorts that never formed, for the checks that
// must displace their own findings rather than pile onto the diagnosis.
func (r AggregationReport) FailedCohorts() map[identity.CohortID]bool {
	if len(r.FailedFormations) == 0 {
		return nil
	}
	out := make(map[identity.CohortID]bool, len(r.FailedFormations))
	for _, f := range r.FailedFormations {
		out[f.Cohort] = true
	}
	return out
}

// checkAggregation judges whether a cohort travelled as one envelope.
func checkAggregation(exp Expectation, joined map[identity.LogicalRequest]*Joined) (AggregationReport, Check) {
	var report AggregationReport
	report.FailedFormations = findFormationFailures(exp, joined)

	if !exp.Cell.AggregatesEnvelopes() {
		// At A=off a cohort is an accounting label and its members travel
		// separately by construction. There is no aggregation to prove, and
		// asserting one envelope per cohort here would fail every correct run.
		return report, pass("envelope_aggregation",
			fmt.Sprintf("%s is A=off; its members travel in their own envelopes", exp.Cell))
	}

	failed := report.FailedCohorts()
	byCohort := map[identity.CohortID][]*Joined{}
	for _, req := range sortedRequests(joined) {
		if failed[req.Cohort] {
			// A cohort that never formed has no envelope, and reporting its absence
			// as a cardinality fault would name a symptom of a failure already
			// named.
			continue
		}
		byCohort[req.Cohort] = append(byCohort[req.Cohort], joined[req])
	}

	cohorts := make([]identity.CohortID, 0, len(byCohort))
	for c := range byCohort {
		cohorts = append(cohorts, c)
	}
	sort.Slice(cohorts, func(i, j int) bool { return cohorts[i] < cohorts[j] })

	var defects []Defect
	for _, cohort := range cohorts {
		members := byCohort[cohort]

		envelopes := map[identity.EnvelopeID]bool{}
		for _, m := range members {
			if m.EnvelopeID != 0 {
				envelopes[m.EnvelopeID] = true
			}
		}
		report.Cohorts = append(report.Cohorts, CohortEnvelopes{
			Cohort: cohort, Envelopes: len(envelopes), Members: len(members),
		})

		if len(envelopes) != 1 {
			defects = append(defects, Defect{
				Kind: DefectEnvelopeCardinality,
				Message: fmt.Sprintf(
					"cohort %d of %d members travelled in %d envelopes; at A=on one envelope carries the cohort, and %d of them is A=off wearing this cell's label",
					cohort, len(members), len(envelopes), len(envelopes)),
			})
		}

		for _, stage := range EnvelopeStages().Stages() {
			values := map[int64][]identity.Ordinal{}
			for _, m := range members {
				if v, ok := m.Stage(stage); ok {
					values[v] = append(values[v], m.Request.Ordinal)
				}
			}
			if len(values) <= 1 {
				continue
			}
			defects = append(defects, Defect{
				Kind:  DefectEnvelopeStageDisagreement,
				Stage: stage.String(),
				Message: fmt.Sprintf(
					"cohort %d reports %d values of %s (%s); it describes the envelope, so a cohort that shares one shares this",
					cohort, len(values), stage, describeDisagreement(values)),
			})
		}
	}

	// The envelope carrying a foreign member is the other half of cardinality: one
	// envelope is the claim, and one envelope of THIS cohort's members is what the
	// claim means.
	for envelope, owners := range envelopeOwners(joined, failed) {
		if len(owners) > 1 {
			defects = append(defects, Defect{
				Kind: DefectEnvelopeCardinality,
				Message: fmt.Sprintf("envelope %d carries members of cohorts %v; an envelope carries one cohort",
					envelope, owners),
			})
		}
	}

	detail := fmt.Sprintf("%d cohorts, one envelope each", len(report.Cohorts))
	if len(report.FailedFormations) > 0 {
		detail += fmt.Sprintf(" (%d cohorts never formed and are judged by their own defect)", len(report.FailedFormations))
	}
	return report, fail("envelope_aggregation", defects, detail)
}

// envelopeOwners maps each envelope id to the cohorts whose members reported it.
func envelopeOwners(
	joined map[identity.LogicalRequest]*Joined,
	failed map[identity.CohortID]bool,
) map[identity.EnvelopeID][]identity.CohortID {
	seen := map[identity.EnvelopeID]map[identity.CohortID]bool{}
	for _, req := range sortedRequests(joined) {
		if failed[req.Cohort] {
			continue
		}
		id := joined[req].EnvelopeID
		if id == 0 {
			continue
		}
		if seen[id] == nil {
			seen[id] = map[identity.CohortID]bool{}
		}
		seen[id][req.Cohort] = true
	}

	out := make(map[identity.EnvelopeID][]identity.CohortID, len(seen))
	for id, cohorts := range seen {
		list := make([]identity.CohortID, 0, len(cohorts))
		for c := range cohorts {
			list = append(list, c)
		}
		sort.Slice(list, func(i, j int) bool { return list[i] < list[j] })
		out[id] = list
	}
	return out
}

// findFormationFailures identifies cohorts the proxy never assembled.
//
// The signature is a cohort where some members reached the proxy and stopped
// there while others never reached it at all: a held member has an arrival and
// no seal, and an absent one has no proxy record. Neither alone is enough. A
// member with an arrival and no seal could be a cohort that failed for some
// other reason, and a member with no proxy record at all could be a lost record
// — it is the two together, in one cohort, that say formation is what failed.
func findFormationFailures(exp Expectation, joined map[identity.LogicalRequest]*Joined) []FormationFailure {
	if !exp.Cell.AggregatesEnvelopes() {
		// Formation happens at the proxy and only where it aggregates. At A=off
		// there is nothing to fail to form, and a cohort short a member is a
		// missing request, which is a different finding with its own name.
		return nil
	}

	held := map[identity.CohortID][]identity.Ordinal{}
	absent := map[identity.CohortID][]identity.Ordinal{}
	for _, req := range sortedRequests(joined) {
		j := joined[req]
		_, arrived := j.Stage(events.StageProxyRecv)
		_, sealed := j.Stage(events.StageCohortSeal)
		switch {
		case arrived && !sealed:
			held[req.Cohort] = append(held[req.Cohort], req.Ordinal)
		case !arrived:
			absent[req.Cohort] = append(absent[req.Cohort], req.Ordinal)
		}
	}

	cohorts := make([]identity.CohortID, 0, len(held))
	for c := range held {
		if len(absent[c]) > 0 {
			cohorts = append(cohorts, c)
		}
	}
	sort.Slice(cohorts, func(i, j int) bool { return cohorts[i] < cohorts[j] })

	out := make([]FormationFailure, 0, len(cohorts))
	for _, c := range cohorts {
		out = append(out, FormationFailure{Cohort: c, Held: held[c], Absent: absent[c]})
	}
	return out
}

// checkFormation words each failed formation once, at cohort level.
//
// Once, because B members failing for one reason is one diagnosis and not B
// (ADR-0010) — and because the checks whose findings this displaces would
// otherwise report the same event a dozen times over, in a vocabulary that
// describes the wreckage rather than the cause.
//
// It is its own check rather than a defect on the aggregation one so that a run
// which lost a cohort is distinguishable, at the level of which check failed,
// from a run whose proxy was not aggregating at all. Those are opposite
// findings: one is a cell that works and lost a cohort, the other is a cell that
// is not the cell it claims to be.
func checkFormation(report AggregationReport) Check {
	defects := make([]Defect, 0, len(report.FailedFormations))
	for _, f := range report.FailedFormations {
		defects = append(defects, Defect{
			Kind: DefectFormationFailure,
			Message: fmt.Sprintf(
				"cohort %d never formed: ordinals %v reached the proxy and were failed with it, ordinals %v never arrived",
				f.Cohort, f.Held, f.Absent),
		})
	}
	return fail("cohort_formation", defects,
		fmt.Sprintf("%d cohorts failed to form", len(report.FailedFormations)))
}

// withoutCohorts hides the requests of cohorts already judged by a cause of
// their own, so that the checks downstream report what else went wrong rather
// than restating one failure in their own vocabulary.
//
// It hides nothing else, and it hides nothing silently: the cohorts it removes
// are named in the verdict by the check that removed them. This is displacement,
// not exclusion — the distinction being that a displaced finding has somewhere
// better to be.
func withoutCohorts(
	joined map[identity.LogicalRequest]*Joined,
	hidden map[identity.CohortID]bool,
) map[identity.LogicalRequest]*Joined {
	if len(hidden) == 0 {
		return joined
	}
	out := make(map[identity.LogicalRequest]*Joined, len(joined))
	for req, j := range joined {
		if !hidden[req.Cohort] {
			out[req] = j
		}
	}
	return out
}

func describeDisagreement(values map[int64][]identity.Ordinal) string {
	instants := make([]int64, 0, len(values))
	for v := range values {
		instants = append(instants, v)
	}
	sort.Slice(instants, func(i, j int) bool { return instants[i] < instants[j] })

	parts := make([]string, 0, len(instants))
	for _, v := range instants {
		ords := values[v]
		sort.Slice(ords, func(i, j int) bool { return ords[i] < ords[j] })
		parts = append(parts, fmt.Sprintf("%d for ordinals %v", v, ords))
	}
	return strings.Join(parts, "; ")
}
