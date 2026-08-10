package validate_test

import (
	"testing"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/testkit"
	"github.com/matthewhoung/batch2go/internal/validate"
)

// The defect suite. Each fixture plants one fault and the validator must fail,
// naming that fault — a validator that passes everything is indistinguishable
// from no validator, and one that fails without saying why is not usable.

func assertNamedDefect(t *testing.T, fixture testkit.Bundle, want validate.DefectKind) validate.Verdict {
	t.Helper()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)

	if verdict.Passed {
		t.Fatalf("a bundle carrying %s must not validate green", want)
	}
	if !verdict.HasDefect(want) {
		t.Fatalf("verdict does not name %s; it found: %v", want, verdict.Defects())
	}
	return verdict
}

// A stage the cell's topology has, gone missing, is a failure. The next test
// establishes the contrast that gives this one its meaning.
func TestMissingTimestampFailsWithTheStageNamed(t *testing.T) {
	base := testkit.NewSpec(identity.CellF00).MustBuild()
	fixture, err := base.WithMissingTimestamp(identity.EmitterAdapter, events.StageAdapterDispatch)
	if err != nil {
		t.Fatalf("plant defect: %v", err)
	}

	verdict := assertNamedDefect(t, fixture, validate.DefectMissingTimestamp)
	var named bool
	for _, d := range verdict.Defects() {
		if d.Kind == validate.DefectMissingTimestamp && d.Stage == events.StageAdapterDispatch.String() {
			named = true
		}
	}
	if !named {
		t.Errorf("the defect should name t_adapter_dispatch, got: %v", verdict.Defects())
	}
}

// The contrast that makes typed absence worth having: D0 has no proxy or adapter
// stages at all, and their absence must not read as missing evidence.
func TestAbsentByTopologyIsNotAMissingTimestamp(t *testing.T) {
	fixture := testkit.NewSpec(identity.CellD0).MustBuild()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)

	if !verdict.Passed {
		t.Fatalf("D0's missing proxy and adapter stages are absent by topology, not missing: %v", verdict.Defects())
	}
	if verdict.HasDefect(validate.DefectMissingTimestamp) {
		t.Error("D0 reported a missing timestamp for a stage its topology does not have")
	}

	// And the same records judged as F00 — a cell whose topology does have those
	// stages — must fail. Same evidence, different expectation, different verdict.
	asF00 := fixture.Expectation
	asF00.Cell = identity.CellF00
	if v := validate.Validate(asF00, fixture.Records); !v.HasDefect(validate.DefectMissingTimestamp) {
		t.Error("D0's records judged against F00's topology should report missing timestamps")
	}
}

// A timestamp no process on this path could have observed is its own defect.
func TestUnexpectedStageForTopologyFails(t *testing.T) {
	base := testkit.NewSpec(identity.CellD0).MustBuild()
	fixture, err := base.WithUnexpectedStage(events.StageProxyRecv, 1_000_000_000_500)
	if err != nil {
		t.Fatalf("plant defect: %v", err)
	}
	assertNamedDefect(t, fixture, validate.DefectUnexpectedStage)
}

// Membership that does not match the cohort labels means the accounting and the
// physical execution disagree.
func TestMembershipMismatchedToCohortFails(t *testing.T) {
	base := testkit.NewSpec(identity.CellF01).MustBuild()
	fixture, err := base.WithForeignCohortMembership()
	if err != nil {
		t.Fatalf("plant defect: %v", err)
	}
	assertNamedDefect(t, fixture, validate.DefectMembershipMismatch)
}

// The permanent counter-fixture. An own-uid-only echo is evidence-shaped and
// attests nothing; it must be named as such rather than passed off as a count
// mismatch (ADR-0007).
func TestOwnUIDEchoIsNamedAsItsOwnDefect(t *testing.T) {
	// F01 is V=on, so a cohort executes as one execution of B — which is the only
	// place the echo is distinguishable from correct evidence.
	fixture := testkit.NewSpec(identity.CellF01).MustBuild().WithOwnUIDEcho()
	assertNamedDefect(t, fixture, validate.DefectOwnUIDEcho)
}

// The other permanent counter-fixture. The unbatched entry must never coalesce
// singles: a V=off cell that ran a batched execution would carry a factor level
// it did not realize.
func TestCoalescedSinglesFails(t *testing.T) {
	fixture := testkit.NewSpec(identity.CellF00).MustBuild().WithCoalescedSingles()
	assertNamedDefect(t, fixture, validate.DefectCoalescedSingles)
}

// Timestamps from two clock domains must never be subtracted. The resulting
// duration would look entirely plausible, which is why this is caught
// structurally and not by noticing an odd value.
func TestCrossClockDomainSubtractionFails(t *testing.T) {
	base := testkit.NewSpec(identity.CellF00).MustBuild()
	fixture, err := base.WithCrossClockDomain(identity.EmitterTriton)
	if err != nil {
		t.Fatalf("plant defect: %v", err)
	}
	assertNamedDefect(t, fixture, validate.DefectCrossClockDomain)
}

func TestNonMonotonicPathFails(t *testing.T) {
	base := testkit.NewSpec(identity.CellD0).MustBuild()
	fixture, err := base.WithNonMonotonicPath(identity.EmitterTriton, events.StageComputeEnd, 10_000_000)
	if err != nil {
		t.Fatalf("plant defect: %v", err)
	}
	assertNamedDefect(t, fixture, validate.DefectNonMonotonic)
}

// Evidence the instrument lost is reported, never absorbed.
func TestDroppedRecordsFail(t *testing.T) {
	fixture := testkit.NewSpec(identity.CellD0).MustBuild().WithDroppedRecords(3)
	assertNamedDefect(t, fixture, validate.DefectDroppedRecords)
}

// A request that produced no records at all must not simply shrink the
// denominator.
func TestMissingRequestFails(t *testing.T) {
	fixture := testkit.NewSpec(identity.CellD0).MustBuild().WithMissingRequest()
	assertNamedDefect(t, fixture, validate.DefectMissingRequest)
}

// Evidence that did not fit is different from evidence of a smaller execution.
func TestTruncatedMembershipEvidenceFails(t *testing.T) {
	base := testkit.NewSpec(identity.CellF01).MustBuild()
	fixture, err := base.WithTruncatedMembership()
	if err != nil {
		t.Fatalf("plant defect: %v", err)
	}
	assertNamedDefect(t, fixture, validate.DefectTruncatedEvidence)
}

// Every defect fixture must fail. This guards the suite itself: if a future
// change made one of the planted faults invisible, the individual test above
// would still be there but the guarantee would be gone.
func TestEveryDefectFixtureFails(t *testing.T) {
	d0 := testkit.NewSpec(identity.CellD0).MustBuild()
	f00 := testkit.NewSpec(identity.CellF00).MustBuild()
	f01 := testkit.NewSpec(identity.CellF01).MustBuild()

	missing, err := f00.WithMissingTimestamp(identity.EmitterProxy, events.StageProxySend)
	if err != nil {
		t.Fatalf("plant: %v", err)
	}
	unexpected, err := d0.WithUnexpectedStage(events.StageAdapterRecv, 1_000_000_001_000)
	if err != nil {
		t.Fatalf("plant: %v", err)
	}
	foreign, err := f01.WithForeignCohortMembership()
	if err != nil {
		t.Fatalf("plant: %v", err)
	}
	crossDomain, err := f00.WithCrossClockDomain(identity.EmitterAdapter)
	if err != nil {
		t.Fatalf("plant: %v", err)
	}

	fixtures := map[string]testkit.Bundle{
		"missing_timestamp":  missing,
		"unexpected_stage":   unexpected,
		"foreign_cohort":     foreign,
		"own_uid_echo":       f01.WithOwnUIDEcho(),
		"coalesced_singles":  f00.WithCoalescedSingles(),
		"cross_clock_domain": crossDomain,
		"dropped_records":    d0.WithDroppedRecords(1),
		"missing_request":    d0.WithMissingRequest(),
	}
	for name, fixture := range fixtures {
		if v := validate.Validate(fixture.Expectation, fixture.Records); v.Passed {
			t.Errorf("defect fixture %q validated green", name)
		}
	}

	// And the controls: without a planted fault, each cell validates green.
	for _, control := range []testkit.Bundle{d0, f00, f01} {
		if v := validate.Validate(control.Expectation, control.Records); !v.Passed {
			t.Errorf("control fixture for %s failed: %v", control.Spec.Cell, v.Defects())
		}
	}
}

// The adapter must forward on arrival at A=off. A joined cohort leaves together
// even though it arrived apart, and that shape — not the size of any single
// wait — is what the check keys on.
func TestAdapterJoiningAtAOffFails(t *testing.T) {
	control := testkit.NewSpec(identity.CellF00).MustBuild()
	if v := validate.Validate(control.Expectation, control.Records); !v.Passed {
		t.Fatalf("the control fixture dispatches on arrival and should pass: %v", v.Defects())
	}

	fixture := control.WithAdapterJoin()
	assertNamedDefect(t, fixture, validate.DefectAdapterWaiting)
}

// D0 has no adapter at all, so the check has nothing to judge and must not
// invent a finding.
func TestAdapterDispatchCheckSkipsCellsWithoutAnAdapter(t *testing.T) {
	fixture := testkit.NewSpec(identity.CellD0).MustBuild()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)

	for _, c := range verdict.Checks {
		if c.Name == "adapter_dispatch_on_arrival" && !c.Passed {
			t.Errorf("D0 has no adapter; the check should pass trivially, got: %v", c.Defects)
		}
	}
}

// The seal's owner is conditional on Factor A, and a seal from the wrong process
// must fail even though its value is correct and every other check passes.
func TestCohortSealFromTheWrongEmitterFails(t *testing.T) {
	base := testkit.NewSpec(identity.CellF00).MustBuild()
	fixture, err := base.WithSealFromWrongEmitter()
	if err != nil {
		t.Fatalf("plant defect: %v", err)
	}

	verdict := assertNamedDefect(t, fixture, validate.DefectStageOwnership)

	// The value is intact and in the right column, so nothing else notices.
	if verdict.HasDefect(validate.DefectMissingTimestamp) {
		t.Error("the seal is still present; this is an ownership defect, not a missing timestamp")
	}
	for _, d := range verdict.Defects() {
		if d.Kind == validate.DefectStageOwnership && d.Stage != events.StageCohortSeal.String() {
			t.Errorf("ownership defect names %s, want t_cohort_seal", d.Stage)
		}
	}
}

// Two processes observing the same attestation must agree. Each is internally
// consistent, so only the comparison catches a mapping error between them.
func TestDisagreeingMembershipSourcesFail(t *testing.T) {
	base := testkit.NewSpec(identity.CellF00).MustBuild()
	fixture, err := base.WithDisagreeingMembershipSources()
	if err != nil {
		t.Fatalf("plant defect: %v", err)
	}
	assertNamedDefect(t, fixture, validate.DefectMembershipDisagree)
}

// With only one source there is nothing to compare, and the check must not
// invent a disagreement — but the attestation itself is still judged.
func TestSingleMembershipSourceIsStillJudged(t *testing.T) {
	base := testkit.NewSpec(identity.CellF00).MustBuild()

	only := base.WithMembershipOnlyFromOneSource(identity.EmitterAdapter)
	if v := validate.Validate(only.Expectation, only.Records); !v.Passed {
		t.Errorf("one consistent source should still validate green: %v", v.Defects())
	}

	// And an attestation that is wrong on its own still fails, single source or not.
	broken, err := only.WithForeignCohortMembership()
	if err != nil {
		t.Fatalf("plant defect: %v", err)
	}
	assertNamedDefect(t, broken, validate.DefectMembershipMismatch)
}

// The cohort-level check that can actually fail.
//
// It replaced an interval-coverage test that could not: under fixed-cohort
// release the members' in-flight intervals always overlap, so their union was
// identically the makespan and uncovered time was zero for every input. This one
// tests the cell's declared mechanism instead — one model instance, executions
// one at a time — and a violation is invisible everywhere else.
func TestOverlappingExecutionsFail(t *testing.T) {
	control := testkit.NewSpec(identity.CellF00).MustBuild()
	if v := validate.Validate(control.Expectation, control.Records); !v.Passed {
		t.Fatalf("the control fixture serializes and should pass: %v", v.Defects())
	}

	fixture := control.WithOverlappingExecutions()
	verdict := assertNamedDefect(t, fixture, validate.DefectOverlappingExecutions)

	// Everything else still looks right, which is the whole point.
	for _, c := range verdict.Checks {
		switch c.Name {
		case "membership", "contamination", "presence_mask", "record_integrity":
			if !c.Passed {
				t.Errorf("%s failed too; the overlap should be invisible to it", c.Name)
			}
		}
	}
}

// A vectorized cohort executes once, so there is nothing to serialize and the
// check must not invent a finding.
func TestSerializationCheckSkipsVectorizedCells(t *testing.T) {
	fixture := testkit.NewSpec(identity.CellF01).MustBuild()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)
	for _, c := range verdict.Checks {
		if c.Name == "execution_serialization" && !c.Passed {
			t.Errorf("F01 executes each cohort once; the check should pass trivially, got: %v", c.Defects)
		}
	}
}
