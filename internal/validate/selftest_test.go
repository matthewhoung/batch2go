package validate_test

import (
	"testing"
	"time"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/testkit"
	"github.com/matthewhoung/batch2go/internal/validate"
)

// The instrument self-test.
//
// Before any live conservation number can be trusted, the validator has to be
// shown recovering delays that were put there on purpose. Otherwise "the
// residual was within tolerance" only establishes that the arithmetic closes —
// not that a stage's duration lands in the stage it belongs to. Every named
// stage of every implemented cell gets a distinct, deliberately unroundish
// injected delay so that a misattribution cannot hide behind a coincidence.

func injectedDelays() map[string]time.Duration {
	return map[string]time.Duration{
		validate.StageWForm:         37 * time.Microsecond,
		validate.StageBarrierWait:   41 * time.Microsecond,
		validate.StageReleaseToSend: 53 * time.Microsecond,
		validate.StageXReq:          211 * time.Microsecond,
		validate.StageXReqHop1:      149 * time.Microsecond,
		validate.StageAPack:         307 * time.Microsecond,
		validate.StageXReqHop2:      173 * time.Microsecond,
		validate.StageAdapterUnpack: 89 * time.Microsecond,
		validate.StageXReqHop3:      67 * time.Microsecond,
		validate.StageQBackend:      1223 * time.Microsecond,
		validate.StageSComp:         4001 * time.Microsecond,
		validate.StageXRespHop1:     71 * time.Microsecond,
		validate.StageResponsePack:  113 * time.Microsecond,
		validate.StageXRespHop2:     197 * time.Microsecond,
		validate.StageFFanout:       131 * time.Microsecond,
		validate.StageXRespHop3:     43 * time.Microsecond,
		validate.StageXResp:         167 * time.Microsecond,
	}
}

func specWithInjectedDelays(cell identity.Cell) testkit.Spec {
	spec := testkit.NewSpec(cell)
	for stage, d := range injectedDelays() {
		spec = spec.WithDelay(stage, d)
	}
	return spec
}

// variesAcrossCohort names the stages whose per-member values are not all the
// injected duration, and says why. Each of them is a member waiting for
// something that belongs to another member, so the injected value is the floor
// and the spread above it is the waiting.
//
// Getting this list wrong in either direction hides something. Declaring a
// stage variable when it is not would accept a fixture whose delays landed in
// the wrong place; leaving one out would report a spread as a recovery failure.
func variesAcrossCohort(cell identity.Cell, stage string) (string, bool) {
	switch stage {
	case validate.StageQBackend:
		// Every cell: request i waits behind requests 0..i-1 on the one model
		// instance, and that wait is what the cycle model books here (M1 §2.2).
		return "the first member of a cohort waits behind nobody", true
	case validate.StageWForm:
		// A=on only: the seal is one instant and each member measures back to its
		// own arrival, so the member that arrived last waited only the injected
		// time and the first waited longest (ADR-0010).
		return "the member that arrived last waited for nobody", cell.AggregatesEnvelopes()
	case validate.StageResponsePack:
		// A=on only: one response is packed after the last member's result, so a
		// member that finished early measures its own wait for the rest of the
		// cohort into this interval.
		return "the member whose result came last waited for nobody", cell.AggregatesEnvelopes()
	default:
		return "", false
	}
}

func TestSelfTestRecoversEveryInjectedDelay(t *testing.T) {
	for _, cell := range []identity.Cell{identity.CellD0, identity.CellF00, identity.CellF10} {
		t.Run(string(cell), func(t *testing.T) {
			spec := specWithInjectedDelays(cell)
			fixture := spec.MustBuild()

			verdict := validate.Validate(fixture.Expectation, fixture.Records)
			if !verdict.Passed {
				t.Fatalf("a well-formed fixture must validate green, got: %v", verdict.Defects())
			}

			spans, err := validate.Chain(cell)
			if err != nil {
				t.Fatalf("chain: %v", err)
			}
			expectedRequests := spec.CohortCount * spec.CohortSize

			for _, span := range spans {
				summary, ok := verdict.Conservation.StageSummary(span.Name)
				if !ok {
					t.Errorf("no summary for stage %s", span.Name)
					continue
				}
				if summary.Count != expectedRequests {
					t.Errorf("%s summarized over %d requests, want %d", span.Name, summary.Count, expectedRequests)
				}

				want := int64(spec.Delay(span.Name))
				tolerance := int64(float64(want) * spec.ToleranceFraction)

				// Some stages legitimately vary across a cohort, and for those the
				// injected value is the floor rather than every member's value. The
				// member that is not waiting for anybody recovers it exactly, and the
				// spread above it is the waiting — which is a measurement, not an
				// error, so the test asserts the spread exists rather than tolerating
				// it.
				if reason, varies := variesAcrossCohort(cell, span.Name); varies {
					if diff := summary.MinNanos - want; diff > tolerance || diff < -tolerance {
						t.Errorf("%s min = %dns, injected %dns; %s", span.Name, summary.MinNanos, want, reason)
					}
					if summary.MaxNanos <= summary.MinNanos {
						t.Errorf("%s does not vary across a cohort; %s, so the wait went somewhere else", span.Name, reason)
					}
					continue
				}

				for _, got := range []struct {
					label string
					value int64
				}{
					{"median", summary.MedianNanos},
					{"min", summary.MinNanos},
					{"max", summary.MaxNanos},
				} {
					if diff := got.value - want; diff > tolerance || diff < -tolerance {
						t.Errorf("%s %s = %dns, injected %dns (off by %+dns, tolerance %dns)",
							span.Name, got.label, got.value, want, diff, tolerance)
					}
				}
			}
		})
	}
}

// Recovering a delay is only meaningful if changing it moves the right stage and
// nothing else. This is what separates "the arithmetic closes" from "the
// decomposition attributes time correctly".
func TestChangingOneInjectedDelayMovesOnlyThatStage(t *testing.T) {
	const bumped = validate.StageSComp
	const bump = 5 * time.Millisecond

	base := specWithInjectedDelays(identity.CellF00).MustBuild()
	baseVerdict := validate.Validate(base.Expectation, base.Records)

	changed := specWithInjectedDelays(identity.CellF00).
		WithDelay(bumped, injectedDelays()[bumped]+bump).MustBuild()
	changedVerdict := validate.Validate(changed.Expectation, changed.Records)

	if !changedVerdict.Passed {
		t.Fatalf("fixture should still validate green: %v", changedVerdict.Defects())
	}

	spans, _ := validate.Chain(identity.CellF00)
	for _, span := range spans {
		before, _ := baseVerdict.Conservation.StageSummary(span.Name)
		after, _ := changedVerdict.Conservation.StageSummary(span.Name)
		delta := after.MedianNanos - before.MedianNanos

		if span.Name == bumped {
			if delta != int64(bump) {
				t.Errorf("%s moved by %dns, want %dns", span.Name, delta, int64(bump))
			}
			continue
		}
		if span.Name == validate.StageQBackend {
			// A longer execution makes later members wait longer, by construction.
			// That the increase lands here and nowhere else is the point.
			if delta <= 0 {
				t.Error("Q_backend did not absorb the extra serialization wait a longer S_comp creates")
			}
			continue
		}
		if delta != 0 {
			t.Errorf("%s moved by %dns; only %s should have changed", span.Name, delta, bumped)
		}
	}
}

// The residual is exactly the in-cycle time the cycle model does not name, and
// nothing else. Checking the identity rather than checking for zero is what
// keeps the conservation test from degenerating into an arithmetic tautology.
func TestResidualIsExactlyTheUnnamedTime(t *testing.T) {
	for _, cell := range []identity.Cell{identity.CellD0, identity.CellF00} {
		t.Run(string(cell), func(t *testing.T) {
			spec := specWithInjectedDelays(cell)
			fixture := spec.MustBuild()
			verdict := validate.Validate(fixture.Expectation, fixture.Records)

			if !verdict.Passed {
				t.Fatalf("fixture should validate green: %v", verdict.Defects())
			}

			spans, _ := validate.Chain(cell)
			var wantResidual int64
			var named []string
			for i, span := range spans {
				if validate.PreCycle(spans, i) || span.Accounted {
					continue
				}
				wantResidual += int64(spec.Delay(span.Name))
				named = append(named, span.Name)
			}

			for _, rc := range verdict.Conservation.Requests {
				if rc.ResidualNanos != wantResidual {
					t.Fatalf("%v: residual %+dns, want %+dns (the unnamed in-cycle spans %v)",
						rc.Request, rc.ResidualNanos, wantResidual, named)
				}
				if rc.AccountedNanos+rc.UnaccountedNanos != rc.EndToEndNanos {
					t.Fatalf("%v: %d accounted + %d unaccounted != %d end to end",
						rc.Request, rc.AccountedNanos, rc.UnaccountedNanos, rc.EndToEndNanos)
				}
			}
		})
	}
}

// D0's cycle is fully named, so its residual closes exactly. F00's is not: the
// adapter's unpack and response-pack costs are real time inside the cycle that
// the model does not name, and they must show up rather than being absorbed.
func TestResidualIsZeroOnlyWhenTheCycleIsFullyNamed(t *testing.T) {
	d0 := specWithInjectedDelays(identity.CellD0).MustBuild()
	if got := validate.Validate(d0.Expectation, d0.Records).Conservation.MaxAbsResidualNanos; got != 0 {
		t.Errorf("D0's cycle is fully named; residual = %dns, want 0", got)
	}

	f00 := specWithInjectedDelays(identity.CellF00).MustBuild()
	if got := validate.Validate(f00.Expectation, f00.Records).Conservation.MaxAbsResidualNanos; got == 0 {
		t.Error("F00 has unnamed in-cycle time; a zero residual would mean it was absorbed somewhere")
	}
}

// The conserved interval is t15 − t2. What happens before the client sends —
// formation and the load generator's own dispatch latency — is measured and
// reported, but a spike there is a fact about the harness, not unaccounted time
// inside the cycle, and it must not fail the run.
func TestPreSendStagesAreReportedButNotConserved(t *testing.T) {
	const spike = 3 * time.Millisecond

	fixture := specWithInjectedDelays(identity.CellD0).
		WithDelay(validate.StageReleaseToSend, spike).MustBuild()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)

	if !verdict.Passed {
		t.Fatalf("a spike before the client send must not fail conservation: %v", verdict.Defects())
	}
	summary, ok := verdict.Conservation.StageSummary(validate.StageReleaseToSend)
	if !ok {
		t.Fatal("the pre-send stage should still be summarized")
	}
	if summary.MedianNanos != int64(spike) {
		t.Errorf("release_to_send median = %dns, want the injected %dns — it must still be measured",
			summary.MedianNanos, int64(spike))
	}
}

// In-cycle time beyond the declared tolerance fails the run, and the residual is
// reported signed rather than relabeled as a stage.
func TestUnaccountedTimeBeyondToleranceFailsTheRun(t *testing.T) {
	// An adapter unpack cost of 2ms against a cycle of roughly 6.5ms is far past
	// the 5% tolerance — the kind of thing that happens when a process starts
	// spending real time somewhere the cycle model does not model.
	const inflated = 2 * time.Millisecond

	fixture := specWithInjectedDelays(identity.CellF00).
		WithDelay(validate.StageAdapterUnpack, inflated).MustBuild()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)

	if verdict.Passed {
		t.Fatal("a cycle with unaccounted time beyond tolerance must not validate green")
	}
	if !verdict.HasDefect(validate.DefectConservation) {
		t.Errorf("the verdict should name the conservation defect, got: %v", verdict.Defects())
	}

	wantResidual := int64(inflated) + int64(injectedDelays()[validate.StageResponsePack])
	for _, rc := range verdict.Conservation.Requests {
		if rc.ResidualNanos != wantResidual {
			t.Errorf("%v: residual %+dns, want %+dns", rc.Request, rc.ResidualNanos, wantResidual)
		}
		// And it must not have been quietly folded into a neighbouring stage.
		// S_comp is checked rather than Q_backend because Q_backend legitimately
		// varies with a member's position behind earlier executions.
		if got, want := rc.Stages[validate.StageSComp], int64(injectedDelays()[validate.StageSComp]); got != want {
			t.Errorf("%v: S_comp = %dns, want %dns — unaccounted time leaked into a named stage",
				rc.Request, got, want)
		}
	}
}

// Cohort accounting for multi-RPC cells is interval-based: members overlap, so
// their spans are never summed. The union has to be shorter than the sum.
func TestMultiRPCCohortAccountingUsesIntervalsNotSums(t *testing.T) {
	fixture := specWithInjectedDelays(identity.CellF00).MustBuild()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)

	if validate.AdditiveConservation(identity.CellF00) {
		t.Fatal("F00 is a multi-RPC cell; its stages must not be summed additively")
	}
	if len(verdict.Conservation.Cohorts) != fixture.Spec.CohortCount {
		t.Fatalf("accounted %d cohorts, want %d", len(verdict.Conservation.Cohorts), fixture.Spec.CohortCount)
	}

	for _, c := range verdict.Conservation.Cohorts {
		if c.UncoveredNanos != 0 {
			t.Errorf("cohort %d leaves %dns of its makespan uncovered", c.Cohort, c.UncoveredNanos)
		}
		// The members are staggered but overlapping, so summing their spans
		// overstates the makespan — which is exactly why they are not summed.
		if c.SumOfSpansNanos <= c.MakespanNanos {
			t.Errorf("cohort %d: sum of member spans %dns does not exceed the makespan %dns; "+
				"the fixture is not exercising overlap", c.Cohort, c.SumOfSpansNanos, c.MakespanNanos)
		}
		if c.CoveredNanos != c.MakespanNanos {
			t.Errorf("cohort %d: covered %dns of a %dns makespan", c.Cohort, c.CoveredNanos, c.MakespanNanos)
		}
	}
}

// A verdict has to be reproducible from the archive alone: same input, same
// finding, every time.
func TestVerdictIsReproducible(t *testing.T) {
	fixture := specWithInjectedDelays(identity.CellF00).MustBuild()

	first := validate.Validate(fixture.Expectation, fixture.Records)
	second := validate.Validate(fixture.Expectation, fixture.Records)

	if first.Passed != second.Passed {
		t.Fatalf("verdicts disagree: %v then %v", first.Passed, second.Passed)
	}
	if len(first.Checks) != len(second.Checks) {
		t.Fatalf("check counts differ: %d then %d", len(first.Checks), len(second.Checks))
	}
	for i := range first.Checks {
		if first.Checks[i].Name != second.Checks[i].Name || first.Checks[i].Passed != second.Checks[i].Passed {
			t.Errorf("check %d differs: %+v vs %+v", i, first.Checks[i], second.Checks[i])
		}
	}
	if first.Conservation.MaxAbsResidualNanos != second.Conservation.MaxAbsResidualNanos {
		t.Error("conservation residuals differ between runs of the same input")
	}
}

// W_form is the cycle model's formation term and belongs to the proxy. The load
// generator's barrier wait is a different quantity that happens to sit between
// the same two timestamps at A=off.
//
// They were the same name once, and nothing caught it: a delivered D0 bundle —
// a cell with no proxy at all — reported a W_form median of 68 microseconds,
// while M1 §2.2's stage table gives D0 a dash in that row. One archive column
// meant two unrelated things, keyed by a cell label no check consulted. This is
// the check.
func TestFormationAndBarrierWaitAreNeverTheSameName(t *testing.T) {
	for _, cell := range identity.AllCells() {
		spans, err := validate.Chain(cell)
		if err != nil {
			continue // cells with no chain yet
		}

		var hasFormation, hasBarrierWait bool
		for _, s := range spans {
			switch s.Name {
			case validate.StageWForm:
				hasFormation = true
				if s.Start != events.StageProxyRecv {
					t.Errorf("%s: W_form starts at %s; formation is measured at the proxy, from the member's arrival",
						cell, s.Start)
				}
			case validate.StageBarrierWait:
				hasBarrierWait = true
				if s.Start != events.StageSched {
					t.Errorf("%s: barrier_wait starts at %s, want t_sched", cell, s.Start)
				}
			}
		}

		if hasFormation && hasBarrierWait {
			t.Errorf("%s carries both W_form and barrier_wait; no cell has both", cell)
		}
		if want := cell.AggregatesEnvelopes(); hasFormation != want {
			t.Errorf("%s: W_form present = %v, but the cell aggregates = %v — formation exists exactly where the proxy assembles a cohort",
				cell, hasFormation, want)
		}
		if hasBarrierWait == cell.AggregatesEnvelopes() {
			t.Errorf("%s: barrier_wait present = %v for an aggregating = %v cell; the generator's wait is the A=off quantity",
				cell, hasBarrierWait, cell.AggregatesEnvelopes())
		}
	}
}

// Neither quantity may enter the conserved sum: both sit before the client send,
// which is where the conservation identity starts (M2-PLAN §4.3).
func TestPreSendQuantitiesAreNeverAccounted(t *testing.T) {
	for _, cell := range identity.AllCells() {
		spans, err := validate.Chain(cell)
		if err != nil {
			continue
		}
		for i, s := range spans {
			if !validate.PreCycle(spans, i) {
				continue
			}
			if s.Accounted {
				t.Errorf("%s: %s is before the client send but marked accounted; it would be summed into a cycle it is not part of",
					cell, s.Name)
			}
		}
	}
}

// The conservation class each cell carries, pinned against ADR-0009 so that the
// decision record and the code cannot drift apart again — they already did once,
// with the ADR committed while the function still called F10 additive.
func TestConservationClassMatchesTheDecisionRecord(t *testing.T) {
	for cell, additive := range map[identity.Cell]bool{
		identity.CellD0:     false,
		identity.CellF00:    false,
		identity.CellF01:    false,
		identity.CellF00Seq: false,
		// One envelope, but B concurrent executions serializing on one instance:
		// B paths, so the members' stages overlap and are never summed (ADR-0009).
		identity.CellF10: false,
		// One request, one execution, one path.
		identity.CellF11P: true,
		// Classified additive by ADR-0009; spec 0003 must re-derive it before the
		// cell runs.
		identity.CellF11D: true,
	} {
		if got := validate.AdditiveConservation(cell); got != additive {
			t.Errorf("%s: additive = %v, ADR-0009 says %v", cell, got, additive)
		}
	}
}

// F10's accounting is stated over the cohort, not over its members.
//
// The cohort's chain is the same chain a member walks, evaluated at the instants
// on the cohort's own critical path — earliest until the backend, latest after
// it. Those tile the makespan exactly, so what is left over is the time the
// cycle model does not name and nothing else. If it were not exact, the residual
// would be absorbing the cohort's own serialization and the number would move
// with B rather than with the instrument.
func TestCohortResidualIsExactlyTheUnnamedTime(t *testing.T) {
	spec := specWithInjectedDelays(identity.CellF10)
	fixture := spec.MustBuild()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)
	if !verdict.Passed {
		t.Fatalf("a well-formed F10 fixture must validate green: %v", verdict.Defects())
	}

	spans, _ := validate.Chain(identity.CellF10)
	var want int64
	var named []string
	for i, span := range spans {
		if validate.PreCycle(spans, i) || span.Accounted {
			continue
		}
		want += int64(spec.Delay(span.Name))
		named = append(named, span.Name)
	}

	if len(verdict.Conservation.Cohorts) != spec.CohortCount {
		t.Fatalf("decomposed %d cohorts, want %d", len(verdict.Conservation.Cohorts), spec.CohortCount)
	}
	for _, c := range verdict.Conservation.Cohorts {
		if c.ResidualNanos != want {
			t.Errorf("cohort %d: residual %+dns, want %+dns (the unnamed in-cycle spans %v)",
				c.Cohort, c.ResidualNanos, want, named)
		}
		if c.AccountedNanos+c.UnaccountedNanos != c.MakespanNanos {
			t.Errorf("cohort %d: %d accounted + %d unaccounted != %d makespan",
				c.Cohort, c.AccountedNanos, c.UnaccountedNanos, c.MakespanNanos)
		}
	}
}

// An envelope-granularity stage happens once per cohort and appears in all B
// records. Counting it per member would multiply a one-off cost by cohort size —
// and the number would still look plausible, because it would move with B in the
// same direction the aggregation effect does.
func TestEnvelopeStagesAreCountedOncePerCohort(t *testing.T) {
	spec := specWithInjectedDelays(identity.CellF10)
	fixture := spec.MustBuild()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)

	// A_pack runs from the seal to the send, both of which the cohort shares, so
	// it is a cohort cost outright.
	const stage = validate.StageAPack
	want := int64(spec.Delay(stage))

	var cohortTotal int64
	for _, c := range verdict.Conservation.Cohorts {
		got, ok := c.Stages[stage]
		if !ok {
			t.Fatalf("cohort %d has no %s", c.Cohort, stage)
		}
		if got != want {
			t.Errorf("cohort %d: %s = %dns, injected %dns", c.Cohort, stage, got, want)
		}
		cohortTotal += got
	}

	// The naive alternative, spelled out: summing the members' records reports the
	// same interval B times.
	var memberTotal int64
	for _, rc := range verdict.Conservation.Requests {
		memberTotal += rc.Stages[stage]
	}
	if memberTotal != cohortTotal*int64(spec.CohortSize) {
		t.Fatalf("the per-member sum of %s is %dns against a cohort total of %dns; the point of this test is that it is exactly %d times larger",
			stage, memberTotal, cohortTotal, spec.CohortSize)
	}
	if cohortTotal != want*int64(spec.CohortCount) {
		t.Errorf("%s totals %dns over %d cohorts, want %dns each", stage, cohortTotal, spec.CohortCount, want)
	}
}

// Formation wait is reported at both granularities, and they are not two
// observations. The cohort's value is identically the largest of the members' —
// the member that arrived first is the member that waited longest — so their
// agreement establishes nothing either one did not already say. This test states
// the relationship so that nobody later reads it as corroboration.
func TestFormationWaitIsReportedAtBothGranularities(t *testing.T) {
	spec := specWithInjectedDelays(identity.CellF10)
	fixture := spec.MustBuild()
	verdict := validate.Validate(fixture.Expectation, fixture.Records)

	perMember := map[identity.CohortID][]int64{}
	for _, rc := range verdict.Conservation.Requests {
		perMember[rc.Request.Cohort] = append(perMember[rc.Request.Cohort], rc.Stages[validate.StageWForm])
	}

	for _, c := range verdict.Conservation.Cohorts {
		waits := perMember[c.Cohort]
		if len(waits) != spec.CohortSize {
			t.Fatalf("cohort %d reports %d member formation waits, want %d", c.Cohort, len(waits), spec.CohortSize)
		}

		var largest, smallest int64 = waits[0], waits[0]
		for _, w := range waits {
			if w > largest {
				largest = w
			}
			if w < smallest {
				smallest = w
			}
		}
		if c.FormationWaitNanos != largest {
			t.Errorf("cohort %d: formation wait %dns, largest member wait %dns — the cohort's value is that member's, not a second measurement",
				c.Cohort, c.FormationWaitNanos, largest)
		}
		// And the two really do differ across members, or the relationship above
		// would be vacuous.
		if largest == smallest {
			t.Errorf("cohort %d: every member reports formation wait %dns; the members did not arrive at different instants", c.Cohort, largest)
		}
		if want := int64(spec.Delay(validate.StageWForm)); smallest != want {
			t.Errorf("cohort %d: shortest member wait %dns, injected %dns — that member arrived last and waited for nobody",
				c.Cohort, smallest, want)
		}
	}
}

// The executions of an F10 cohort serialize on the single model instance exactly
// as F00's do: one envelope in, B executions out, and the backend runs them one
// after another. Aggregating the transport changes nothing about that, and the
// check that establishes it has to apply here too — otherwise F10 would be the
// one cell where overlapping executions went unnoticed.
func TestExecutionSerializationHoldsForF10(t *testing.T) {
	good := specWithInjectedDelays(identity.CellF10).MustBuild()
	if v := validate.Validate(good.Expectation, good.Records); !v.Passed {
		t.Fatalf("a well-formed F10 fixture must validate green: %v", v.Defects())
	}

	overlapped := good.Clone().WithOverlappingExecutions()
	v := validate.Validate(overlapped.Expectation, overlapped.Records)
	if v.Passed {
		t.Fatal("an F10 cohort whose executions overlapped was accepted; V=off means B executions in sequence")
	}
	if !v.HasDefect(validate.DefectOverlappingExecutions) {
		t.Errorf("the overlap was not named as one: %v", v.Defects())
	}
}
