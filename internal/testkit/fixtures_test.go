package testkit

import (
	"testing"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/validate"
)

// The fixtures are the evidence the offline assertions are tested against, so a
// fixture that describes a run no process could have performed does not weaken
// those assertions — it silently satisfies them. These tests are about the
// evidence itself, before any validator sees it.

// One envelope carries the cohort at A=on, and every member reports its id and
// its timestamps. A fixture that minted an envelope per member would be A=off
// evidence wearing an A=on label, and the cardinality check that exists to catch
// exactly that would pass against it.
func TestAggregateFixtureGivesACohortOneEnvelopeAndOneSeal(t *testing.T) {
	spec := NewSpec(identity.CellF10)
	fixture := spec.MustBuild()

	for c := 0; c < spec.CohortCount; c++ {
		cohort := spec.FirstCohortID + identity.CohortID(c)

		envelopes := map[identity.EnvelopeID]bool{}
		values := map[events.Stage]map[int64]bool{}
		members := map[identity.Ordinal]bool{}

		for _, d := range fixture.Records {
			rec := d.Record
			if rec.Cohort != cohort {
				continue
			}
			members[rec.Ordinal] = true
			if rec.EnvelopeID != 0 {
				envelopes[rec.EnvelopeID] = true
			}
			for _, stage := range validate.EnvelopeStages().Stages() {
				if v, ok := rec.Stage(stage); ok {
					if values[stage] == nil {
						values[stage] = map[int64]bool{}
					}
					values[stage][v] = true
				}
			}
		}

		if len(members) != spec.CohortSize {
			t.Fatalf("cohort %d has %d members, want %d", cohort, len(members), spec.CohortSize)
		}
		if len(envelopes) != 1 {
			t.Errorf("cohort %d travelled in %d envelopes, want 1", cohort, len(envelopes))
		}
		for _, stage := range validate.EnvelopeStages().Stages() {
			if n := len(values[stage]); n != 1 {
				t.Errorf("cohort %d carries %d distinct values of %s, want 1", cohort, n, stage)
			}
		}
	}
}

// Formation wait is per member and the seal is not. The first member to arrive
// waits longest, so the injected W_form is the floor of a cohort's spread rather
// than every member's value — a fixture where all B agreed would be describing a
// cohort whose members arrived at the same instant, which is the one case that
// makes formation wait unmeasurable.
func TestAggregateFixtureGivesEachMemberItsOwnFormationWait(t *testing.T) {
	spec := NewSpec(identity.CellF10)
	fixture := spec.MustBuild()
	cohort := spec.FirstCohortID

	waits := map[int64]bool{}
	var floor int64 = -1
	for _, d := range fixture.Records {
		rec := d.Record
		if rec.Cohort != cohort {
			continue
		}
		recv, okRecv := rec.Stage(events.StageProxyRecv)
		seal, okSeal := rec.Stage(events.StageCohortSeal)
		if !okRecv || !okSeal {
			continue
		}
		wait := seal - recv
		if wait < 0 {
			t.Errorf("%v was sealed %dns before it arrived", rec.Request(), -wait)
		}
		waits[wait] = true
		if floor < 0 || wait < floor {
			floor = wait
		}
	}

	if len(waits) != spec.CohortSize {
		t.Errorf("cohort %d reports %d distinct formation waits, want one per member", cohort, len(waits))
	}
	// The last member to arrive waits only the injected time; nobody waits less.
	if want := int64(spec.Delay(validate.StageWForm)); floor != want {
		t.Errorf("the shortest formation wait is %dns, injected %dns — that member arrived last and waited for nobody", floor, want)
	}
}

// A vectorized cohort is one execution, so its members share one window. Giving
// them separate windows would describe B executions wearing a V=on label, and
// the coalescing evidence would agree with it.
func TestVectorizedFixtureGivesACohortOneExecutionWindow(t *testing.T) {
	spec := NewSpec(identity.CellF11D)
	fixture := spec.MustBuild()
	cohort := spec.FirstCohortID

	windows := map[[2]int64]bool{}
	for _, d := range fixture.Records {
		rec := d.Record
		if rec.Cohort != cohort {
			continue
		}
		start, okStart := rec.Stage(events.StageComputeStart)
		end, okEnd := rec.Stage(events.StageComputeEnd)
		if okStart && okEnd {
			windows[[2]int64{start, end}] = true
		}
	}
	if len(windows) != 1 {
		t.Errorf("cohort %d has %d distinct execution windows, want 1", cohort, len(windows))
	}
}

// At A=off nothing is shared, and that is the contrast. Each logical request
// travels in its own envelope, so the ids are all distinct — which is what makes
// counting them a measurement of Factor A rather than of cohort size.
func TestIndependentFixtureGivesEachMemberItsOwnEnvelope(t *testing.T) {
	spec := NewSpec(identity.CellF00)
	fixture := spec.MustBuild()

	envelopes := map[identity.EnvelopeID]bool{}
	var carried int
	for _, d := range fixture.Records {
		if id := d.Record.EnvelopeID; id != 0 {
			envelopes[id] = true
			carried++
		}
	}
	if carried == 0 {
		t.Fatal("no A=off record carries an envelope id")
	}
	if want := spec.CohortCount * spec.CohortSize; len(envelopes) != want {
		t.Errorf("%d distinct envelopes for %d logical requests, want one each", len(envelopes), want)
	}
}

// The builder's self-check has to be able to fail, or it is a comment. Each
// mutation below is a fixture that would otherwise satisfy every A=on assertion
// while describing evidence no proxy could emit.
func TestTheBuilderRefusesEvidenceNoProxyCouldEmit(t *testing.T) {
	for name, spoil := range map[string]func(Bundle) Bundle{
		"a member with its own seal": func(b Bundle) Bundle {
			return withStageBumped(b, identity.EmitterProxy, events.StageCohortSeal)
		},
		"a member with its own send": func(b Bundle) Bundle {
			return withStageBumped(b, identity.EmitterProxy, events.StageProxySend)
		},
		"a member with its own adapter receipt": func(b Bundle) Bundle {
			return withStageBumped(b, identity.EmitterAdapter, events.StageAdapterRecv)
		},
		"a member in its own envelope": func(b Bundle) Bundle {
			b = b.Clone()
			for i, d := range b.Records {
				if d.Record.Emitter == identity.EmitterProxy && d.Record.Ordinal == 0 {
					b.Records[i].Record.EnvelopeID += 1000
					break
				}
			}
			return b
		},
	} {
		t.Run(name, func(t *testing.T) {
			good := NewSpec(identity.CellF10).MustBuild()
			if err := good.checkGranularity(); err != nil {
				t.Fatalf("the control fixture must pass: %v", err)
			}
			if err := spoil(good).checkGranularity(); err == nil {
				t.Errorf("%s was accepted; the check would not have caught the fake it exists for", name)
			}
		})
	}
}

// withStageBumped moves one member's value for a stage the cohort is supposed to
// share, which is what a per-member-sealing proxy would produce.
func withStageBumped(b Bundle, emitter identity.Emitter, stage events.Stage) Bundle {
	b = b.Clone()
	for i, d := range b.Records {
		if d.Record.Emitter != emitter || d.Record.Ordinal != 0 {
			continue
		}
		if v, ok := d.Record.Stage(stage); ok {
			b.Records[i].Record.SetStage(stage, v+1)
			break
		}
	}
	return b
}
