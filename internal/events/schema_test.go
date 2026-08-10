package events

import (
	"testing"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// D0 is the direct path: proxy and adapter stages do not exist for it. That is a
// different fact from a timestamp going missing, and the topology mask is where
// the difference is typed (ADR-0005).
func TestD0TopologyExcludesProxyAndAdapterStages(t *testing.T) {
	mask, err := TopologyMask(identity.CellD0)
	if err != nil {
		t.Fatalf("topology mask: %v", err)
	}

	present := []Stage{
		StageSched, StageClientSend, StageCohortSeal,
		StageQueueStart, StageComputeStart, StageComputeEnd, StageClientRecv,
	}
	for _, s := range present {
		if !mask.Has(s) {
			t.Errorf("D0 topology should include %s", s)
		}
	}
	absentByTopology := []Stage{
		StageProxyRecv, StageProxySend, StageProxyRespRecv, StageProxyFanout,
		StageAdapterRecv, StageAdapterDispatch, StageAdapterResult, StageAdapterSend,
	}
	for _, s := range absentByTopology {
		if mask.Has(s) {
			t.Errorf("D0 has no %s stage: it never traverses that process", s)
		}
	}
	if got, want := mask.Len(), len(present); got != want {
		t.Errorf("D0 topology has %d stages, want %d", got, want)
	}
}

// F00 traverses the full shared path, so a complete record for it accounts for
// every timestamp in the schema.
func TestF00TopologyCoversAllFifteenStages(t *testing.T) {
	mask, err := TopologyMask(identity.CellF00)
	if err != nil {
		t.Fatalf("topology mask: %v", err)
	}
	if mask.Len() != StageCount {
		t.Fatalf("F00 topology has %d stages, want all %d: %v", mask.Len(), StageCount, mask)
	}
}

// t_cohort_seal ownership is conditional on Factor A, and the schema records the
// emitter rather than inferring it (ADR-0001).
func TestCohortSealOwnershipFollowsFactorA(t *testing.T) {
	for _, tc := range []struct {
		cell identity.Cell
		want identity.Emitter
	}{
		{identity.CellD0, identity.EmitterLoadGen},
		{identity.CellF00, identity.EmitterLoadGen},
		{identity.CellF01, identity.EmitterLoadGen},
		{identity.CellF10, identity.EmitterProxy},
		{identity.CellF11D, identity.EmitterProxy},
		{identity.CellF11P, identity.EmitterProxy},
	} {
		if got := SealOwner(tc.cell); got != tc.want {
			t.Errorf("%s seal owner = %v, want %v", tc.cell, got, tc.want)
		}
	}
}

// A process may only write the stages it can legitimately observe. In F00 the
// load generator owns the seal; in F10 the proxy does, and the load generator
// must not be able to claim it.
func TestOwnedStagesTrackSealOwnership(t *testing.T) {
	f00LoadGen := OwnedStages(identity.CellF00, identity.EmitterLoadGen)
	if !f00LoadGen.Has(StageCohortSeal) {
		t.Error("at A=off the load generator owns t_cohort_seal")
	}
	if OwnedStages(identity.CellF00, identity.EmitterProxy).Has(StageCohortSeal) {
		t.Error("at A=off the proxy emits no cohort seal: it does no joining (ADR-0001)")
	}

	if !OwnedStages(identity.CellF10, identity.EmitterProxy).Has(StageCohortSeal) {
		t.Error("at A=on the proxy owns t_cohort_seal at envelope seal")
	}
	if OwnedStages(identity.CellF10, identity.EmitterLoadGen).Has(StageCohortSeal) {
		t.Error("at A=on the load generator does not own the seal")
	}

	// D0 has no proxy or adapter, so those emitters own nothing there.
	if got := OwnedStages(identity.CellD0, identity.EmitterProxy); got != 0 {
		t.Errorf("D0 proxy owns %v, want nothing", got)
	}
	if got := OwnedStages(identity.CellD0, identity.EmitterAdapter); got != 0 {
		t.Errorf("D0 adapter owns %v, want nothing", got)
	}
}

// Every emitter's owned stages must partition the cell topology exactly: no
// stage unowned (nobody would record it), none owned twice (two processes would
// each claim the same instant).
func TestOwnedStagesPartitionTheTopology(t *testing.T) {
	emitters := []identity.Emitter{
		identity.EmitterLoadGen, identity.EmitterProxy,
		identity.EmitterAdapter, identity.EmitterTriton,
	}
	for _, cell := range []identity.Cell{identity.CellD0, identity.CellF00} {
		topology, err := TopologyMask(cell)
		if err != nil {
			t.Fatalf("%s: %v", cell, err)
		}
		var union StageMask
		var totalOwned int
		for _, e := range emitters {
			owned := OwnedStages(cell, e)
			if owned&^topology != 0 {
				t.Errorf("%s/%s owns %v outside the topology", cell, e, owned&^topology)
			}
			union |= owned
			totalOwned += owned.Len()
		}
		if union != topology {
			t.Errorf("%s: stages %v are in the topology but owned by no emitter", cell, topology&^union)
		}
		if totalOwned != topology.Len() {
			t.Errorf("%s: %d stage ownerships for %d stages — a stage is claimed twice", cell, totalOwned, topology.Len())
		}
	}
}

func TestSetStageMarksPresenceAndAbsenceReadsAsAbsent(t *testing.T) {
	var r Record
	if _, ok := r.Stage(StageComputeStart); ok {
		t.Error("an unset stage must read as absent")
	}
	// Zero is a legitimate instant, and must not be confused with absence.
	r.SetStage(StageComputeStart, 0)
	ts, ok := r.Stage(StageComputeStart)
	if !ok || ts != 0 {
		t.Errorf("stage set to 0 reads as (%d,%v), want (0,true)", ts, ok)
	}
}

func TestStageNamesMatchTheSchemaVocabulary(t *testing.T) {
	if len(AllStages()) != StageCount {
		t.Fatalf("AllStages returned %d stages, want %d", len(AllStages()), StageCount)
	}
	for _, s := range AllStages() {
		got, err := ParseStage(s.String())
		if err != nil {
			t.Errorf("stage %d name %q does not parse: %v", s, s, err)
			continue
		}
		if got != s {
			t.Errorf("%q parsed to stage %d, want %d", s, got, s)
		}
	}
}

func TestUIDInvertsToItsLogicalRequest(t *testing.T) {
	for _, want := range []identity.LogicalRequest{
		{Cohort: 0, Ordinal: 0},
		{Cohort: 1, Ordinal: 3},
		{Cohort: 999, Ordinal: 15},
		{Cohort: 4095, Ordinal: identity.MaxOrdinal},
	} {
		if got := want.UID().LogicalRequest(); got != want {
			t.Errorf("uid %d inverted to %v, want %v", want.UID(), got, want)
		}
	}
}
