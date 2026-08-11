package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/modelrepo"
)

// The manifest is the only thing that authorizes a run configuration, so its
// validation is not a formality: a manifest that declares a cell it cannot
// realize, or expectations its own cell contradicts, has to fail before Triton
// is touched rather than after a run has produced uninterpretable numbers.

func validD0() Manifest {
	return Manifest{
		SchemaVersion: SchemaVersion,
		Experiment:    "exp-1",
		Session:       "sess-1",
		Run:           "run-1",
		Cell:          identity.CellD0,
		Environment:   "envV",
		Cohort:        Cohort{Size: 4, Count: 25, WarmupCount: 3},
		Model: Model{
			Catalog:    "artifacts/catalog.json",
			RuntimeDir: "results/runtime-models",
			ArtifactID: "synthetic_k8_p65536",
			Entry:      modelrepo.EntryUnbatched,
		},
		Workload:  Workload{ReleaseMode: ReleaseFixedCohort, RequestTimeoutMillis: 30000},
		Transport: Transport{TritonEndpoint: "127.0.0.1:8001", MaxMessageBytes: 256 << 20},
		GC:        GC{GOGC: 100, GOMEMLIMIT: 2 << 30},
		Tracing:   Tracing{Mode: "timestamps", TraceDir: "results/triton-traces"},
		Output:    Output{BundleDir: "results/bundles"},
		ExpectedEvidence: ExpectedEvidence{
			Executions: 100, ExecutionsPerCohort: 4, BatchSize: 1,
		},
		Conservation: Conservation{ToleranceFraction: 0.05},
	}
}

func validF00() Manifest {
	m := validD0()
	m.Cell = identity.CellF00
	m.Transport.ProxyEndpoint = "127.0.0.1:9100"
	m.Transport.AdapterEndpoint = "127.0.0.1:9101"
	m.ExpectedEvidence.MaxAdapterDispatchWaitNanos = 1_000_000
	return m
}

// validF10 is the A=on manifest. Its proxy holds a cohort while it assembles,
// so it declares a formation deadline, and its adapter releases B members in one
// fan-out, so it declares how far apart those submissions may be.
func validF10() Manifest {
	m := validF00()
	m.Cell = identity.CellF10
	m.Run = "run-f10-test"
	m.Cohort.FormationDeadlineMillis = 5_000
	m.ExpectedEvidence.MaxDispatchSkewNanos = 250_000
	return m
}

// validFor returns a manifest shaped for the cell, so a sweep over the design
// fails an unimplemented cell for being unimplemented rather than for carrying
// the wrong shape.
func validFor(cell identity.Cell) Manifest {
	var m Manifest
	switch {
	case !cell.UsesProxy():
		m = validD0()
	case cell.AggregatesEnvelopes():
		m = validF10()
	default:
		m = validF00()
	}
	m.Cell = cell
	return m
}

func TestValidManifestsPass(t *testing.T) {
	for name, m := range map[string]Manifest{"D0": validD0(), "F00": validF00(), "F10": validF10()} {
		if err := m.Validate(); err != nil {
			t.Errorf("%s manifest should validate: %v", name, err)
		}
	}
}

// The repository's own manifests are data reviewed like code; they must parse
// and validate.
func TestRepositoryManifestsValidate(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "experiments", "manifests", "*.json"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(paths) == 0 {
		t.Skip("no manifests in the repository yet")
	}
	for _, path := range paths {
		if _, err := Load(path); err != nil {
			t.Errorf("%s: %v", filepath.Base(path), err)
		}
	}
}

// A cell's declared factor levels and its target model entry have to agree. A
// V=off cell aimed at the dynamic entry would run batched executions while still
// carrying the V=off label.
func TestExecutorCellCompatibilityIsEnforced(t *testing.T) {
	m := validD0()
	m.Model.Entry = modelrepo.EntryDynamic
	err := m.Validate()
	if err == nil {
		t.Fatal("a V=off cell targeting the dynamic entry must be refused")
	}
	if !strings.Contains(err.Error(), "V=off") {
		t.Errorf("the error should explain the factor mismatch, got: %v", err)
	}
}

// The expected evidence tuple has to be consistent with the cell's contract.
func TestExpectedEvidenceMustMatchTheCellContract(t *testing.T) {
	for name, mutate := range map[string]func(*Manifest){
		"executions do not multiply out": func(m *Manifest) { m.ExpectedEvidence.Executions = 99 },
		"V=off cohort produces B executions": func(m *Manifest) {
			m.ExpectedEvidence.ExecutionsPerCohort = 1
			m.ExpectedEvidence.Executions = 25
		},
		"V=off executions have batch size 1": func(m *Manifest) { m.ExpectedEvidence.BatchSize = 4 },
	} {
		m := validD0()
		mutate(&m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s: should have been refused", name)
		}
	}
}

// D0 is the direct path and must not name a proxy; every factorial cell must.
func TestPathEndpointsMustMatchTheCellTopology(t *testing.T) {
	m := validD0()
	m.Transport.ProxyEndpoint = "127.0.0.1:9100"
	if err := m.Validate(); err == nil {
		t.Error("D0 is the direct path and must not name a proxy endpoint")
	}

	f := validF00()
	f.Transport.ProxyEndpoint = ""
	if err := f.Validate(); err == nil {
		t.Error("F00 traverses the shared path and needs a proxy endpoint")
	}
}

// Cells beyond this slice parse but must not run: a manifest naming one fails
// visibly rather than falling back to something that looks like a result.
// The manifest validator consults the one authority rather than a list of its
// own. A second list here could disagree with the runner's, and the
// disagreement would surface as a manifest that validated and then failed after
// a model repository had been materialized.
func TestUnimplementedCellsAreRefused(t *testing.T) {
	for _, cell := range identity.AllCells() {
		m := validFor(cell)

		err := m.Validate()
		switch {
		case cell.Implemented() && err != nil:
			t.Errorf("cell %s is implemented and must be accepted: %v", cell, err)
		case !cell.Implemented() && err == nil:
			t.Errorf("cell %s is not implemented and must be refused", cell)
		case !cell.Implemented() && !strings.Contains(err.Error(), string(cell)):
			t.Errorf("cell %s: refusal %q should name the cell", cell, err)
		}
	}
}

// Formation exists exactly where the proxy aggregates. Both directions are
// refused: a deadline where nothing is assembled would declare a stage the run
// does not have, and its absence where a cohort is held would leave the bound to
// a code default nobody wrote down (ADR-0010).
func TestFormationDeadlineExistsExactlyWhereTheProxyAggregates(t *testing.T) {
	for _, cell := range identity.ImplementedCells() {
		m := validFor(cell)

		// Take the deadline away.
		absent := m
		absent.Cohort.FormationDeadlineMillis = 0
		err := absent.Validate()
		if cell.AggregatesEnvelopes() && err == nil {
			t.Errorf("cell %s holds a cohort and must declare a formation deadline", cell)
		}
		if !cell.AggregatesEnvelopes() && err != nil {
			t.Errorf("cell %s forms no cohort and needs no deadline: %v", cell, err)
		}

		// Give it one it should not have.
		present := m
		present.Cohort.FormationDeadlineMillis = 5_000
		err = present.Validate()
		if !cell.AggregatesEnvelopes() {
			if err == nil {
				t.Errorf("cell %s forms no cohort at the proxy and must not declare a formation deadline", cell)
			} else if !strings.Contains(err.Error(), "formation_deadline_millis") {
				t.Errorf("cell %s: refusal %q should name the parameter", cell, err)
			}
		}
	}
}

// The formation deadline shares a context with the client's request deadline, so
// it has to fire first. Otherwise the client cancels, the held members' contexts
// die, and the cohort is torn down through a path that has no name — leaving the
// formation-failure diagnosis unreachable (ADR-0010).
func TestFormationDeadlineMustBeStrictlyShorterThanTheRequestDeadline(t *testing.T) {
	for name, deadline := range map[string]int{
		"equal to the request deadline": 30_000,
		"longer than it":                30_001,
	} {
		m := validF10()
		m.Cohort.FormationDeadlineMillis = deadline
		m.Workload.RequestTimeoutMillis = 30_000

		err := m.Validate()
		if err == nil {
			t.Errorf("%s: should have been refused", name)
			continue
		}
		if !strings.Contains(err.Error(), "strictly shorter") {
			t.Errorf("%s: refusal %q should say why the ordering matters", name, err)
		}
	}

	m := validF10()
	m.Cohort.FormationDeadlineMillis = 29_999
	m.Workload.RequestTimeoutMillis = 30_000
	if err := m.Validate(); err != nil {
		t.Errorf("a deadline one millisecond shorter is strictly shorter: %v", err)
	}
}

// A serial fan-out produces the same execution count, the same histogram and the
// same attested membership as a correct one; only the skew betrays it. The bound
// it is judged against is declared, not defaulted.
func TestFanOutCellsMustDeclareTheDispatchSkewBound(t *testing.T) {
	for _, cell := range identity.ImplementedCells() {
		m := validFor(cell)
		m.ExpectedEvidence.MaxDispatchSkewNanos = 0

		err := m.Validate()
		switch {
		case cell.AggregatesEnvelopes() && err == nil:
			t.Errorf("cell %s releases its cohort in one fan-out and must bound its skew", cell)
		case cell.AggregatesEnvelopes() && !strings.Contains(err.Error(), "max_dispatch_skew_nanos"):
			t.Errorf("cell %s: refusal %q should name the parameter", cell, err)
		case !cell.AggregatesEnvelopes() && err != nil:
			t.Errorf("cell %s releases one member per dispatch and has nothing to skew: %v", cell, err)
		}
	}
}

// F10 is V=off: a cohort produces B executions of shape [1,…], exactly as F00
// does. Only the transport differs, and a manifest that said otherwise would be
// declaring a different cell.
func TestF10DeclaresTheSameExecutionShapeAsF00(t *testing.T) {
	// Asserting the fixture's own literals back at itself would prove nothing —
	// no change to Validate could fail it. What is asserted is the refusal.
	for name, mutate := range map[string]func(*Manifest){
		"a cohort as one execution": func(m *Manifest) { m.ExpectedEvidence.ExecutionsPerCohort = 1 },
		"a batched execution":       func(m *Manifest) { m.ExpectedEvidence.BatchSize = m.Cohort.Size },
	} {
		bad := validF10()
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Errorf("%s: F10 is V=off and must refuse it", name)
		}
	}
}

// The pinned constants are treatment definition, not defaults to fall back on.
func TestPinnedConstantsAreRequired(t *testing.T) {
	for name, mutate := range map[string]func(*Manifest){
		"gRPC ceiling": func(m *Manifest) { m.Transport.MaxMessageBytes = 0 },
		"GOGC":         func(m *Manifest) { m.GC.GOGC = 0 },
		"GOMEMLIMIT":   func(m *Manifest) { m.GC.GOMEMLIMIT = 0 },
		"trace dir":    func(m *Manifest) { m.Tracing.TraceDir = "" },
		"tolerance":    func(m *Manifest) { m.Conservation.ToleranceFraction = 0 },
		"timeout":      func(m *Manifest) { m.Workload.RequestTimeoutMillis = 0 },
	} {
		m := validD0()
		mutate(&m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s must be pinned in the manifest", name)
		}
	}
}

// An adapter-bearing cell must declare the dispatch-wait bound the validator
// judges it against.
func TestSharedPathCellsMustDeclareTheAdapterDispatchBound(t *testing.T) {
	m := validF00()
	m.ExpectedEvidence.MaxAdapterDispatchWaitNanos = 0
	if err := m.Validate(); err == nil {
		t.Error("a cell traversing the adapter must declare its dispatch-wait bound")
	}
}

// A key this build does not understand means a setting the author meant to
// apply would be silently ignored.
func TestUnknownFieldsAreRefused(t *testing.T) {
	m := validD0()
	blob, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(blob, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	raw["max_queue_delay_micros"] = 500
	blob, _ = json.Marshal(raw)

	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Error("an unrecognized manifest key must be refused, not ignored")
	}
}

func TestSchemaVersionMismatchIsRefused(t *testing.T) {
	m := validD0()
	m.SchemaVersion = SchemaVersion + 1
	if err := m.Validate(); err == nil {
		t.Error("a manifest from another protocol revision must be refused")
	}
}
