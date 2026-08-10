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

func TestValidManifestsPass(t *testing.T) {
	for name, m := range map[string]Manifest{"D0": validD0(), "F00": validF00()} {
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
func TestUnimplementedCellsAreRefused(t *testing.T) {
	for _, cell := range []identity.Cell{identity.CellF01, identity.CellF10, identity.CellF11D, identity.CellF11P} {
		m := validF00()
		m.Cell = cell
		if err := m.Validate(); err == nil {
			t.Errorf("cell %s is not implemented and must be refused", cell)
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
