// Package manifest is the only package that authorizes a run configuration.
//
// A manifest fully determines a run. Nothing experimental may be decided by a
// code default or a shell argument: if a number affects what is measured, it is
// in here, it is versioned, and it lands in the run bundle unchanged
// (ARCHITECTURE §7.2).
package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/modelrepo"
)

// SchemaVersion is the manifest format this build reads. A manifest from a
// different protocol revision is refused rather than partially understood.
const SchemaVersion = 1

// Manifest is one run's complete configuration.
type Manifest struct {
	SchemaVersion int    `json:"schema_version"`
	Description   string `json:"description,omitempty"`

	Experiment  identity.ExperimentID `json:"experiment_id"`
	Session     identity.SessionID    `json:"session_id"`
	Run         identity.RunID        `json:"run_id"`
	Cell        identity.Cell         `json:"cell"`
	Environment string                `json:"environment"`

	Cohort    Cohort    `json:"cohort"`
	Model     Model     `json:"model"`
	Workload  Workload  `json:"workload"`
	Transport Transport `json:"transport"`
	GC        GC        `json:"gc"`
	Tracing   Tracing   `json:"tracing"`
	Output    Output    `json:"output"`

	ExpectedEvidence ExpectedEvidence `json:"expected_evidence"`
	Conservation     Conservation     `json:"conservation"`
}

// Cohort fixes B and how many cohorts the run releases.
type Cohort struct {
	// Size is B, the number of logical requests released together. It is fixed by
	// the conformance gate, never by effect size (M1 Rev 4 decision 2).
	Size int `json:"size"`

	// Count is how many cohorts this run releases after warm-up.
	Count int `json:"count"`

	// WarmupCount is released before recording begins. Warm-up requests traverse
	// the same path but produce no evidence.
	WarmupCount int `json:"warmup_count"`
}

// Model names the artifact and the entry to serve it from.
type Model struct {
	Catalog     string              `json:"catalog"`
	ArtifactDir string              `json:"artifact_dir"`
	RuntimeDir  string              `json:"runtime_dir"`
	ArtifactID  string              `json:"artifact_id"`
	Entry       modelrepo.EntryKind `json:"entry"`

	// ExpectedDigest is checked against the catalog before anything is loaded, so
	// a regenerated artifact cannot silently replace the one a manifest names.
	ExpectedDigest string `json:"expected_digest"`
}

// Workload fixes the arrival process and its seed.
type Workload struct {
	Seed        int64  `json:"seed"`
	ReleaseMode string `json:"release_mode"`

	// InterCohortGapMillis separates consecutive cohorts. In the single-flight
	// identification regime it is what keeps cohorts from overlapping.
	InterCohortGapMillis int `json:"inter_cohort_gap_millis"`

	RequestTimeoutMillis int `json:"request_timeout_millis"`
}

// ReleaseMode values. Fixed-cohort release is the identification mode; the
// sequential diagnostic and the open-loop Stage C modes arrive in later slices.
const (
	ReleaseFixedCohort = "fixed-cohort"
	ReleaseSequential  = "sequential"
)

// Transport pins the gRPC limits. They are treatment definition, not tuning:
// packing cost is a declared constituent of the aggregation effect (ADR-0003).
type Transport struct {
	TritonEndpoint        string `json:"triton_endpoint"`
	ProxyEndpoint         string `json:"proxy_endpoint,omitempty"`
	AdapterEndpoint       string `json:"adapter_endpoint,omitempty"`
	MaxMessageBytes       int    `json:"max_message_bytes"`
	InitialWindowSize     int32  `json:"initial_window_size"`
	InitialConnWindowSize int32  `json:"initial_conn_window_size"`
}

// GC pins the Go collector's settings, which are recorded in the bundle
// alongside the per-run statistics they explain (ADR-0004).
type GC struct {
	GOGC         int   `json:"gogc"`
	GOMEMLIMIT   int64 `json:"gomemlimit_bytes"`
	SampleMillis int   `json:"sample_millis"`
}

// Tracing configures the backend timestamp source. Its overhead is
// treatment-correlated and is calibrated and frozen in a later slice; recording
// starts now (M2-PLAN §4.4).
type Tracing struct {
	Mode     string `json:"mode"`
	TraceDir string `json:"trace_dir"`
}

// Output is where the run bundle lands.
type Output struct {
	BundleDir string `json:"bundle_dir"`
}

// ExpectedEvidence is what the manifest asserts the run must produce. Declaring
// it up front is what turns "the run finished" into "the run did what its cell
// says it does".
type ExpectedEvidence struct {
	// Executions is n_k: how many model executions the run should produce.
	Executions int `json:"executions"`

	// ExecutionsPerCohort is J_k.
	ExecutionsPerCohort int `json:"executions_per_cohort"`

	// BatchSize is b_kj, the shape every execution should have.
	BatchSize int `json:"batch_size"`

	// MaxAdapterDispatchWaitNanos bounds the adapter's arrival-to-dispatch gap.
	// At A=off the adapter forwards on arrival, so this bounds unpack cost, not
	// waiting; the validator also checks the shape of the release, which is what
	// actually distinguishes forwarding from joining (ADR-0001).
	MaxAdapterDispatchWaitNanos int64 `json:"max_adapter_dispatch_wait_nanos,omitempty"`
}

// Conservation fixes the tolerance the residual is judged against.
type Conservation struct {
	ToleranceFraction float64 `json:"tolerance_fraction"`
}

// Load reads and validates a manifest.
func Load(path string) (*Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("manifest: read %s: %w", path, err)
	}
	var m Manifest
	dec := json.NewDecoder(bytes.NewReader(b))
	// Unknown fields are an error: a manifest key this build does not understand
	// means the run would silently ignore something the author meant to set.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("manifest: parse %s: %w", path, err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", path, err)
	}
	return &m, nil
}

// RequestTimeout is the per-request deadline.
func (m *Manifest) RequestTimeout() time.Duration {
	return time.Duration(m.Workload.RequestTimeoutMillis) * time.Millisecond
}

// InterCohortGap is the pause between cohort releases.
func (m *Manifest) InterCohortGap() time.Duration {
	return time.Duration(m.Workload.InterCohortGapMillis) * time.Millisecond
}

// TotalCohorts counts warm-up and recorded cohorts together.
func (m *Manifest) TotalCohorts() int { return m.Cohort.WarmupCount + m.Cohort.Count }

// ModelEntryName is the Triton model name this run serves from.
func (m *Manifest) ModelEntryName() string {
	return modelrepo.EntryName(m.Model.ArtifactID, m.Model.Entry)
}

// Validate rejects a manifest that could not produce interpretable evidence.
//
// The checks are deliberately not only well-formedness. Executor–cell
// compatibility and the expected evidence tuple are checked here because a
// manifest that declares a cell it cannot realize, or expectations that
// contradict its own cell, must fail before Triton is touched — not after a run
// has produced numbers nobody can interpret.
func (m *Manifest) Validate() error {
	if m.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema version %d, this build reads %d", m.SchemaVersion, SchemaVersion)
	}
	for _, f := range []struct {
		name, value string
	}{
		{"experiment_id", string(m.Experiment)},
		{"session_id", string(m.Session)},
		{"run_id", string(m.Run)},
		{"environment", m.Environment},
		{"model.catalog", m.Model.Catalog},
		{"model.artifact_id", m.Model.ArtifactID},
		{"model.runtime_dir", m.Model.RuntimeDir},
		{"transport.triton_endpoint", m.Transport.TritonEndpoint},
		{"output.bundle_dir", m.Output.BundleDir},
	} {
		if f.value == "" {
			return fmt.Errorf("%s is required", f.name)
		}
	}

	cell, err := identity.ParseCell(string(m.Cell))
	if err != nil {
		return err
	}
	if err := cell.CheckImplemented(); err != nil {
		return err
	}
	if cell.UsesProxy() && m.Transport.ProxyEndpoint == "" {
		return fmt.Errorf("cell %s traverses the shared path and needs transport.proxy_endpoint", cell)
	}
	if !cell.UsesProxy() && m.Transport.ProxyEndpoint != "" {
		return fmt.Errorf("cell %s is the direct path and must not name a proxy endpoint", cell)
	}

	if m.Cohort.Size <= 0 {
		return fmt.Errorf("cohort.size must be positive, got %d", m.Cohort.Size)
	}
	if m.Cohort.Size > identity.MaxOrdinal {
		return fmt.Errorf("cohort.size %d exceeds the %d an ordinal can encode", m.Cohort.Size, identity.MaxOrdinal)
	}
	if m.Cohort.Count <= 0 {
		return fmt.Errorf("cohort.count must be positive, got %d", m.Cohort.Count)
	}
	if m.Cohort.WarmupCount < 0 {
		return fmt.Errorf("cohort.warmup_count must not be negative, got %d", m.Cohort.WarmupCount)
	}

	switch m.Workload.ReleaseMode {
	case ReleaseFixedCohort:
	case ReleaseSequential:
		return fmt.Errorf("release mode %q is the F00-seq diagnostic, not implemented in this slice", m.Workload.ReleaseMode)
	default:
		return fmt.Errorf("unknown release mode %q", m.Workload.ReleaseMode)
	}
	if m.Workload.RequestTimeoutMillis <= 0 {
		return fmt.Errorf("workload.request_timeout_millis must be positive; a request without a deadline can vanish from the record")
	}

	switch m.Model.Entry {
	case modelrepo.EntryUnbatched:
		if cell.VectorizesCompute() {
			return fmt.Errorf("cell %s declares V=on but targets the unbatched entry", cell)
		}
	case modelrepo.EntryDynamic, modelrepo.EntryExplicit:
		if !cell.VectorizesCompute() {
			return fmt.Errorf("cell %s declares V=off but targets the %s entry", cell, m.Model.Entry)
		}
		return fmt.Errorf("entry %s belongs to cells outside this slice", m.Model.Entry)
	default:
		return fmt.Errorf("unknown model entry %q", m.Model.Entry)
	}

	if m.Transport.MaxMessageBytes <= 0 {
		return fmt.Errorf("transport.max_message_bytes must be set; it is a pinned constant, not a default (ADR-0003)")
	}
	if m.GC.GOGC <= 0 {
		return fmt.Errorf("gc.gogc must be pinned in the manifest (ADR-0004)")
	}
	if m.GC.GOMEMLIMIT <= 0 {
		return fmt.Errorf("gc.gomemlimit_bytes must be pinned in the manifest (ADR-0004)")
	}
	if m.Tracing.TraceDir == "" {
		return fmt.Errorf("tracing.trace_dir is required; the backend timestamps of the schema come from there")
	}

	if err := m.validateExpectedEvidence(cell); err != nil {
		return err
	}
	if m.Conservation.ToleranceFraction <= 0 || m.Conservation.ToleranceFraction >= 1 {
		return fmt.Errorf("conservation.tolerance_fraction must be in (0,1), got %v", m.Conservation.ToleranceFraction)
	}
	return nil
}

// validateExpectedEvidence checks the declared evidence tuple against the cell's
// contract, so a manifest cannot expect something its own cell cannot produce.
func (m *Manifest) validateExpectedEvidence(cell identity.Cell) error {
	e := m.ExpectedEvidence
	if e.Executions != m.Cohort.Count*e.ExecutionsPerCohort {
		return fmt.Errorf("expected_evidence.executions %d does not equal %d cohorts times %d executions each",
			e.Executions, m.Cohort.Count, e.ExecutionsPerCohort)
	}
	if cell.VectorizesCompute() {
		if e.ExecutionsPerCohort != 1 {
			return fmt.Errorf("cell %s is V=on: a cohort executes as one execution, not %d", cell, e.ExecutionsPerCohort)
		}
		if e.BatchSize != m.Cohort.Size {
			return fmt.Errorf("cell %s is V=on: the execution's batch size must be B=%d, not %d", cell, m.Cohort.Size, e.BatchSize)
		}
		return nil
	}
	if e.ExecutionsPerCohort != m.Cohort.Size {
		return fmt.Errorf("cell %s is V=off: a cohort produces B=%d executions, not %d", cell, m.Cohort.Size, e.ExecutionsPerCohort)
	}
	if e.BatchSize != 1 {
		return fmt.Errorf("cell %s is V=off: every execution has shape [1,…], not [%d,…]", cell, e.BatchSize)
	}
	if cell.UsesProxy() && e.MaxAdapterDispatchWaitNanos <= 0 {
		return fmt.Errorf("cell %s traverses the adapter and must declare expected_evidence.max_adapter_dispatch_wait_nanos", cell)
	}
	return nil
}
