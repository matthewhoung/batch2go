// Package runner is the control plane: it turns a manifest into an executed,
// self-describing run bundle.
package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/matthewhoung/batch2go/internal/adapter"
	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/events/clockdomain"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/manifest"
	"github.com/matthewhoung/batch2go/internal/modelrepo"
	"github.com/matthewhoung/batch2go/internal/triton"
	"github.com/matthewhoung/batch2go/internal/workload"
)

// BundleSchemaVersion is the run-bundle format version.
//
// It became 2 when the bundle started naming the envelope protocol that carried
// its payloads, and 3 when the adapter began recording its own configuration. A
// bundle written before either decodes the new field as its zero value, which is
// indistinguishable from a real answer — so the version moves and LoadBundle
// refuses the older shape rather than reading a contract number, or an adapter's
// transport limits, out of a field nobody wrote.
const BundleSchemaVersion = 3

// Terminal run states.
const (
	StateCompleted = "completed"
	StateFailed    = "failed"
)

// Bundle is a run's complete, self-describing record.
//
// Self-describing is the requirement, not a nicety: offline analysis and the
// validator must be able to reach a verdict from the archive alone, with no
// network, no live server, and no out-of-band context about how the run was set
// up. Everything needed to interpret the event records is therefore here — the
// manifest, the schema version, the clock domain, the served model's identity,
// the transport limits, the collector settings, and the backend's own counters.
type Bundle struct {
	SchemaVersion int `json:"schema_version"`

	Experiment identity.ExperimentID `json:"experiment_id"`
	Session    identity.SessionID    `json:"session_id"`
	Run        identity.RunID        `json:"run_id"`
	Cell       identity.Cell         `json:"cell"`
	State      string                `json:"state"`

	// Failure names why a run ended in StateFailed. A failed run keeps its
	// evidence: nothing is excluded silently.
	Failure string `json:"failure,omitempty"`

	Manifest *manifest.Manifest `json:"manifest"`

	// ClockDomain is what makes the timestamps subtractable. Without it the
	// records are numbers of unknown provenance.
	ClockDomain *clockdomain.Domain `json:"clock_domain"`

	// EventSchemaVersion is the record vocabulary the streams were written with,
	// and EnvelopeSchemaVersion the transport contract that carried the payloads.
	// Both belong in the archive for the same reason: a run's records are only
	// interpretable against the contracts that produced them, and those contracts
	// change between slices while archived runs do not.
	EventSchemaVersion    int `json:"event_schema_version"`
	EnvelopeSchemaVersion int `json:"envelope_schema_version"`

	// Adapter is the adapter process's own account of how it was configured. It
	// is a pointer because D0 has no adapter at all: an absent one is typed
	// absence, exactly as a stage outside a cell's topology is, and a zero-valued
	// record would let a direct-path bundle claim a transport nobody set.
	Adapter *adapter.ProcessRecord `json:"adapter,omitempty"`

	Server        ServerRecord         `json:"server"`
	ModelEntry    modelrepo.Entry      `json:"model_entry"`
	ModelGraph    modelrepo.Graph      `json:"model_graph"`
	ModelIdentity triton.ModelIdentity `json:"model_identity"`
	Transport     triton.Config        `json:"transport"`
	GC            GCStats              `json:"gc"`

	// TritonStats is the backend's own accounting across the recorded window: the
	// raw material for the contamination check, not a conclusion about it.
	TritonStats triton.StatisticsDelta `json:"triton_statistics_delta"`

	Streams  []StreamRecord    `json:"streams"`
	Schedule []workload.Cohort `json:"realized_schedule"`

	// FirstRecordedCohort is the first cohort id that produced evidence. Warm-up
	// cohorts traverse the same path and are recorded in the raw streams, but they
	// do not enter the archive and are not judged.
	FirstRecordedCohort identity.CohortID `json:"first_recorded_cohort"`

	StartedAtMonotonic  int64  `json:"started_at_monotonic"`
	FinishedAtMonotonic int64  `json:"finished_at_monotonic"`
	StartedAtWall       string `json:"started_at_wall"`

	Files BundleFiles `json:"files"`
}

// ServerRecord identifies the inference server that served the run.
type ServerRecord struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Endpoint    string `json:"endpoint"`
	ImageDigest string `json:"image_digest,omitempty"`
}

// StreamRecord accounts for one process's event stream. Dropped records are
// evidence about the instrument and are reported rather than absorbed.
type StreamRecord struct {
	Emitter string `json:"emitter"`
	File    string `json:"file"`
	Written uint64 `json:"written"`
	Dropped uint64 `json:"dropped"`
}

// BundleFiles lists the bundle's contents relative to its directory.
type BundleFiles struct {
	Manifest     string   `json:"manifest"`
	EventStreams []string `json:"event_streams"`
	Archive      string   `json:"archive"`
	Traces       []string `json:"traces"`
}

// Dir is where a run's bundle lives.
func Dir(m *manifest.Manifest) string {
	return filepath.Join(m.Output.BundleDir, string(m.Run))
}

// Layout holds the paths inside one bundle directory.
type Layout struct {
	Root     string
	Manifest string
	Bundle   string
	Events   string
	Archive  string
	Traces   string
}

// NewLayout prepares a bundle directory.
func NewLayout(root string) (Layout, error) {
	l := Layout{
		Root:     root,
		Manifest: filepath.Join(root, "manifest.json"),
		Bundle:   filepath.Join(root, "bundle.json"),
		Events:   filepath.Join(root, "events"),
		Archive:  filepath.Join(root, "events.parquet"),
		Traces:   filepath.Join(root, "traces"),
	}
	for _, dir := range []string{l.Root, l.Events, l.Traces} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return Layout{}, fmt.Errorf("runner: create bundle directory %s: %w", dir, err)
		}
	}
	return l, nil
}

// StreamPath is where one emitter's binary record stream is written.
func (l Layout) StreamPath(emitter identity.Emitter) string {
	return filepath.Join(l.Events, emitter.String()+".b2g")
}

// WriteBundle writes the bundle description and a copy of the manifest.
func WriteBundle(l Layout, b *Bundle) error {
	b.SchemaVersion = BundleSchemaVersion
	b.EventSchemaVersion = events.SchemaVersion
	b.EnvelopeSchemaVersion = envelope.SchemaVersion

	if err := writeJSON(l.Manifest, b.Manifest); err != nil {
		return err
	}
	return writeJSON(l.Bundle, b)
}

// BundleFileName is what a bundle description is called inside its directory.
// It is named once so that no caller has to know it, which is how a directory
// came to be handed to a function that opens a file.
const BundleFileName = "bundle.json"

// LoadBundleDir reads the bundle description out of a run's directory.
//
// It exists because every operator-facing path in this repository names a run by
// its directory — `make validate BUNDLE=results/bundles/<run>`, the suite's own
// Dir(), the archive layout — while LoadBundle opens a file. Passing one to the
// other opens the directory successfully and fails on the first read, so the
// mistake surfaces as a JSON error about something that is not JSON, at the
// moment the command is used rather than when it is built. Callers holding a
// directory use this; LoadBundle stays for callers that genuinely hold a path.
func LoadBundleDir(dir string) (*Bundle, error) {
	return LoadBundle(filepath.Join(dir, BundleFileName))
}

// LoadBundle reads a bundle description back from the path of the file itself.
// It is how the validator and the analysis toolchain open an archived run.
func LoadBundle(path string) (*Bundle, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("runner: open bundle %s: %w", path, err)
	}
	defer f.Close()

	var b Bundle
	if err := json.NewDecoder(f).Decode(&b); err != nil {
		return nil, fmt.Errorf("runner: parse bundle %s: %w", path, err)
	}
	if b.SchemaVersion != BundleSchemaVersion {
		return nil, fmt.Errorf("runner: bundle %s has schema version %d, this build reads %d",
			path, b.SchemaVersion, BundleSchemaVersion)
	}
	return &b, nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("runner: encode %s: %w", path, err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("runner: write %s: %w", path, err)
	}
	return nil
}

// copyFile preserves a raw trace file with the bundle.
func copyFile(dst, src string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("runner: open %s: %w", src, err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("runner: create %s: %w", dst, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("runner: copy %s to %s: %w", src, dst, err)
	}
	return nil
}

func wallClockStamp() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// RecordedOnly drops warm-up records, which traverse the same path but produce
// no evidence.
func RecordedOnly(records []events.Decoded, firstRecorded identity.CohortID) []events.Decoded {
	out := make([]events.Decoded, 0, len(records))
	for _, d := range records {
		if d.Record.Cohort < firstRecorded {
			continue
		}
		out = append(out, d)
	}
	return out
}

// externalStreamRecord accounts for a stream written by another process.
//
// The counters come from the sidecar the service wrote, not from counting rows:
// a dropped record leaves no row to count, so recovering the count from the file
// alone would report "dropped: 0" on no evidence. The stream is still read back,
// because one that will not parse is a failure now rather than a surprise during
// analysis, and the two counts must agree.
func externalStreamRecord(l Layout, emitter identity.Emitter) (StreamRecord, error) {
	path := l.StreamPath(emitter)
	records, err := events.ReadFile(path)
	if err != nil {
		return StreamRecord{}, err
	}
	stats, err := events.ReadStats(path)
	if err != nil {
		return StreamRecord{}, err
	}
	if stats.Written != uint64(len(records)) {
		return StreamRecord{}, fmt.Errorf(
			"runner: %s reported %d records written but its stream holds %d; evidence was lost between the two",
			emitter, stats.Written, len(records))
	}
	return StreamRecord{
		Emitter: emitter.String(),
		File:    filepath.Join("events", emitter.String()+".b2g"),
		Written: stats.Written,
		Dropped: stats.Dropped,
	}, nil
}
