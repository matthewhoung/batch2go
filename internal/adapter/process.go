package adapter

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/matthewhoung/batch2go/internal/executor"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/triton"
)

// ProcessRecordSchemaVersion is the version of the record below. A record from
// another revision is refused rather than partially understood, for the same
// reason the bundle refuses an older shape: a field this build does not know
// about decodes as its zero value, and a zero is indistinguishable from an
// answer somebody gave.
const ProcessRecordSchemaVersion = 1

// ProcessRecord is the adapter's own account of how it was configured.
//
// It exists because the claim the whole aggregation contrast rests on — that
// F00 and F10 differ in transport aggregation and in nothing else — has to be
// checkable against archives rather than believed. Until now nothing about this
// process reached the bundle, so the comparison would have been the runner's
// intent compared with itself: the same manifest, read twice, agreeing.
//
// Every field below is therefore taken from the live object that will actually
// serve the run, not from the flag that was used to build it. The distinction is
// the entire point. A record populated from the flags would restate what the
// runner already wrote into the bundle, and two cells would agree on their
// configuration no matter what the processes did with it.
type ProcessRecord struct {
	SchemaVersion int `json:"schema_version"`

	Experiment  identity.ExperimentID  `json:"experiment_id"`
	Session     identity.SessionID     `json:"session_id"`
	Run         identity.RunID         `json:"run_id"`
	Cell        identity.Cell          `json:"cell"`
	ClockDomain identity.ClockDomainID `json:"clock_domain_id"`

	// Executor is the kind this process actually wired, reported by the code that
	// wired it. Re-deriving it from the cell here would report what the cell
	// implies rather than what the process built — the same substitution the flag
	// question above turns on.
	Executor executor.Kind `json:"executor_kind"`

	// ModelEntry is the Triton model name this adapter submits against.
	ModelEntry string `json:"model_entry"`

	// Downstream is the channel this process opened to Triton, read back from the
	// gateway rather than from the flags that configured it.
	Downstream triton.Config `json:"downstream_transport"`

	// Serving is the gRPC limit set this process accepts envelopes under.
	Serving ServingConfig `json:"serving_transport"`

	// FeatureWidth and PayloadFloats are the tensor shape it builds. They are
	// properties of the model contract, and two cells serving one model must
	// agree on them.
	FeatureWidth  int `json:"feature_width"`
	PayloadFloats int `json:"payload_floats"`
}

// ServingConfig is the adapter's client-facing gRPC limits. It mirrors the
// downstream limits it holds for Triton, and the two are recorded separately
// because they are set separately and only one of them is the transport under
// study.
type ServingConfig struct {
	MaxMessageBytes       int   `json:"max_message_bytes"`
	InitialWindowSize     int32 `json:"initial_window_size"`
	InitialConnWindowSize int32 `json:"initial_conn_window_size"`
}

// Validate refuses a record that could not describe a process that ran.
func (r ProcessRecord) Validate() error {
	switch {
	case r.SchemaVersion != ProcessRecordSchemaVersion:
		return fmt.Errorf("adapter: process record schema version %d, this build reads %d",
			r.SchemaVersion, ProcessRecordSchemaVersion)
	case r.Run == "":
		return fmt.Errorf("adapter: process record needs a run id")
	case r.Cell == "":
		return fmt.Errorf("adapter: process record needs a cell")
	case r.Executor == "":
		return fmt.Errorf("adapter: process record needs the executor kind the process wired")
	case r.ModelEntry == "":
		return fmt.Errorf("adapter: process record needs the model entry it serves")
	}
	return nil
}

// ProcessRecordPath is the sidecar the adapter leaves beside its event stream.
//
// It rides alongside the stream for the same reason the stream's counters do:
// this process's knowledge of itself lives in its own memory, and the runner
// cannot recover it after the process exits. The two share the assumption that
// the adapter and the runner see one filesystem, which holds while the runner
// spawns it as a child and would have to become an RPC under the Env 2 split
// these binaries are kept independently deployable for.
func ProcessRecordPath(streamPath string) string { return streamPath + ".process.json" }

// WriteProcessRecord persists the adapter's account of itself.
func WriteProcessRecord(streamPath string, r ProcessRecord) error {
	if err := r.Validate(); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("adapter: encode process record: %w", err)
	}
	if err := os.WriteFile(ProcessRecordPath(streamPath), append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("adapter: write process record: %w", err)
	}
	return nil
}

// ReadProcessRecord recovers it. A missing file is an error rather than an empty
// record: "the adapter was configured this way" and "nobody said" must not look
// alike, which is the same rule the stream counters follow.
func ReadProcessRecord(streamPath string) (ProcessRecord, error) {
	var r ProcessRecord
	b, err := os.ReadFile(ProcessRecordPath(streamPath))
	if err != nil {
		return r, fmt.Errorf("adapter: read process record for %s: %w", streamPath, err)
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("adapter: parse process record for %s: %w", streamPath, err)
	}
	if err := r.Validate(); err != nil {
		return r, err
	}
	return r, nil
}
