package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/events"
)

// A bundle has to be interpretable from the archive alone, and the records are
// only interpretable against the contracts that produced them. Both versions are
// therefore written by the archiver rather than supplied by a caller, so a run
// cannot claim a contract it did not speak.
func TestBundleRecordsTheContractsThatProducedIt(t *testing.T) {
	layout, err := NewLayout(t.TempDir())
	if err != nil {
		t.Fatalf("new layout: %v", err)
	}

	written := &Bundle{
		Run:                   "run-1",
		Cell:                  "F00",
		State:                 StateCompleted,
		SchemaVersion:         99,
		EventSchemaVersion:    99,
		EnvelopeSchemaVersion: 99,
	}
	if err := WriteBundle(layout, written); err != nil {
		t.Fatalf("write bundle: %v", err)
	}

	read, err := LoadBundle(filepath.Join(layout.Root, "bundle.json"))
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	if read.SchemaVersion != BundleSchemaVersion {
		t.Errorf("bundle schema version = %d, want %d", read.SchemaVersion, BundleSchemaVersion)
	}
	if read.EventSchemaVersion != events.SchemaVersion {
		t.Errorf("event schema version = %d, want %d", read.EventSchemaVersion, events.SchemaVersion)
	}
	if read.EnvelopeSchemaVersion != envelope.SchemaVersion {
		t.Errorf("envelope schema version = %d, want %d", read.EnvelopeSchemaVersion, envelope.SchemaVersion)
	}
}

// A bundle from before a contract was recorded is refused rather than read. Its
// missing field would decode as zero, which is not an answer this build could
// interpret the run against — and a self-describing archive that answers zero is
// worse than one that will not open.
//
// Both older shapes are named, because each one is refused for its own reason: a
// version-1 bundle predates the envelope protocol being recorded, and a
// version-2 bundle predates the adapter recording its own configuration — so a
// version-2 archive read by this build would report an adapter that was never
// configured, and a comparison against it would compare a run with a blank.
func TestLoadBundleRefusesAnOlderBundleShape(t *testing.T) {
	for name, body := range map[string]string{
		"before the envelope protocol was recorded": `{"schema_version": 1, "run_id": "run-1", "cell": "F00", "event_schema_version": 1}`,
		"before the adapter recorded its own configuration": `{"schema_version": 2, "run_id": "run-1", "cell": "F00",
			"event_schema_version": 2, "envelope_schema_version": 1}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bundle.json")
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				t.Fatalf("write bundle: %v", err)
			}

			_, err := LoadBundle(path)
			if err == nil {
				t.Fatal("a bundle written under an older format must be refused")
			}
			if !strings.Contains(err.Error(), "schema version") {
				t.Errorf("the error should name the format version, got: %v", err)
			}
		})
	}
}
