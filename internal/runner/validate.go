package runner

import (
	"fmt"
	"path/filepath"

	"github.com/matthewhoung/batch2go/internal/events"
	eventsparquet "github.com/matthewhoung/batch2go/internal/events/parquet"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/validate"
)

// VerdictFile is where a bundle's verdict is written, inside the bundle.
const VerdictFile = "verdict.json"

// Expectation translates a bundle into what the validator should judge it
// against.
//
// The translation lives here rather than in the validator because the validator
// deliberately knows nothing about manifests, bundles, or the gateway — that is
// what lets a verdict be reproduced from an archive with no live state, and what
// keeps the judging code from sharing implementation with the emitting code.
func Expectation(b *Bundle) (validate.Expectation, error) {
	if b.Manifest == nil {
		return validate.Expectation{}, fmt.Errorf("runner: bundle %s carries no manifest; it cannot be judged", b.Run)
	}
	m := b.Manifest

	var dropped uint64
	for _, s := range b.Streams {
		dropped += s.Dropped
	}

	var clockDomain identity.ClockDomainID
	if b.ClockDomain != nil {
		clockDomain = b.ClockDomain.ID
	}

	return validate.Expectation{
		Run:                         b.Run,
		Cell:                        b.Cell,
		ClockDomain:                 clockDomain,
		CohortSize:                  m.Cohort.Size,
		CohortCount:                 m.Cohort.Count,
		FirstCohortID:               identity.CohortID(m.Cohort.WarmupCount),
		ExecutionsPerCohort:         m.ExpectedEvidence.ExecutionsPerCohort,
		BatchSize:                   m.ExpectedEvidence.BatchSize,
		Executions:                  m.ExpectedEvidence.Executions,
		ToleranceFraction:           m.Conservation.ToleranceFraction,
		MaxAdapterDispatchWaitNanos: m.ExpectedEvidence.MaxAdapterDispatchWaitNanos,
		MaxDispatchSkewNanos:        m.ExpectedEvidence.MaxDispatchSkewNanos,
		ExecutionCountDelta:         b.TritonStats.ExecutionCount,
		InferenceCountDelta:         b.TritonStats.InferenceCount,
		BatchSizeHistogram:          b.TritonStats.BatchSizes,
		DroppedRecords:              dropped,
	}, nil
}

// ValidateBundle judges an archived run.
//
// It reads the Parquet archive rather than the raw binary streams, because the
// archive is what leaves the machine and what analysis reads — so this is the
// artifact whose verdict has to be reproducible. The raw streams are kept beside
// it until the bundle validates (ADR-0005), which is what makes a disagreement
// between the two investigable rather than merely suspected.
func ValidateBundle(bundleDir string) (*Bundle, validate.Verdict, error) {
	bundle, err := LoadBundle(filepath.Join(bundleDir, "bundle.json"))
	if err != nil {
		return nil, validate.Verdict{}, err
	}
	exp, err := Expectation(bundle)
	if err != nil {
		return bundle, validate.Verdict{}, err
	}

	archive := bundle.Files.Archive
	if archive == "" {
		archive = "events.parquet"
	}
	records, err := eventsparquet.Read(filepath.Join(bundleDir, archive))
	if err != nil {
		return bundle, validate.Verdict{}, err
	}
	if err := checkSchemaVersions(records); err != nil {
		return bundle, validate.Verdict{}, err
	}

	return bundle, validate.Validate(exp, records), nil
}

// ValidateStreams judges the raw binary streams instead of the archive. The two
// must agree; a disagreement means the archive conversion lost or changed
// something, which is worth knowing before a bundle is trusted.
func ValidateStreams(bundleDir string, bundle *Bundle) (validate.Verdict, error) {
	exp, err := Expectation(bundle)
	if err != nil {
		return validate.Verdict{}, err
	}
	var records []events.Decoded
	for _, stream := range bundle.Streams {
		decoded, err := events.ReadFile(filepath.Join(bundleDir, stream.File))
		if err != nil {
			return validate.Verdict{}, err
		}
		records = append(records, decoded...)
	}
	// The same warm-up filter the archive applies: judging the streams against a
	// different record set than the archive would make the two verdicts
	// incomparable, which is the whole point of computing both.
	return validate.Validate(exp, RecordedOnly(records, bundle.FirstRecordedCohort)), nil
}

func checkSchemaVersions(records []events.Decoded) error {
	for _, d := range records {
		if d.SchemaVersion != events.SchemaVersion {
			return fmt.Errorf("runner: archive carries event schema version %d, this build reads %d",
				d.SchemaVersion, events.SchemaVersion)
		}
	}
	return nil
}

// WriteVerdict stores a verdict inside its bundle.
func WriteVerdict(bundleDir string, v validate.Verdict) error {
	return writeJSON(filepath.Join(bundleDir, VerdictFile), v)
}
