package runner

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// ImplementationSchemaVersion versions the property set below. It is part of the
// digest, so a run comparing two bundles cannot mistake a digest computed over a
// different set of properties for a match.
const ImplementationSchemaVersion = 1

// Implementation is the closed set of properties two cells must share for their
// contrast to be a contrast of one factor.
//
// The whole aggregation claim rests on F00 and F10 differing in transport
// aggregation and in nothing else. Left as an intention, that is an article of
// faith: the two runs use different manifests, start different processes, and
// nothing checks that what those processes built was the same. A comparison of
// two client implementations wearing cell labels would produce a clean effect
// and a wrong one.
//
// What is deliberately NOT here is as load-bearing as what is. The cell, the run
// id, the seed, the formation deadline and the skew bound differ between the two
// by design — they are the treatment and its parameters — and admitting any of
// them would make the digests differ for the reason the comparison exists to
// permit.
type Implementation struct {
	SchemaVersion int `json:"schema_version"`

	// What the adapter process actually wired. From its own record, not from the
	// manifest the runner handed it.
	ExecutorKind  string `json:"executor_kind"`
	ModelEntry    string `json:"model_entry"`
	FeatureWidth  int    `json:"feature_width"`
	PayloadFloats int    `json:"payload_floats"`

	// The channel the adapter opened to the backend, and the limits it served
	// envelopes under. Serialization cost is a declared constituent of the
	// measured effect (ADR-0003), so a difference here is a difference in the
	// thing being measured rather than in how it was measured.
	DownstreamMaxMessageBytes       int   `json:"downstream_max_message_bytes"`
	DownstreamInitialWindowSize     int32 `json:"downstream_initial_window_size"`
	DownstreamInitialConnWindowSize int32 `json:"downstream_initial_conn_window_size"`
	ServingMaxMessageBytes          int   `json:"serving_max_message_bytes"`
	ServingInitialWindowSize        int32 `json:"serving_initial_window_size"`
	ServingInitialConnWindowSize    int32 `json:"serving_initial_conn_window_size"`

	// The artifact and the served entry. Two cells reading different weights, or
	// a differently generated graph, are not running one model.
	ArtifactID     string `json:"artifact_id"`
	ArtifactDigest string `json:"artifact_digest"`
	ConfigDigest   string `json:"config_digest"`
	EntryKind      string `json:"entry_kind"`

	// What Triton reports it is serving, which is the only account of the model
	// that comes from the server rather than from the files fed to it.
	MaxBatchSize           int    `json:"max_batch_size"`
	DynamicBatchingEnabled bool   `json:"dynamic_batching_enabled"`
	InstanceKind           string `json:"instance_kind"`
	InstanceCount          int    `json:"instance_count"`

	// The envelope protocol that carried the payloads. Two cells speaking
	// different versions of it are not two levels of one factor.
	EnvelopeSchemaVersion int `json:"envelope_schema_version"`
}

// Digest is a stable fingerprint of the property set.
//
// Canonical JSON over a versioned struct, in the house style of the clock
// domain's identifier: the field names and their order are part of what is
// hashed, so adding, renaming or reordering a property changes the digest. That
// is the mechanism behind "a new property cannot be forgotten silently" — not
// the comparison, which only ever reports on the properties it was given, but
// the golden-digest test, which breaks the moment the set changes and forces
// whoever changed it to say so.
func (i Implementation) Digest() (string, error) {
	b, err := json.Marshal(i)
	if err != nil {
		return "", fmt.Errorf("runner: encode implementation: %w", err)
	}
	sum := sha256.Sum256(b)
	return fmt.Sprintf("sha256:%x", sum), nil
}

// ImplementationOf reads a bundle's account of what its run actually built.
func ImplementationOf(b *Bundle) (Implementation, error) {
	if b == nil {
		return Implementation{}, fmt.Errorf("runner: no bundle")
	}
	if b.Adapter == nil {
		return Implementation{}, fmt.Errorf(
			"runner: bundle for %s carries no adapter record, so what its adapter built is unknown; only the shared path can be compared this way",
			b.Cell)
	}
	return Implementation{
		SchemaVersion: ImplementationSchemaVersion,

		ExecutorKind:  string(b.Adapter.Executor),
		ModelEntry:    b.Adapter.ModelEntry,
		FeatureWidth:  b.Adapter.FeatureWidth,
		PayloadFloats: b.Adapter.PayloadFloats,

		DownstreamMaxMessageBytes:       b.Adapter.Downstream.MaxMessageBytes,
		DownstreamInitialWindowSize:     b.Adapter.Downstream.InitialWindowSize,
		DownstreamInitialConnWindowSize: b.Adapter.Downstream.InitialConnWindowSize,
		ServingMaxMessageBytes:          b.Adapter.Serving.MaxMessageBytes,
		ServingInitialWindowSize:        b.Adapter.Serving.InitialWindowSize,
		ServingInitialConnWindowSize:    b.Adapter.Serving.InitialConnWindowSize,

		ArtifactID:     b.ModelEntry.ArtifactID,
		ArtifactDigest: b.ModelEntry.ArtifactDigest,
		ConfigDigest:   b.ModelEntry.ConfigDigest,
		EntryKind:      string(b.ModelEntry.Kind),

		MaxBatchSize:           b.ModelIdentity.MaxBatchSize,
		DynamicBatchingEnabled: b.ModelIdentity.DynamicBatchingEnabled,
		InstanceKind:           b.ModelIdentity.InstanceKind,
		InstanceCount:          b.ModelIdentity.InstanceCount,

		EnvelopeSchemaVersion: b.EnvelopeSchemaVersion,
	}, nil
}

// Comparison is the finding: whether two cells resolved to one implementation,
// and where they did not.
type Comparison struct {
	CellA identity.Cell `json:"cell_a"`
	CellB identity.Cell `json:"cell_b"`

	Same   bool   `json:"same"`
	Digest string `json:"digest,omitempty"`

	// Differences names each property that disagreed and both values, because
	// "they differ" is not a usable finding.
	Differences []PropertyDifference `json:"differences,omitempty"`
}

// PropertyDifference is one property two bundles disagreed on.
type PropertyDifference struct {
	Property string `json:"property"`
	A        string `json:"a"`
	B        string `json:"b"`
}

// SameImplementation asserts that two archived runs differ in their factor
// levels and in nothing else that could produce an effect.
//
// It refuses rather than passes quietly in three cases, each of which is a way
// the assertion could be satisfied by saying nothing. A bundle whose run did not
// complete describes an implementation that never served a request. Comparing a
// bundle with itself is trivially true and establishes nothing. And two cells
// that are the same cell are not a contrast at all.
func SameImplementation(a, b *Bundle) (Comparison, error) {
	for _, bundle := range []*Bundle{a, b} {
		if bundle == nil {
			return Comparison{}, fmt.Errorf("runner: same-implementation needs two bundles")
		}
		if bundle.State != StateCompleted {
			return Comparison{}, fmt.Errorf(
				"runner: the bundle for %s is %q, so what it built was never exercised; comparing it would assert something about a run that did not happen",
				bundle.Cell, bundle.State)
		}
	}
	if a.Cell == b.Cell {
		return Comparison{}, fmt.Errorf(
			"runner: both bundles are %s; comparing a cell with itself passes by saying nothing", a.Cell)
	}

	implA, err := ImplementationOf(a)
	if err != nil {
		return Comparison{}, err
	}
	implB, err := ImplementationOf(b)
	if err != nil {
		return Comparison{}, err
	}

	cmp := Comparison{CellA: a.Cell, CellB: b.Cell}
	cmp.Differences = diffImplementations(implA, implB)
	cmp.Same = len(cmp.Differences) == 0
	if cmp.Same {
		if cmp.Digest, err = implA.Digest(); err != nil {
			return cmp, err
		}
	}
	return cmp, nil
}

// diffImplementations reports every property that disagreed, by walking the
// canonical encoding rather than a hand-written list of comparisons.
//
// Walking the encoding is what keeps the two in step: a property added to the
// struct is compared without anyone remembering to add a line here, which is the
// half of "cannot be forgotten silently" that the digest alone does not cover.
func diffImplementations(a, b Implementation) []PropertyDifference {
	fieldsA, errA := implementationFields(a)
	fieldsB, errB := implementationFields(b)
	if errA != nil || errB != nil {
		return []PropertyDifference{{Property: "(encoding)", A: fmt.Sprint(errA), B: fmt.Sprint(errB)}}
	}

	var out []PropertyDifference
	for _, name := range implementationFieldOrder(a) {
		va, vb := fieldsA[name], fieldsB[name]
		if string(va) != string(vb) {
			out = append(out, PropertyDifference{Property: name, A: string(va), B: string(vb)})
		}
	}
	return out
}

func implementationFields(i Implementation) (map[string]json.RawMessage, error) {
	b, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// implementationFieldOrder is the struct's own field order, so that a diff reads
// in the order the properties are declared rather than alphabetically.
func implementationFieldOrder(i Implementation) []string {
	b, err := json.Marshal(i)
	if err != nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	// Consume the opening brace, then every key in encoding order.
	if _, err := dec.Token(); err != nil {
		return nil
	}
	var order []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return order
		}
		key, ok := tok.(string)
		if !ok {
			return order
		}
		order = append(order, key)
		var discard json.RawMessage
		if err := dec.Decode(&discard); err != nil {
			return order
		}
	}
	return order
}
