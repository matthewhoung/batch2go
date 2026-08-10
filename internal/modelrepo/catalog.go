// Package modelrepo reads the artifact catalog, verifies artifacts against
// their declared digests, and materializes the runtime Triton model repository.
//
// Models are immutable artifacts. Triton runs in explicit model-control mode and
// loads only what a cell requires, so the model actually serving is provably the
// model the manifest named — verified by digest before load, never inferred from
// a file being in the right directory (ARCHITECTURE §9).
package modelrepo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// CatalogSchemaVersion is the catalog format this package reads.
const CatalogSchemaVersion = 1

// Catalog is the repository's record of which model artifacts exist and what
// they are. Binaries live outside Git; this is their provenance.
type Catalog struct {
	SchemaVersion int        `json:"schema_version"`
	Artifacts     []Artifact `json:"artifacts"`
}

// Artifact is one model graph: its identity, its digest, and its I/O schema.
type Artifact struct {
	ArtifactID string `json:"artifact_id"`
	Digest     string `json:"digest"`
	Bytes      int64  `json:"bytes"`
	Precision  string `json:"precision"`

	// MembershipEvidence records whether this graph attests its execution's full
	// uid set or merely echoes each request's own uid. The echo variant is a
	// permanent test fixture and must never reach a live run (ADR-0007).
	MembershipEvidence string `json:"membership_evidence"`

	Generator Generator `json:"generator"`
	Graph     Graph     `json:"graph"`
	IO        IO        `json:"io"`
}

// Generator identifies what produced an artifact, so a regeneration that changes
// the bytes is visible as a different digest rather than a silent substitution.
type Generator struct {
	Producer        string `json:"producer"`
	ProducerVersion string `json:"producer_version"`
	Opset           int    `json:"opset"`
	IRVersion       int    `json:"ir_version"`
}

// Graph holds the synthetic model's parameters. Kappa is a repeated-block count,
// dimensionless; realized milliseconds are per-environment measurements recorded
// elsewhere (ADR-0002).
type Graph struct {
	Kappa         int     `json:"kappa"`
	FeatureWidth  int     `json:"feature_width"`
	PayloadFloats int     `json:"payload_floats"`
	PayloadMiB    float64 `json:"payload_mib"`
}

// IO is the artifact's tensor schema.
type IO struct {
	Inputs  []Tensor `json:"inputs"`
	Outputs []Tensor `json:"outputs"`
}

// Tensor is one declared input or output. Dims entries are either integers or
// symbolic names, so they are carried as raw JSON values.
type Tensor struct {
	Name     string `json:"name"`
	DataType string `json:"data_type"`
	Dims     []any  `json:"dims"`
}

// SelfAttesting reports whether this artifact returns each execution's full uid
// set to every member.
func (a Artifact) SelfAttesting() bool { return a.MembershipEvidence == "self_attesting" }

// LoadCatalog reads and validates a catalog manifest.
func LoadCatalog(path string) (*Catalog, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("modelrepo: read catalog %s: %w", path, err)
	}
	var c Catalog
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("modelrepo: parse catalog %s: %w", path, err)
	}
	if c.SchemaVersion != CatalogSchemaVersion {
		return nil, fmt.Errorf("modelrepo: catalog %s has schema version %d, this build reads %d",
			path, c.SchemaVersion, CatalogSchemaVersion)
	}
	if len(c.Artifacts) == 0 {
		return nil, fmt.Errorf("modelrepo: catalog %s lists no artifacts", path)
	}
	for _, a := range c.Artifacts {
		if a.ArtifactID == "" {
			return nil, fmt.Errorf("modelrepo: catalog %s has an artifact without an id", path)
		}
		if !strings.HasPrefix(a.Digest, "sha256:") {
			return nil, fmt.Errorf("modelrepo: artifact %s has digest %q, want a sha256: digest",
				a.ArtifactID, a.Digest)
		}
	}
	return &c, nil
}

// Artifact looks up one artifact by id.
func (c *Catalog) Artifact(id string) (Artifact, error) {
	for _, a := range c.Artifacts {
		if a.ArtifactID == id {
			return a, nil
		}
	}
	ids := make([]string, 0, len(c.Artifacts))
	for _, a := range c.Artifacts {
		ids = append(ids, a.ArtifactID)
	}
	return Artifact{}, fmt.Errorf("modelrepo: no artifact %q in catalog (have: %s)", id, strings.Join(ids, ", "))
}

// FileDigest is the artifact digest form used throughout: sha256 with a prefix.
func FileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("modelrepo: open %s: %w", path, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("modelrepo: digest %s: %w", path, err)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// Verify checks an artifact file against its catalog digest. A mismatch is a
// hard failure: the alternative is a run whose results describe a model nobody
// declared.
func Verify(artifactPath string, a Artifact) error {
	got, err := FileDigest(artifactPath)
	if err != nil {
		return err
	}
	if got != a.Digest {
		return fmt.Errorf("modelrepo: artifact %s digest is %s, catalog declares %s",
			artifactPath, got, a.Digest)
	}
	info, err := os.Stat(artifactPath)
	if err != nil {
		return fmt.Errorf("modelrepo: stat %s: %w", artifactPath, err)
	}
	if a.Bytes != 0 && info.Size() != a.Bytes {
		return fmt.Errorf("modelrepo: artifact %s is %d bytes, catalog declares %d",
			artifactPath, info.Size(), a.Bytes)
	}
	return nil
}

// ArtifactPath is where an artifact's binary lives inside a local artifact
// directory.
func ArtifactPath(dir, artifactID string) string {
	return filepath.Join(dir, artifactID+".onnx")
}
