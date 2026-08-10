package modelrepo

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// EntryKind names one of the three Triton model configurations backed by a
// single graph artifact. They share one graph digest, one precision, and
// instance_group{count:1}; they differ only in scheduler configuration
// (M1 §2.1), which is what makes the V contrast a configuration difference
// rather than a different model.
type EntryKind string

const (
	// EntryUnbatched serves D0, F00 and F10: batching disabled, shape [1,…].
	// Scheduler coalescing of singles here is an asserted failure, not a warning.
	EntryUnbatched EntryKind = "unbatched"
	// EntryDynamic serves F01 and F11-D: Triton's dynamic batcher forms the batch.
	EntryDynamic EntryKind = "dynamic"
	// EntryExplicit serves F11-P: one pre-formed [B,…] request.
	EntryExplicit EntryKind = "explicit"
)

// EntryName is the Triton model name for an artifact's entry.
func EntryName(artifactID string, kind EntryKind) string {
	return artifactID + "_" + string(kind)
}

// Entry is one materialized Triton model entry, with the two digests recorded
// separately: an artifact can be right while its configuration is wrong, and the
// bundle has to be able to say which (ARCHITECTURE §9).
type Entry struct {
	Name           string    `json:"name"`
	Kind           EntryKind `json:"kind"`
	ArtifactID     string    `json:"artifact_id"`
	ArtifactDigest string    `json:"artifact_digest"`
	ConfigDigest   string    `json:"config_digest"`
	MaxBatchSize   int       `json:"max_batch_size"`
	InstanceKind   string    `json:"instance_kind"`
	InstanceCount  int       `json:"instance_count"`
}

// Repository is the outcome of materializing a runtime model repository.
type Repository struct {
	Root    string  `json:"root"`
	Entries []Entry `json:"entries"`
}

// Entry looks up a materialized entry by kind.
func (r *Repository) Entry(kind EntryKind) (Entry, error) {
	for _, e := range r.Entries {
		if e.Kind == kind {
			return e, nil
		}
	}
	return Entry{}, fmt.Errorf("modelrepo: repository has no %s entry", kind)
}

// Request describes a repository to materialize.
type Request struct {
	Catalog     *Catalog
	ArtifactID  string
	ArtifactDir string
	RuntimeDir  string
	Kinds       []EntryKind

	// MaxBatchSize applies to the dynamic and explicit entries. The unbatched
	// entry always declares 0, which is what makes its shapes exactly [1,…].
	MaxBatchSize int

	// InstanceKind is KIND_GPU or KIND_CPU. Instance count is always 1: more than
	// one instance would let executions overlap and dissolve the serialization
	// the cycle model books in Q_backend.
	InstanceKind string
}

// Materialize builds a runtime Triton model repository from digest-verified
// artifacts. It replaces the target directory wholesale, so a stale entry from a
// previous cell cannot survive into this run.
func Materialize(req Request) (*Repository, error) {
	if req.Catalog == nil {
		return nil, fmt.Errorf("modelrepo: materialize needs a catalog")
	}
	if len(req.Kinds) == 0 {
		return nil, fmt.Errorf("modelrepo: materialize needs at least one entry kind")
	}
	if req.InstanceKind == "" {
		req.InstanceKind = "KIND_GPU"
	}

	artifact, err := req.Catalog.Artifact(req.ArtifactID)
	if err != nil {
		return nil, err
	}
	artifactPath := ArtifactPath(req.ArtifactDir, req.ArtifactID)
	if err := Verify(artifactPath, artifact); err != nil {
		return nil, err
	}
	blob, err := os.ReadFile(artifactPath)
	if err != nil {
		return nil, fmt.Errorf("modelrepo: read artifact %s: %w", artifactPath, err)
	}

	if err := clearDir(req.RuntimeDir); err != nil {
		return nil, err
	}

	repo := &Repository{Root: req.RuntimeDir}
	for _, kind := range req.Kinds {
		entry, err := materializeEntry(req, artifact, kind, blob)
		if err != nil {
			return nil, err
		}
		repo.Entries = append(repo.Entries, entry)
	}
	return repo, nil
}

// clearDir empties a directory, creating it if it does not exist.
//
// It empties rather than replaces on purpose. The runtime repository is a bind
// mount into the running Triton container, and removing the directory itself
// would leave the container holding a mount to an inode that no longer exists —
// after which every model load fails with a repository poll error. The runner
// re-materializes per cell while the server keeps running, so this has to be
// safe to do underneath it.
func clearDir(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("modelrepo: create runtime repository %s: %w", path, err)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("modelrepo: read runtime repository %s: %w", path, err)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(path, e.Name())); err != nil {
			return fmt.Errorf("modelrepo: clear runtime repository %s: %w", path, err)
		}
	}
	return nil
}

func materializeEntry(req Request, artifact Artifact, kind EntryKind, blob []byte) (Entry, error) {
	name := EntryName(artifact.ArtifactID, kind)
	dir := filepath.Join(req.RuntimeDir, name)
	versionDir := filepath.Join(dir, "1")
	if err := os.MkdirAll(versionDir, 0o755); err != nil {
		return Entry{}, fmt.Errorf("modelrepo: create %s: %w", versionDir, err)
	}

	modelPath := filepath.Join(versionDir, "model.onnx")
	if err := os.WriteFile(modelPath, blob, 0o644); err != nil {
		return Entry{}, fmt.Errorf("modelrepo: write %s: %w", modelPath, err)
	}
	// The copy is re-verified rather than trusted: a truncated write would
	// otherwise produce a repository that loads a model no one declared.
	if err := Verify(modelPath, artifact); err != nil {
		return Entry{}, err
	}

	maxBatch := 0
	if kind != EntryUnbatched {
		maxBatch = req.MaxBatchSize
		if maxBatch <= 0 {
			return Entry{}, fmt.Errorf("modelrepo: %s entry needs a positive max batch size", kind)
		}
	}

	config, err := renderConfig(name, kind, artifact, maxBatch, req.InstanceKind)
	if err != nil {
		return Entry{}, err
	}
	configPath := filepath.Join(dir, "config.pbtxt")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		return Entry{}, fmt.Errorf("modelrepo: write %s: %w", configPath, err)
	}
	configDigest, err := FileDigest(configPath)
	if err != nil {
		return Entry{}, err
	}

	return Entry{
		Name:           name,
		Kind:           kind,
		ArtifactID:     artifact.ArtifactID,
		ArtifactDigest: artifact.Digest,
		ConfigDigest:   configDigest,
		MaxBatchSize:   maxBatch,
		InstanceKind:   req.InstanceKind,
		InstanceCount:  1,
	}, nil
}

// renderConfig writes the Triton model configuration for one entry.
func renderConfig(name string, kind EntryKind, artifact Artifact, maxBatch int, instanceKind string) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "# Generated by internal/modelrepo. Do not edit: the runtime repository is\n")
	fmt.Fprintf(&b, "# materialized fresh for every run and this file's digest is recorded.\n")
	fmt.Fprintf(&b, "name: %q\n", name)
	fmt.Fprintf(&b, "backend: \"onnxruntime\"\n")
	fmt.Fprintf(&b, "max_batch_size: %d\n", maxBatch)

	inputs, err := renderTensors("input", artifact.IO.Inputs, maxBatch)
	if err != nil {
		return "", err
	}
	outputs, err := renderTensors("output", artifact.IO.Outputs, maxBatch)
	if err != nil {
		return "", err
	}
	b.WriteString(inputs)
	b.WriteString(outputs)

	switch kind {
	case EntryUnbatched:
		// No dynamic_batching stanza at all. Its absence is the V=off policy;
		// every run separately proves the scheduler honoured it.
	case EntryDynamic:
		fmt.Fprintf(&b, "dynamic_batching {\n  preferred_batch_size: [ %d ]\n}\n", maxBatch)
	case EntryExplicit:
		// Batching is done by the caller, which submits one [B,…] request.
	default:
		return "", fmt.Errorf("modelrepo: unknown entry kind %q", kind)
	}

	fmt.Fprintf(&b, "instance_group [\n  {\n    count: 1\n    kind: %s\n  }\n]\n", instanceKind)
	return b.String(), nil
}

func renderTensors(section string, tensors []Tensor, maxBatch int) (string, error) {
	if len(tensors) == 0 {
		return "", fmt.Errorf("modelrepo: artifact declares no %ss", section)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s [\n", section)
	for i, t := range tensors {
		dataType, err := tritonDataType(t.DataType)
		if err != nil {
			return "", fmt.Errorf("modelrepo: %s %s: %w", section, t.Name, err)
		}
		dims, err := tritonDims(t.Dims, maxBatch)
		if err != nil {
			return "", fmt.Errorf("modelrepo: %s %s: %w", section, t.Name, err)
		}
		fmt.Fprintf(&b, "  {\n    name: %q\n    data_type: %s\n    dims: [ %s ]\n  }", t.Name, dataType, dims)
		if i < len(tensors)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	b.WriteString("]\n")
	return b.String(), nil
}

func tritonDataType(s string) (string, error) {
	switch strings.ToUpper(s) {
	case "FP32":
		return "TYPE_FP32", nil
	case "FP16":
		return "TYPE_FP16", nil
	case "INT64":
		return "TYPE_INT64", nil
	case "INT32":
		return "TYPE_INT32", nil
	default:
		return "", fmt.Errorf("unsupported data type %q", s)
	}
}

// tritonDims translates catalog dims into Triton config dims.
//
// The two entry families declare shape differently. Above max_batch_size 0
// Triton owns the leading dimension, so the config omits it. At 0 the config
// states the full shape and Triton compares it against the graph exactly — a
// symbolic dimension in the artifact must stay symbolic here, because the three
// entries are backed by one graph digest and that graph declares a variable
// batch dimension.
//
// So the unbatched entry's V=off guarantee is not that its declared shape
// forbids [B,…]. It is that max_batch_size 0 disables Triton's batching
// outright, that the shared submission engine sends exactly one member per
// request, and that every run proves the outcome from evidence: execution count
// equal to request count, a batch-size histogram of ones, and an attested
// membership of size one. That per-run proof is what the design asks for;
// max_batch_size alone is never accepted as evidence of V=off (M1 §2.1).
func tritonDims(dims []any, maxBatch int) (string, error) {
	if len(dims) == 0 {
		return "", fmt.Errorf("tensor declares no dims")
	}
	resolved := make([]string, 0, len(dims))
	for _, d := range dims {
		switch v := d.(type) {
		case float64:
			resolved = append(resolved, fmt.Sprintf("%d", int64(v)))
		case int:
			resolved = append(resolved, fmt.Sprintf("%d", v))
		case string:
			resolved = append(resolved, "-1")
		default:
			return "", fmt.Errorf("dim %d has unsupported type %T", len(resolved), d)
		}
	}
	if maxBatch > 0 {
		resolved = resolved[1:]
		if len(resolved) == 0 {
			return "", fmt.Errorf("tensor has no dims left after the batch dimension")
		}
	}
	return strings.Join(resolved, ", "), nil
}
