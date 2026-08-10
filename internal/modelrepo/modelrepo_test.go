package modelrepo

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testCatalog(t *testing.T) (*Catalog, string) {
	t.Helper()
	dir := t.TempDir()
	blob := []byte("not a real onnx graph, but a real digest")
	path := ArtifactPath(dir, "synthetic_k8_p65536")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	digest, err := FileDigest(path)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}

	return &Catalog{
		SchemaVersion: CatalogSchemaVersion,
		Artifacts: []Artifact{{
			ArtifactID:         "synthetic_k8_p65536",
			Digest:             digest,
			Bytes:              int64(len(blob)),
			Precision:          "fp32",
			MembershipEvidence: "self_attesting",
			Generator:          Generator{Producer: "batch2go-modelgen", Opset: 17, IRVersion: 9},
			Graph:              Graph{Kappa: 8, FeatureWidth: 256, PayloadFloats: 65536, PayloadMiB: 0.25},
			IO: IO{
				Inputs: []Tensor{
					{Name: "data", DataType: "FP32", Dims: []any{"N", 256}},
					{Name: "padding", DataType: "FP32", Dims: []any{"N", 65536}},
					{Name: "uid", DataType: "INT64", Dims: []any{"N", 1}},
				},
				Outputs: []Tensor{
					{Name: "data_out", DataType: "FP32", Dims: []any{"N", 256}},
					{Name: "uid_set", DataType: "INT64", Dims: []any{"N", "N"}},
				},
			},
		}},
	}, dir
}

// The unbatched entry must disable Triton's batching outright: max_batch_size 0
// and no dynamic_batching stanza at all. Its dims stay symbolic because all
// three entries share one graph digest and Triton compares the config against
// that graph exactly at max_batch_size 0; the [1,…] shape is then established
// per run from evidence, not from the declaration.
func TestUnbatchedEntryDisablesBatching(t *testing.T) {
	catalog, artifactDir := testCatalog(t)
	runtimeDir := filepath.Join(t.TempDir(), "runtime")

	repo, err := Materialize(Request{
		Catalog:     catalog,
		ArtifactID:  "synthetic_k8_p65536",
		ArtifactDir: artifactDir,
		RuntimeDir:  runtimeDir,
		Kinds:       []EntryKind{EntryUnbatched},
	})
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}

	entry, err := repo.Entry(EntryUnbatched)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}
	if entry.Name != "synthetic_k8_p65536_unbatched" {
		t.Errorf("entry name = %q", entry.Name)
	}
	if entry.MaxBatchSize != 0 {
		t.Errorf("max batch size = %d, want 0", entry.MaxBatchSize)
	}

	config, err := os.ReadFile(filepath.Join(runtimeDir, entry.Name, "config.pbtxt"))
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	got := string(config)

	if !strings.Contains(got, "max_batch_size: 0") {
		t.Error("unbatched entry must declare max_batch_size: 0")
	}
	if strings.Contains(got, "dynamic_batching") {
		t.Error("unbatched entry must have no dynamic_batching stanza: its absence is the V=off policy")
	}
	for _, want := range []string{
		`name: "data"`, "dims: [ -1, 256 ]",
		`name: "padding"`, "dims: [ -1, 65536 ]",
		`name: "uid"`, "dims: [ -1, 1 ]",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("config is missing %q\n%s", want, got)
		}
	}
	// The uid attestation's second dimension is variable: the execution reports
	// how many members it actually had, which is what detects coalescing.
	if !strings.Contains(got, "dims: [ -1, -1 ]") {
		t.Errorf("uid_set should declare a variable second dimension\n%s", got)
	}
	if !strings.Contains(got, "count: 1") {
		t.Error("every entry runs a single instance; overlapping executions would dissolve Q_backend")
	}

	if _, err := os.Stat(filepath.Join(runtimeDir, entry.Name, "1", "model.onnx")); err != nil {
		t.Errorf("model file not materialized: %v", err)
	}
}

// The dynamic entry hands the leading dimension to Triton, so the config omits it.
func TestDynamicEntryDelegatesTheBatchDimensionToTriton(t *testing.T) {
	catalog, artifactDir := testCatalog(t)
	runtimeDir := filepath.Join(t.TempDir(), "runtime")

	repo, err := Materialize(Request{
		Catalog:      catalog,
		ArtifactID:   "synthetic_k8_p65536",
		ArtifactDir:  artifactDir,
		RuntimeDir:   runtimeDir,
		Kinds:        []EntryKind{EntryDynamic},
		MaxBatchSize: 4,
	})
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	entry, _ := repo.Entry(EntryDynamic)
	config, err := os.ReadFile(filepath.Join(runtimeDir, entry.Name, "config.pbtxt"))
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	got := string(config)

	if !strings.Contains(got, "max_batch_size: 4") {
		t.Error("dynamic entry should carry the cohort size as its max batch size")
	}
	if !strings.Contains(got, "dims: [ 256 ]") {
		t.Errorf("dynamic entry omits the batch dimension from dims\n%s", got)
	}
	if !strings.Contains(got, "dynamic_batching") {
		t.Error("dynamic entry needs its dynamic_batching stanza")
	}
}

// All entries of one artifact share one graph digest, one precision, and one
// instance — that shared identity is what makes the V contrast a scheduler
// difference rather than a model difference (M1 §2.1).
func TestAllEntriesShareOneArtifactDigest(t *testing.T) {
	catalog, artifactDir := testCatalog(t)
	repo, err := Materialize(Request{
		Catalog:      catalog,
		ArtifactID:   "synthetic_k8_p65536",
		ArtifactDir:  artifactDir,
		RuntimeDir:   filepath.Join(t.TempDir(), "runtime"),
		Kinds:        []EntryKind{EntryUnbatched, EntryDynamic, EntryExplicit},
		MaxBatchSize: 4,
	})
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if len(repo.Entries) != 3 {
		t.Fatalf("materialized %d entries, want 3", len(repo.Entries))
	}

	digest := repo.Entries[0].ArtifactDigest
	seenConfigs := map[string]bool{}
	for _, e := range repo.Entries {
		if e.ArtifactDigest != digest {
			t.Errorf("%s has artifact digest %s, want the shared %s", e.Name, e.ArtifactDigest, digest)
		}
		if e.InstanceCount != 1 {
			t.Errorf("%s runs %d instances, want 1", e.Name, e.InstanceCount)
		}
		if seenConfigs[e.ConfigDigest] {
			t.Errorf("%s shares a config digest with another entry; the entries must differ", e.Name)
		}
		seenConfigs[e.ConfigDigest] = true
	}
}

// A model whose bytes do not match the catalog must never be loaded: the run
// would otherwise report results for a model nobody declared.
func TestMaterializeRefusesAnArtifactThatFailsItsDigest(t *testing.T) {
	catalog, artifactDir := testCatalog(t)
	path := ArtifactPath(artifactDir, "synthetic_k8_p65536")
	if err := os.WriteFile(path, []byte("a different graph entirely"), 0o644); err != nil {
		t.Fatalf("tamper: %v", err)
	}

	_, err := Materialize(Request{
		Catalog:     catalog,
		ArtifactID:  "synthetic_k8_p65536",
		ArtifactDir: artifactDir,
		RuntimeDir:  filepath.Join(t.TempDir(), "runtime"),
		Kinds:       []EntryKind{EntryUnbatched},
	})
	if err == nil {
		t.Fatal("materializing a tampered artifact must fail")
	}
	if !strings.Contains(err.Error(), "digest") {
		t.Errorf("error should name the digest mismatch, got: %v", err)
	}
}

// Materializing must not leave a previous cell's entry behind, or a run could
// load a model its manifest never named.
func TestMaterializeClearsAStaleRepository(t *testing.T) {
	catalog, artifactDir := testCatalog(t)
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	stale := filepath.Join(runtimeDir, "leftover_model", "1")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatalf("seed stale entry: %v", err)
	}

	if _, err := Materialize(Request{
		Catalog:     catalog,
		ArtifactID:  "synthetic_k8_p65536",
		ArtifactDir: artifactDir,
		RuntimeDir:  runtimeDir,
		Kinds:       []EntryKind{EntryUnbatched},
	}); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runtimeDir, "leftover_model")); !os.IsNotExist(err) {
		t.Error("a stale entry survived materialization")
	}
}

// The real catalog in the repository must parse and describe an attesting model.
func TestRepositoryCatalogParses(t *testing.T) {
	catalog, err := LoadCatalog(filepath.Join("..", "..", "artifacts", "catalog.json"))
	if err != nil {
		t.Skipf("no generated catalog yet (run `make models`): %v", err)
	}
	var attesting, echo int
	for _, a := range catalog.Artifacts {
		if a.SelfAttesting() {
			attesting++
		} else {
			echo++
		}
	}
	if attesting == 0 {
		t.Error("the catalog must contain at least one self-attesting model")
	}
	if echo == 0 {
		t.Error("the naive-echo counter-fixture is permanent and belongs in the catalog (ADR-0007)")
	}
}
