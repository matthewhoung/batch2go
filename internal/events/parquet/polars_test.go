package parquet

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// repoRoot walks up from the test's working directory to the module root, so
// the analysis project can be located without hard-coding a path depth.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the module root")
		}
		dir = parent
	}
}

// The archive is only useful if the analysis toolchain can read it. This runs
// the Python checker against a freshly written archive, which is the same code
// path `make analysis-check` uses — and the check is an independent restatement
// of the schema, not a shared implementation, so agreement means something.
func TestArchiveLoadsInPolarsWithSchemaColumnNames(t *testing.T) {
	uv, err := exec.LookPath("uv")
	if err != nil {
		t.Skip("uv is not installed; run `make analysis-check` to exercise the polars reader")
	}

	root := repoRoot(t)
	archive := filepath.Join(t.TempDir(), "events.parquet")
	if err := Write(archive, sampleRecords()); err != nil {
		t.Fatalf("write archive: %v", err)
	}

	cmd := exec.Command(uv, "run", "--project", filepath.Join(root, "analysis"),
		"python", "-m", "batch2go_analysis.check", archive)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("polars could not load the archive: %v\n%s", err, out)
	}
	t.Logf("%s", out)
}
