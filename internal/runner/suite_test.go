package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// atRepoRoot runs the test from the module root and puts the working directory
// back afterwards. A suite names its manifests the way the manifests name their
// own inputs — relative to the repository root — so reading one means being
// there.
func atRepoRoot(t *testing.T) string {
	t.Helper()
	was, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := was
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the module root")
		}
		dir = parent
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir to module root: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(was) })
	return dir
}

// The suite this build ships has to load, and every manifest it names has to
// parse. A typo in the last cell must fail here rather than after the earlier
// cells have occupied the stack for a quarter of an hour.
func TestDeclaredSuiteLoadsAndEveryContractParses(t *testing.T) {
	atRepoRoot(t)

	suite, err := LoadSuite("experiments/contracts.json")
	if err != nil {
		t.Fatalf("load suite: %v", err)
	}
	contracts := suite.Contracts()
	if len(contracts) != len(suite.Manifests) {
		t.Fatalf("suite declares %d manifests but parsed %d", len(suite.Manifests), len(contracts))
	}
	if len(contracts) == 0 {
		t.Fatal("the acceptance suite declares no contracts")
	}

	for i, m := range contracts {
		if !m.Cell.Implemented() {
			t.Errorf("%s declares cell %s, which this build cannot run", suite.Manifests[i], m.Cell)
		}
		if m.Cohort.Size <= 0 || m.ExpectedEvidence.Executions <= 0 {
			t.Errorf("%s carries no expected evidence to judge its run against", suite.Manifests[i])
		}
	}
}

func TestLoadSuiteRefusesASuiteItCannotTrust(t *testing.T) {
	atRepoRoot(t)

	// A second path holding the same manifest: a different contract by path, the
	// same bundle directory by run id.
	goodCopy := filepath.Join(t.TempDir(), "f00-again.json")
	body, err := os.ReadFile("experiments/manifests/f00-envv-b4.json")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if err := os.WriteFile(goodCopy, body, 0o644); err != nil {
		t.Fatalf("write manifest copy: %v", err)
	}

	const good = "experiments/manifests/f00-envv-b4.json"
	cases := map[string]struct {
		body string
		want string
	}{
		"another schema version": {
			`{"schema_version": 99, "manifests": ["` + good + `"]}`, "schema version",
		},
		"no contracts": {
			`{"schema_version": 1, "manifests": []}`, "no contracts",
		},
		"a cell declared twice": {
			`{"schema_version": 1, "manifests": ["` + good + `", "` + good + `"]}`, "twice",
		},
		"a manifest that is not there": {
			`{"schema_version": 1, "manifests": ["experiments/manifests/nope.json"]}`, "nope.json",
		},
		"two contracts writing one bundle": {
			`{"schema_version": 1, "manifests": ["` + good + `", "` + goodCopy + `"]}`, "overwrite",
		},
		"a key nobody reads": {
			`{"schema_version": 1, "manifests": ["` + good + `"], "cells": ["F10"]}`, "cells",
		},
	}

	for name, tc := range cases {
		path := filepath.Join(t.TempDir(), "contracts.json")
		if err := os.WriteFile(path, []byte(tc.body), 0o644); err != nil {
			t.Fatalf("%s: write suite: %v", name, err)
		}

		_, err := LoadSuite(path)
		if err == nil {
			t.Errorf("%s: should have been refused", name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q should mention %q", name, err, tc.want)
		}
	}
}
