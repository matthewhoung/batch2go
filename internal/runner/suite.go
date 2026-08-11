package runner

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/manifest"
)

// SuiteSchemaVersion is the acceptance-suite format version.
// It became 2 when the suite began declaring the cross-cell comparisons it
// asserts. LoadSuite refuses unknown keys, so a version-1 reader would reject a
// suite carrying them rather than silently running fewer assertions than the
// file names.
const SuiteSchemaVersion = 2

// Suite is the declared acceptance set: which cells this build claims to run,
// and in what order.
//
// It is a file rather than a set of build stanzas because which cells are
// claimed is an experimental statement, not a build step. Gate G1 eventually
// needs seven cells at two cohort sizes; a shell that listed them would be
// deciding something the manifests exist to declare (CODEBASE.md §3). Adding a
// cell to the suite is adding its manifest path here, and the expected evidence
// that manifest already carries.
type Suite struct {
	SchemaVersion int    `json:"schema_version"`
	Description   string `json:"description,omitempty"`

	// Manifests are run in declared order. Paths are relative to the repository
	// root, as the paths inside the manifests themselves are.
	Manifests []string `json:"manifests"`

	// SameImplementation names the cell pairs whose runs must resolve to one
	// implementation. It is declared here rather than hardcoded for the same
	// reason the cells are: which contrasts this build claims is an experimental
	// statement, and a shell or a switch asserting them would be deciding
	// something the suite exists to declare.
	SameImplementation []CellPair `json:"same_implementation,omitempty"`

	contracts []*manifest.Manifest
}

// CellPair is one declared cross-cell assertion.
type CellPair struct {
	A identity.Cell `json:"a"`
	B identity.Cell `json:"b"`

	// Why states what the pair establishes, so a reader of the suite file learns
	// why the comparison is there without opening the code that runs it.
	Why string `json:"why,omitempty"`
}

// LoadSuite reads a declared acceptance suite and parses every manifest it
// names.
//
// Every manifest is parsed here rather than at its turn, so a typo in the last
// cell fails before the first one occupies the stack for a quarter of an hour.
func LoadSuite(path string) (*Suite, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("runner: open suite %s: %w", path, err)
	}
	defer f.Close()

	var s Suite
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("runner: parse suite %s: %w", path, err)
	}
	if s.SchemaVersion != SuiteSchemaVersion {
		return nil, fmt.Errorf("runner: suite %s has schema version %d, this build reads %d",
			path, s.SchemaVersion, SuiteSchemaVersion)
	}
	if len(s.Manifests) == 0 {
		return nil, fmt.Errorf("runner: suite %s declares no contracts; an empty acceptance suite passes by saying nothing", path)
	}

	seen := make(map[string]bool, len(s.Manifests))
	bundles := make(map[string]string, len(s.Manifests))
	for _, p := range s.Manifests {
		if seen[p] {
			return nil, fmt.Errorf("runner: suite %s declares %s twice", path, p)
		}
		seen[p] = true

		m, err := manifest.Load(p)
		if err != nil {
			return nil, fmt.Errorf("runner: suite %s: %w", path, err)
		}
		// Two contracts writing one bundle directory would overwrite each other,
		// and the suite would report both green having judged the second one twice.
		dir := Dir(m)
		if first := bundles[dir]; first != "" {
			return nil, fmt.Errorf("runner: suite %s: %s and %s both write bundle %s; the second would overwrite the first",
				path, first, p, dir)
		}
		bundles[dir] = p
		s.contracts = append(s.contracts, m)
	}

	// A comparison naming a cell the suite does not run has no bundle to read and
	// would be skipped at the moment it mattered. Refusing it here is the same
	// rule as refusing an empty suite: an assertion that cannot execute is worse
	// than no assertion, because the file says it is there.
	declared := make(map[identity.Cell]bool, len(s.contracts))
	for _, m := range s.contracts {
		declared[m.Cell] = true
	}
	for _, pair := range s.SameImplementation {
		if pair.A == pair.B {
			return nil, fmt.Errorf("runner: suite %s compares %s with itself, which passes by saying nothing", path, pair.A)
		}
		for _, cell := range []identity.Cell{pair.A, pair.B} {
			if !declared[cell] {
				return nil, fmt.Errorf(
					"runner: suite %s declares a %s/%s comparison but does not run %s, so the comparison has no bundle to read",
					path, pair.A, pair.B, cell)
			}
		}
	}
	return &s, nil
}

// Contracts are the parsed manifests, in declared order.
func (s *Suite) Contracts() []*manifest.Manifest { return s.contracts }
