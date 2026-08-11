package runner

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/matthewhoung/batch2go/internal/manifest"
)

// SuiteSchemaVersion is the acceptance-suite format version.
const SuiteSchemaVersion = 1

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

	contracts []*manifest.Manifest
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
	return &s, nil
}

// Contracts are the parsed manifests, in declared order.
func (s *Suite) Contracts() []*manifest.Manifest { return s.contracts }
