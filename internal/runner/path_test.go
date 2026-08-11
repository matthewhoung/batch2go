package runner

import (
	"strings"
	"testing"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// The runner's gate consults identity's list rather than carrying one of its
// own. It used to carry one, and two lists that can disagree are one list plus
// a latent bug: a cell added to the manifest validator but not here would pass
// validation, materialize a model repository, start a data plane, and only then
// be refused.
func TestRunnerAgreesWithTheCellAuthority(t *testing.T) {
	for _, cell := range identity.AllCells() {
		err := checkCellImplemented(cell)
		if cell.Implemented() {
			if err != nil {
				t.Errorf("%s is implemented but the runner refuses it: %v", cell, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s is not implemented and the runner must refuse it", cell)
			continue
		}
		if !strings.Contains(err.Error(), string(cell)) {
			t.Errorf("%s: refusal %q should name the cell", cell, err)
		}
	}
}
