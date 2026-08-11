package executor

import (
	"strings"
	"testing"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// Every cell in the design, and the executor its declared properties select.
//
// The ones that must fail are in the table for the same reason as the ones that
// must pass: a cell whose executor this build lacks has to be refused by name,
// because falling back to the individual executor would run a V=on cell as
// V=off and produce n=B, J=B and every execution of batch size 1 — precisely
// what that cell's own contamination check is watching for, so nothing
// downstream would catch it.
func TestExecutorSelectionFollowsTheCellsDeclaredProperties(t *testing.T) {
	cases := map[identity.Cell]struct {
		kind      Kind
		available bool
	}{
		identity.CellD0:     {KindIndividual, true},
		identity.CellF00:    {KindIndividual, true},
		identity.CellF10:    {KindIndividual, true},
		identity.CellF00Seq: {KindIndividual, true},
		identity.CellF01:    {KindDynamic, false},
		identity.CellF11D:   {KindDynamic, false},
		identity.CellF11P:   {KindPreformed, false},
	}

	// The table has to cover the design, or a cell added later selects silently.
	for _, cell := range identity.AllCells() {
		if _, ok := cases[cell]; !ok {
			t.Errorf("cell %s is in the design but this table does not say what it selects", cell)
		}
	}

	for cell, want := range cases {
		got, err := SelectKind(cell)
		if !want.available {
			if err == nil {
				t.Errorf("%s selects the %s executor, which this build does not have; it should be refused", cell, want.kind)
				continue
			}
			if !strings.Contains(err.Error(), string(cell)) || !strings.Contains(err.Error(), string(want.kind)) {
				t.Errorf("%s: error %q should name both the cell and the %s executor", cell, err, want.kind)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", cell, err)
			continue
		}
		if got != want.kind {
			t.Errorf("%s selects the %s executor, want %s", cell, got, want.kind)
		}
	}
}

// Factor A never reaches the choice. F00 and F10 differ in how many logical
// requests share an envelope and in nothing the executor sees, which is what
// makes their contrast an envelope contrast rather than a comparison of two
// client implementations.
func TestFactorADoesNotChangeTheExecutor(t *testing.T) {
	for _, pair := range [][2]identity.Cell{
		{identity.CellF00, identity.CellF10},
		{identity.CellF01, identity.CellF11D},
	} {
		off, offErr := SelectKind(pair[0])
		on, onErr := SelectKind(pair[1])
		if (offErr == nil) != (onErr == nil) {
			t.Errorf("%s and %s disagree about whether an executor exists: %v vs %v", pair[0], pair[1], offErr, onErr)
			continue
		}
		if off != on {
			t.Errorf("%s selects %s but %s selects %s; aggregation is not a compute decision", pair[0], off, pair[1], on)
		}
	}
}
