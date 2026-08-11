package identity

import (
	"strings"
	"testing"
)

// Which cells this build runs is stated once. These assertions are about that
// statement being usable as an authority: it has to list the cells, refuse the
// others by name, and tell a cell outside the design apart from one inside it
// that this slice has not built.
func TestImplementedCellsIsTheAuthority(t *testing.T) {
	implemented := ImplementedCells()
	if len(implemented) == 0 {
		t.Fatal("this build claims to run no cells at all")
	}
	for _, c := range implemented {
		if !c.Implemented() {
			t.Errorf("%s is listed as implemented but does not report itself so", c)
		}
	}

	// The list is a subset of the design, in contract-table order.
	order := map[Cell]int{}
	for i, c := range AllCells() {
		order[c] = i
	}
	last := -1
	for _, c := range implemented {
		i, ok := order[c]
		if !ok {
			t.Errorf("%s is listed as implemented but is not a cell the design defines", c)
			continue
		}
		if i <= last {
			t.Errorf("%s breaks contract-table order", c)
		}
		last = i
	}
}

func TestCheckImplementedTellsTheTwoMistakesApart(t *testing.T) {
	for _, c := range AllCells() {
		err := c.CheckImplemented()
		if c.Implemented() {
			if err != nil {
				t.Errorf("%s is implemented: %v", c, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s is not implemented and must be refused", c)
			continue
		}
		if !strings.Contains(err.Error(), string(c)) {
			t.Errorf("%s: refusal %q should name the cell", c, err)
		}
		if !strings.Contains(err.Error(), "in the design") {
			t.Errorf("%s: refusal %q should say the cell exists but this build has not built it", c, err)
		}
	}

	// A cell the design does not define is a different mistake, and says so.
	err := Cell("F42").CheckImplemented()
	if err == nil {
		t.Fatal("a cell outside the design must be refused")
	}
	if strings.Contains(err.Error(), "in the design") {
		t.Errorf("an unknown cell was reported as a design cell this build lacks: %v", err)
	}
	if !strings.Contains(err.Error(), "unknown cell") {
		t.Errorf("refusal %q should say the cell is not one the design defines", err)
	}
}

// F11-P sits outside the factorial because where its batch is formed is a third
// property of a cell rather than a third factor. Executor selection reads that
// property here instead of testing a cell name at the call site.
func TestPreformsBatchIsOnlyF11P(t *testing.T) {
	for _, c := range AllCells() {
		if got, want := c.PreformsBatch(), c == CellF11P; got != want {
			t.Errorf("%s.PreformsBatch() = %v, want %v", c, got, want)
		}
	}
}
