package direct

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"

	"github.com/matthewhoung/batch2go/internal/identity"
)

const (
	modulePath   = "github.com/matthewhoung/batch2go"
	directPkg    = modulePath + "/internal/client/direct"
	envelopePkg  = modulePath + "/internal/envelope"
	proxyPkg     = modulePath + "/internal/proxy"
	adapterPkg   = modulePath + "/internal/adapter"
	tritonPkg    = modulePath + "/internal/triton"
	sharedClient = modulePath + "/internal/client/shared"
)

// pkgInfo is the slice of `go list` output these tests need.
type pkgInfo struct {
	ImportPath string   `json:"ImportPath"`
	Deps       []string `json:"Deps"`
	Imports    []string `json:"Imports"`
}

func goList(t *testing.T, patterns ...string) []pkgInfo {
	t.Helper()
	args := append([]string{"list", "-json"}, patterns...)
	cmd := exec.Command("go", args...)
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("go list unavailable: %v", err)
	}

	var pkgs []pkgInfo
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for dec.More() {
		var p pkgInfo
		if err := dec.Decode(&p); err != nil {
			t.Fatalf("decode go list output: %v", err)
		}
		pkgs = append(pkgs, p)
	}
	return pkgs
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// The direct client must be incapable of speaking the shared path. This is the
// structural half of the guarantee: with no dependency on the envelope protocol,
// the proxy, or the adapter, there is no expression in this package that
// produces an envelope — the misuse is impossible rather than discouraged.
func TestDirectClientCannotReachTheSharedPath(t *testing.T) {
	pkgs := goList(t, directPkg)
	if len(pkgs) != 1 {
		t.Fatalf("go list returned %d packages for the direct client", len(pkgs))
	}

	for _, forbidden := range []string{envelopePkg, proxyPkg, adapterPkg} {
		if contains(pkgs[0].Deps, forbidden) {
			t.Errorf("the direct client depends on %s; it could then construct a shared-path request", forbidden)
		}
	}
	if !contains(pkgs[0].Deps, tritonPkg) {
		t.Error("the direct client should reach Triton directly; that is what makes it the diagnostic path")
	}
}

// Nothing on the shared path may reach the direct client. If the proxy or the
// adapter could construct one, a factorial cell could bypass the hop it is
// defined to traverse.
func TestSharedPathPackagesCannotReachTheDirectClient(t *testing.T) {
	for _, pkg := range []string{proxyPkg, adapterPkg, envelopePkg} {
		pkgs := goList(t, pkg)
		if len(pkgs) == 0 {
			continue // not yet implemented in this slice
		}
		if contains(pkgs[0].Deps, directPkg) {
			t.Errorf("%s depends on the direct client; a factorial cell could then skip the proxy", pkg)
		}
	}
}

// The mirror-image boundary: the shared-path client must not be able to reach
// Triton, or it could bypass the adapter.
func TestSharedClientCannotReachTritonDirectly(t *testing.T) {
	pkgs := goList(t, sharedClient)
	if len(pkgs) == 0 {
		t.Skip("the shared-path client does not exist yet")
	}
	if contains(pkgs[0].Deps, tritonPkg) {
		t.Errorf("%s depends on %s; the shared path must reach the backend through the adapter",
			sharedClient, tritonPkg)
	}
}

// And the runtime half of the guarantee.
func TestNewRefusesEveryCellButD0(t *testing.T) {
	for _, cell := range identity.AllCells() {
		_, err := New(cell, nil, "model")
		switch cell {
		case identity.CellD0:
			// D0 gets past the cell check and fails on the missing submitter.
			if err == nil || !strings.Contains(err.Error(), "submission engine") {
				t.Errorf("D0 should be accepted by the cell check, got: %v", err)
			}
		default:
			if err == nil {
				t.Errorf("cell %s must not be able to construct a direct client", cell)
				continue
			}
			if !strings.Contains(err.Error(), "shared path") {
				t.Errorf("cell %s rejection should explain the shared path, got: %v", cell, err)
			}
		}
	}
}
