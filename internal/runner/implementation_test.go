package runner

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/matthewhoung/batch2go/internal/adapter"
	"github.com/matthewhoung/batch2go/internal/executor"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/modelrepo"
	"github.com/matthewhoung/batch2go/internal/triton"
)

// The claim the aggregation contrast rests on: F00 and F10 differ in transport
// aggregation and in nothing else. These tests are what turn that from an
// intention into an assertion over two archives.

// sharedImplementation is what both cells' adapters must have built. The bundles
// below differ only in the ways the two cells are supposed to differ.
func sharedImplementation(cell identity.Cell) *Bundle {
	return &Bundle{
		SchemaVersion:         BundleSchemaVersion,
		Cell:                  cell,
		Run:                   identity.RunID("run-" + string(cell)),
		State:                 StateCompleted,
		EnvelopeSchemaVersion: 1,
		Adapter: &adapter.ProcessRecord{
			SchemaVersion: adapter.ProcessRecordSchemaVersion,
			Run:           identity.RunID("run-" + string(cell)),
			Cell:          cell,
			Executor:      executor.KindIndividual,
			ModelEntry:    "m_unbatched",
			FeatureWidth:  16,
			PayloadFloats: 65536,
			Downstream: triton.Config{
				Endpoint:              "127.0.0.1:8001",
				MaxMessageBytes:       268435456,
				InitialWindowSize:     4194304,
				InitialConnWindowSize: 16777216,
			},
			Serving: adapter.ServingConfig{
				MaxMessageBytes:       268435456,
				InitialWindowSize:     4194304,
				InitialConnWindowSize: 16777216,
			},
		},
		ModelEntry: modelrepo.Entry{
			Name:           "m_unbatched",
			Kind:           modelrepo.EntryUnbatched,
			ArtifactID:     "synthetic_k8_p65536",
			ArtifactDigest: "sha256:aaaa",
			ConfigDigest:   "sha256:bbbb",
		},
		ModelIdentity: triton.ModelIdentity{
			MaxBatchSize:  0,
			InstanceKind:  "KIND_GPU",
			InstanceCount: 1,
		},
	}
}

// The assertion itself. Two cells whose adapters wired the same executor, the
// same model and the same transport limits resolve to one implementation, and
// the digest names it.
func TestF00AndF10ResolveToOneImplementation(t *testing.T) {
	f00 := sharedImplementation(identity.CellF00)
	f10 := sharedImplementation(identity.CellF10)

	cmp, err := SameImplementation(f00, f10)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if !cmp.Same {
		t.Fatalf("two cells of one implementation were reported as differing: %+v", cmp.Differences)
	}
	if !strings.HasPrefix(cmp.Digest, "sha256:") {
		t.Errorf("the comparison carries no digest, got %q", cmp.Digest)
	}
}

// Each property that must match gets its own case. A comparison that failed only
// on, say, the executor kind would let a differing payload width through, and
// the contrast would silently become a contrast of two things.
func TestDeliberatelyDifferingBundlesFailTheComparison(t *testing.T) {
	for name, differ := range map[string]func(*Bundle){
		"a different executor": func(b *Bundle) {
			b.Adapter.Executor = executor.Kind("some-other-executor")
		},
		"a different model entry": func(b *Bundle) {
			b.Adapter.ModelEntry = "m_dynamic"
		},
		"a different payload width": func(b *Bundle) {
			b.Adapter.PayloadFloats = 1024
		},
		"a different feature width": func(b *Bundle) {
			b.Adapter.FeatureWidth = 32
		},
		"a different downstream message ceiling": func(b *Bundle) {
			b.Adapter.Downstream.MaxMessageBytes = 1 << 20
		},
		"a different downstream flow-control window": func(b *Bundle) {
			b.Adapter.Downstream.InitialWindowSize = 65536
		},
		"a different serving message ceiling": func(b *Bundle) {
			b.Adapter.Serving.MaxMessageBytes = 1 << 20
		},
		"a different artifact": func(b *Bundle) {
			b.ModelEntry.ArtifactDigest = "sha256:cccc"
		},
		"a different model configuration": func(b *Bundle) {
			b.ModelEntry.ConfigDigest = "sha256:dddd"
		},
		"a different served batch ceiling": func(b *Bundle) {
			b.ModelIdentity.MaxBatchSize = 4
		},
		"dynamic batching enabled on one side": func(b *Bundle) {
			b.ModelIdentity.DynamicBatchingEnabled = true
		},
		"a different instance count": func(b *Bundle) {
			b.ModelIdentity.InstanceCount = 2
		},
		"a different envelope protocol": func(b *Bundle) {
			b.EnvelopeSchemaVersion = 2
		},
	} {
		t.Run(name, func(t *testing.T) {
			f00 := sharedImplementation(identity.CellF00)
			f10 := sharedImplementation(identity.CellF10)
			differ(f10)

			cmp, err := SameImplementation(f00, f10)
			if err != nil {
				t.Fatalf("compare: %v", err)
			}
			if cmp.Same {
				t.Fatalf("%s was accepted as the same implementation", name)
			}
			if len(cmp.Differences) == 0 {
				t.Fatal("the comparison failed without naming a property")
			}
			if cmp.Digest != "" {
				t.Error("a failed comparison carries a digest, which would read as a match")
			}
			for _, d := range cmp.Differences {
				if d.A == d.B {
					t.Errorf("property %s is reported as differing but both sides read %q", d.Property, d.A)
				}
			}
		})
	}
}

// The things that are SUPPOSED to differ must not fail the comparison, or the
// assertion could never hold for the two cells it exists for.
func TestTheTreatmentItselfDoesNotFailTheComparison(t *testing.T) {
	f00 := sharedImplementation(identity.CellF00)
	f10 := sharedImplementation(identity.CellF10)

	// Different cells, different run ids, and a formation deadline only one of
	// them has — the treatment and its parameters.
	f10.Run = "run-f10-different"
	f10.Adapter.Run = "run-f10-different"
	f10.Adapter.ClockDomain = "cd-somethingelse"

	cmp, err := SameImplementation(f00, f10)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if !cmp.Same {
		t.Errorf("the comparison failed on properties the two cells are supposed to differ in: %+v", cmp.Differences)
	}
}

// Three ways the assertion could be satisfied by saying nothing, each refused.
func TestTheComparisonRefusesWhatItCannotAssert(t *testing.T) {
	completed := sharedImplementation(identity.CellF00)

	t.Run("a run that did not complete", func(t *testing.T) {
		failed := sharedImplementation(identity.CellF10)
		failed.State = StateFailed
		if _, err := SameImplementation(completed, failed); err == nil {
			t.Error("a failed run's implementation was compared; it never served a request")
		}
	})

	t.Run("a bundle against itself", func(t *testing.T) {
		if _, err := SameImplementation(completed, sharedImplementation(identity.CellF00)); err == nil {
			t.Error("a cell was compared with itself, which passes by saying nothing")
		}
	})

	t.Run("a bundle with no adapter", func(t *testing.T) {
		direct := sharedImplementation(identity.CellD0)
		direct.Adapter = nil
		if _, err := SameImplementation(completed, direct); err == nil {
			t.Error("a bundle carrying no adapter record was compared; what its adapter built is unknown")
		}
	})
}

// The golden digest. This is what actually delivers "a new property cannot be
// forgotten silently": adding, removing, renaming or reordering a field in
// Implementation changes the canonical encoding and breaks this test, so whoever
// changes the property set has to say so rather than discovering later that a
// comparison had stopped covering something.
//
// If this fails and the change was intended, update the constant in the same
// commit that changed the struct — and check that the new property is one two
// cells of one factor genuinely must share.
func TestTheComparedPropertySetIsPinned(t *testing.T) {
	const want = "sha256:a8ccec34b6ef6456a1fd27ffd77d6922894872c44710e2e20ce3b9a58e0ce367"

	impl, err := ImplementationOf(sharedImplementation(identity.CellF10))
	if err != nil {
		t.Fatalf("implementation: %v", err)
	}
	got, err := impl.Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if got != want {
		t.Errorf("the compared property set has changed.\n got: %s\nwant: %s\n\n"+
			"If that was intended, update this constant in the same commit — and satisfy yourself that the "+
			"property added or removed is one two cells of a single factor must share.", got, want)
	}
}

// The comparison over archives, not over structs.
//
// Every test above builds its bundles in memory, which is exactly how a defect
// in the loading path survived them: the only two ways this assertion is ever
// invoked — the subcommand and the acceptance suite — both name a run by its
// directory, and handing a directory to a function that opens a file fails at
// the first read rather than at the open. This test walks the path an operator
// walks, so a mistake there is a failing test rather than a failing suite run
// after a quarter of an hour of GPU time.
func TestTheComparisonReadsBundlesFromTheirDirectories(t *testing.T) {
	write := func(t *testing.T, b *Bundle) string {
		t.Helper()
		layout, err := NewLayout(filepath.Join(t.TempDir(), string(b.Run)))
		if err != nil {
			t.Fatalf("layout: %v", err)
		}
		if err := WriteBundle(layout, b); err != nil {
			t.Fatalf("write bundle: %v", err)
		}
		return layout.Root
	}

	dirA := write(t, sharedImplementation(identity.CellF00))
	dirB := write(t, sharedImplementation(identity.CellF10))

	loadedA, err := LoadBundleDir(dirA)
	if err != nil {
		t.Fatalf("load %s: %v", dirA, err)
	}
	loadedB, err := LoadBundleDir(dirB)
	if err != nil {
		t.Fatalf("load %s: %v", dirB, err)
	}

	cmp, err := SameImplementation(loadedA, loadedB)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if !cmp.Same {
		t.Fatalf("two cells of one implementation differed after a round trip through disk: %+v", cmp.Differences)
	}

	// The adapter record has to survive the round trip too. It is the only part
	// of the comparison that comes from another process, and a bundle that
	// dropped it would compare two blanks and call them equal.
	if loadedA.Adapter == nil || loadedB.Adapter == nil {
		t.Fatal("the adapter record did not survive being written and read back")
	}
	if loadedA.Adapter.Executor == "" {
		t.Error("the adapter record round-tripped empty")
	}
}
