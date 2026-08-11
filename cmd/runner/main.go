// Command runner is the control plane: it turns a manifest into executed,
// validated runs. Entry points here parse flags and wire components; no
// experiment or measurement logic lives in this package (CODEBASE.md §5).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/manifest"
	"github.com/matthewhoung/batch2go/internal/modelrepo"
	"github.com/matthewhoung/batch2go/internal/runner"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "materialize":
		err = materialize(os.Args[2:])
	case "run":
		err = run(os.Args[2:])
	case "validate":
		err = validateBundle(os.Args[2:])
	case "validate-manifest":
		err = validateManifest(os.Args[2:])
	case "contracts":
		err = contracts(os.Args[2:])
	case "same-implementation":
		err = sameImplementation(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "runner: unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: runner <subcommand> [flags]

subcommands:
  materialize        build the runtime Triton model repository from verified artifacts
  run                execute a manifest and write its run bundle
  validate           judge an archived run bundle offline
  validate-manifest  check a manifest without running it
  contracts          run every contract a declared acceptance suite names
  same-implementation  assert two archived cells differ in their factor levels and nothing else
`)
}

// sameImplementation asserts, over two archives, that a pair of cells resolved
// to one implementation.
//
// The aggregation contrast rests on F00 and F10 differing in transport
// aggregation and in nothing else. Left unchecked that is an article of faith:
// the two runs use different manifests and start different processes, and a
// comparison of two client implementations wearing cell labels would produce a
// clean effect and a wrong one. Like every other judgement here it reads
// archives only — no network, no live state.
func sameImplementation(args []string) error {
	fs := flag.NewFlagSet("same-implementation", flag.ExitOnError)
	a := fs.String("a", "", "run bundle directory for the first cell")
	b := fs.String("b", "", "run bundle directory for the second cell")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *a == "" || *b == "" {
		return fmt.Errorf("same-implementation needs --a and --b")
	}

	bundleA, err := runner.LoadBundleDir(*a)
	if err != nil {
		return err
	}
	bundleB, err := runner.LoadBundleDir(*b)
	if err != nil {
		return err
	}
	return reportComparison(bundleA, bundleB)
}

// reportComparison prints the finding and turns it into an exit status.
func reportComparison(a, b *runner.Bundle) error {
	cmp, err := runner.SameImplementation(a, b)
	if err != nil {
		return err
	}
	if cmp.Same {
		fmt.Printf("%s and %s resolved to one implementation\n  digest %s\n", cmp.CellA, cmp.CellB, cmp.Digest)
		return nil
	}
	fmt.Printf("%s and %s did NOT resolve to one implementation:\n", cmp.CellA, cmp.CellB)
	for _, d := range cmp.Differences {
		fmt.Printf("  %-34s %s != %s\n", d.Property, d.A, d.B)
	}
	return fmt.Errorf("%d properties differ; the contrast between these cells would be a contrast of more than one thing",
		len(cmp.Differences))
}

// validateBundle judges an archived run. It touches no network and no live
// state: the verdict comes from the bundle alone, which is what makes it
// reproducible by anyone holding the archive.
func validateBundle(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	bundleDir := fs.String("bundle", "", "run bundle directory")
	verbose := fs.Bool("verbose", false, "print every defect")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *bundleDir == "" {
		return fmt.Errorf("validate needs --bundle")
	}
	return judgeBundle(*bundleDir, *verbose)
}

// judgeBundle reaches a verdict from an archived bundle and reports it. It
// touches no network and no live state, which is what makes the verdict
// reproducible by anyone holding the archive.
func judgeBundle(bundleDir string, verbose bool) error {
	bundle, verdict, err := runner.ValidateBundle(bundleDir)
	if err != nil {
		return err
	}
	if err := runner.WriteVerdict(bundleDir, verdict); err != nil {
		return err
	}

	status := "FAILED"
	if verdict.Passed {
		status = "PASSED"
	}
	fmt.Printf("%s %s cell=%s\n", status, bundle.Run, bundle.Cell)
	for _, c := range verdict.Checks {
		mark := "fail"
		if c.Passed {
			mark = "ok  "
		}
		fmt.Printf("  %s %-18s %s\n", mark, c.Name, c.Detail)
	}

	// Residuals are reported whether or not they passed, signed and never
	// relabeled (M1 §4).
	fmt.Printf("  conservation: max |residual| %.4f%% of path, tolerance %.2f%% (cohort intervals reported, not gated)\n",
		verdict.Conservation.MaxAbsResidualFraction*100,
		verdict.Conservation.ToleranceFraction*100)
	for _, s := range verdict.Conservation.Stages {
		fmt.Printf("    %-18s n=%-5d median=%8dns  min=%8dns  max=%8dns\n",
			s.Name, s.Count, s.MedianNanos, s.MinNanos, s.MaxNanos)
	}

	if defects := verdict.Defects(); len(defects) > 0 {
		limit := len(defects)
		if !verbose && limit > 10 {
			limit = 10
		}
		fmt.Printf("  %d defects:\n", len(defects))
		for _, d := range defects[:limit] {
			fmt.Printf("    %s\n", d)
		}
		if limit < len(defects) {
			fmt.Printf("    ... %d more (use --verbose)\n", len(defects)-limit)
		}
	}

	if !verdict.Passed {
		return fmt.Errorf("bundle %s did not validate", bundle.Run)
	}
	return nil
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	manifestPath := fs.String("manifest", "", "run manifest")
	imageDigest := fs.String("image-digest", "", "pinned server container digest, recorded in the bundle")
	quiet := fs.Bool("quiet", false, "suppress progress output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return fmt.Errorf("run needs --manifest")
	}

	m, err := manifest.Load(*manifestPath)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, err = executeManifest(ctx, m, runOptions(*imageDigest, *quiet))
	return err
}

// contracts runs every contract the declared acceptance suite names.
//
// Each one is executed and then judged from its own archive, because the two
// are different claims: that the stack can produce the run, and that the run's
// records support the verdict without the stack being there. The suite stops at
// the first failure — running on would report verdicts produced by a stack whose
// earlier condition did not hold.
func contracts(args []string) error {
	fs := flag.NewFlagSet("contracts", flag.ExitOnError)
	suitePath := fs.String("suite", "experiments/contracts.json", "declared acceptance suite")
	imageDigest := fs.String("image-digest", "", "pinned server container digest, recorded in every bundle")
	quiet := fs.Bool("quiet", false, "suppress progress output")
	verbose := fs.Bool("verbose", false, "print every defect")
	if err := fs.Parse(args); err != nil {
		return err
	}

	suite, err := runner.LoadSuite(*suitePath)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	declared := suite.Contracts()
	cells := make([]string, 0, len(declared))
	for _, m := range declared {
		cells = append(cells, string(m.Cell))
	}
	fmt.Printf("suite %s: %d contracts (%s)\n\n", *suitePath, len(declared), strings.Join(cells, ", "))

	opts := runOptions(*imageDigest, *quiet)
	for i, m := range declared {
		fmt.Printf("== contract %d of %d: %s B=%d ==\n", i+1, len(declared), m.Cell, m.Cohort.Size)

		bundle, err := executeManifest(ctx, m, opts)
		if err != nil {
			return fmt.Errorf("contract %s: %w", m.Cell, err)
		}
		if err := judgeBundle(runner.Dir(m), *verbose); err != nil {
			return fmt.Errorf("contract %s (%s): %w", m.Cell, bundle.Run, err)
		}
		fmt.Printf("\n")
	}

	// The cells are green; now the cross-cell assertions the suite declares. They
	// run last because they read the bundles the runs above just wrote, and they
	// are part of acceptance rather than a separate errand: a contrast between two
	// cells is only a contrast of one factor if this holds, so a suite that ran
	// every cell green and never checked it would have proved less than it says.
	for _, pair := range suite.SameImplementation {
		fmt.Printf("== same implementation: %s vs %s ==\n", pair.A, pair.B)
		if pair.Why != "" {
			fmt.Printf("%s\n", pair.Why)
		}
		a, err := bundleForCell(declared, pair.A)
		if err != nil {
			return err
		}
		b, err := bundleForCell(declared, pair.B)
		if err != nil {
			return err
		}
		if err := reportComparison(a, b); err != nil {
			return fmt.Errorf("same implementation %s/%s: %w", pair.A, pair.B, err)
		}
		fmt.Printf("\n")
	}

	fmt.Printf("contracts: %d of %d validated green (%s), %d cross-cell assertions held\n",
		len(declared), len(declared), strings.Join(cells, ", "), len(suite.SameImplementation))
	return nil
}

// bundleForCell loads the archive the suite's run of that cell just wrote. The
// bundle is read back from disk rather than kept from the run, so the comparison
// judges the archive — the same thing anyone re-running it later would hold.
func bundleForCell(declared []*manifest.Manifest, cell identity.Cell) (*runner.Bundle, error) {
	for _, m := range declared {
		if m.Cell == cell {
			return runner.LoadBundleDir(runner.Dir(m))
		}
	}
	return nil, fmt.Errorf("the suite declares no contract for %s", cell)
}

// executeManifest runs one manifest and reports what the run produced. A failed
// run still wrote a bundle, so its line is printed before the error is returned.
func executeManifest(ctx context.Context, m *manifest.Manifest, opts runner.Options) (*runner.Bundle, error) {
	bundle, err := runner.Run(ctx, m, opts)
	if bundle != nil {
		fmt.Printf("%s %s cell=%s cohorts=%d executions=%d\n",
			bundle.State, bundle.Run, bundle.Cell, len(bundle.Schedule), bundle.TritonStats.ExecutionCount)
	}
	return bundle, err
}

func runOptions(imageDigest string, quiet bool) runner.Options {
	opts := runner.Options{ImageDigest: imageDigest}
	if !quiet {
		opts.Logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "runner: "+format+"\n", args...)
		}
	}
	return opts
}

func validateManifest(args []string) error {
	fs := flag.NewFlagSet("validate-manifest", flag.ExitOnError)
	manifestPath := fs.String("manifest", "", "run manifest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return fmt.Errorf("validate-manifest needs --manifest")
	}
	m, err := manifest.Load(*manifestPath)
	if err != nil {
		return err
	}
	fmt.Printf("ok %s: cell=%s B=%d cohorts=%d expecting %d executions of batch size %d\n",
		*manifestPath, m.Cell, m.Cohort.Size, m.Cohort.Count,
		m.ExpectedEvidence.Executions, m.ExpectedEvidence.BatchSize)
	return nil
}

func materialize(args []string) error {
	fs := flag.NewFlagSet("materialize", flag.ExitOnError)
	catalogPath := fs.String("catalog", "artifacts/catalog.json", "artifact catalog manifest")
	artifactDir := fs.String("artifact-dir", "artifacts/generated", "directory holding artifact binaries")
	runtimeDir := fs.String("runtime-dir", "results/runtime-models", "runtime Triton model repository to build")
	artifactID := fs.String("artifact-id", "", "artifact to materialize")
	entries := fs.String("entries", "unbatched", "comma-separated entry kinds: unbatched,dynamic,explicit")
	maxBatch := fs.Int("max-batch-size", 4, "max batch size for the dynamic and explicit entries")
	instanceKind := fs.String("instance-kind", "KIND_GPU", "Triton instance kind")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *artifactID == "" {
		return fmt.Errorf("materialize needs --artifact-id")
	}

	catalog, err := modelrepo.LoadCatalog(*catalogPath)
	if err != nil {
		return err
	}
	kinds, err := parseEntryKinds(*entries)
	if err != nil {
		return err
	}

	repo, err := modelrepo.Materialize(modelrepo.Request{
		Catalog:      catalog,
		ArtifactID:   *artifactID,
		ArtifactDir:  *artifactDir,
		RuntimeDir:   *runtimeDir,
		Kinds:        kinds,
		MaxBatchSize: *maxBatch,
		InstanceKind: *instanceKind,
	})
	if err != nil {
		return err
	}

	out, err := json.MarshalIndent(repo, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func parseEntryKinds(s string) ([]modelrepo.EntryKind, error) {
	var kinds []modelrepo.EntryKind
	for _, raw := range strings.Split(s, ",") {
		switch kind := modelrepo.EntryKind(strings.TrimSpace(raw)); kind {
		case modelrepo.EntryUnbatched, modelrepo.EntryDynamic, modelrepo.EntryExplicit:
			kinds = append(kinds, kind)
		case "":
			continue
		default:
			return nil, fmt.Errorf("unknown entry kind %q", raw)
		}
	}
	if len(kinds) == 0 {
		return nil, fmt.Errorf("no entry kinds given")
	}
	return kinds, nil
}
