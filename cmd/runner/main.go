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
`)
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

	bundle, verdict, err := runner.ValidateBundle(*bundleDir)
	if err != nil {
		return err
	}
	if err := runner.WriteVerdict(*bundleDir, verdict); err != nil {
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
		if !*verbose && limit > 10 {
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

	opts := runner.Options{ImageDigest: *imageDigest}
	if !*quiet {
		opts.Logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "runner: "+format+"\n", args...)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	bundle, err := runner.Run(ctx, m, opts)
	if bundle != nil {
		fmt.Printf("%s %s cell=%s cohorts=%d executions=%d\n",
			bundle.State, bundle.Run, bundle.Cell, len(bundle.Schedule), bundle.TritonStats.ExecutionCount)
	}
	return err
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
