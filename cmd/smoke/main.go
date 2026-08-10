// Command smoke sends one request through the Triton gateway and verifies the
// uid attestation it comes back with.
//
// It is the diagnostic that front-loads the riskiest unknowns of the stack: GPU
// passthrough, dynamic shapes for the tile output, explicit model control, and
// digest-verified loading. It proves the path works; it produces no evidence and
// no run bundle.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/modelrepo"
	"github.com/matthewhoung/batch2go/internal/triton"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "smoke: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	endpoint := flag.String("endpoint", "127.0.0.1:8001", "Triton gRPC endpoint")
	catalogPath := flag.String("catalog", "artifacts/catalog.json", "artifact catalog manifest")
	artifactID := flag.String("artifact-id", "", "artifact whose entry to exercise")
	entry := flag.String("entry", "unbatched", "model entry kind")
	timeout := flag.Duration("timeout", 90*time.Second, "readiness timeout")
	flag.Parse()

	if *artifactID == "" {
		return fmt.Errorf("smoke needs --artifact-id")
	}

	catalog, err := modelrepo.LoadCatalog(*catalogPath)
	if err != nil {
		return err
	}
	artifact, err := catalog.Artifact(*artifactID)
	if err != nil {
		return err
	}
	if !artifact.SelfAttesting() {
		return fmt.Errorf("artifact %s does not attest membership (%s); the smoke would prove nothing",
			artifact.ArtifactID, artifact.MembershipEvidence)
	}
	modelName := modelrepo.EntryName(*artifactID, modelrepo.EntryKind(*entry))

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+30*time.Second)
	defer cancel()

	gw, err := triton.Dial(triton.DefaultConfig(*endpoint))
	if err != nil {
		return err
	}
	defer gw.Close()

	if err := gw.WaitLive(ctx, *timeout); err != nil {
		return err
	}
	name, version, err := gw.ServerMetadata(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("server:    %s %s at %s\n", name, version, *endpoint)

	// Explicit model control: quiesce to nothing loaded, then load exactly the
	// entry named. A model left ready from a previous run is a wrong-model risk.
	if err := gw.Quiesce(ctx); err != nil {
		return err
	}
	if err := gw.LoadModel(ctx, modelName); err != nil {
		return err
	}
	fmt.Printf("loaded:    %s (artifact %s)\n", modelName, artifact.Digest)

	id, err := gw.ModelIdentity(ctx, modelName)
	if err != nil {
		return err
	}
	if err := checkEntry(id, modelrepo.EntryKind(*entry)); err != nil {
		return err
	}
	fmt.Printf("config:    backend=%s max_batch_size=%d dynamic_batching=%v instances=%d %s\n",
		id.Backend, id.MaxBatchSize, id.DynamicBatchingEnabled, id.InstanceCount, id.InstanceKind)

	before, err := gw.Statistics(ctx, modelName)
	if err != nil {
		return err
	}

	submitter, err := triton.NewSubmitter(gw, artifact.Graph.FeatureWidth, artifact.Graph.PayloadFloats)
	if err != nil {
		return err
	}
	member := identity.LogicalRequest{Cohort: 1, Ordinal: 0}

	start := time.Now()
	result, err := submitter.Submit(ctx, modelName, []identity.LogicalRequest{member})
	if err != nil {
		return err
	}
	elapsed := time.Since(start)

	after, err := gw.Statistics(ctx, modelName)
	if err != nil {
		return err
	}
	delta, err := triton.Delta(before, after)
	if err != nil {
		return err
	}

	fmt.Printf("request:   %s uid=%d payload=%d bytes round-trip=%s\n",
		member, member.UID(), submitter.LogicalBytes(), elapsed.Round(time.Microsecond))
	fmt.Printf("response:  data_out=%d bytes batch_size=%d\n", result.DataOutBytes, result.BatchSize)

	if err := checkAttestation(result, member); err != nil {
		return err
	}
	fmt.Printf("attested:  membership=%v — verified as this execution's complete uid set\n", result.Membership)

	fmt.Printf("statistics: inference_count +%d execution_count +%d batch_sizes %v\n",
		delta.InferenceCount, delta.ExecutionCount, formatHistogram(delta.BatchSizes))
	if delta.ExecutionCount != 1 {
		return fmt.Errorf("one request produced %d executions; the unbatched entry must run exactly one",
			delta.ExecutionCount)
	}
	fmt.Println("smoke: ok")
	return nil
}

// checkEntry refuses a served configuration that does not match the entry the
// caller asked for. max_batch_size alone is never accepted as evidence of V=off
// (M1 §2.1), so the dynamic-batching stanza is checked separately.
func checkEntry(id triton.ModelIdentity, kind modelrepo.EntryKind) error {
	if id.InstanceCount != 1 {
		return fmt.Errorf("model %s runs %d instances; every entry runs exactly one", id.Name, id.InstanceCount)
	}
	for _, want := range []string{triton.InputData, triton.InputPadding, triton.InputUID} {
		if !contains(id.Inputs, want) {
			return fmt.Errorf("model %s has no %q input; it is not a batch2go synthetic model", id.Name, want)
		}
	}
	if !contains(id.Outputs, triton.OutputUIDSet) {
		return fmt.Errorf("model %s has no %q output; membership could not be attested", id.Name, triton.OutputUIDSet)
	}
	if kind == modelrepo.EntryUnbatched {
		if id.MaxBatchSize != 0 {
			return fmt.Errorf("unbatched entry %s serves max_batch_size=%d, want 0", id.Name, id.MaxBatchSize)
		}
		if id.DynamicBatchingEnabled {
			return fmt.Errorf("unbatched entry %s has dynamic batching enabled", id.Name)
		}
	}
	return nil
}

// checkAttestation verifies the response carries this execution's membership.
// At the unbatched entry the execution has exactly one member, so the attested
// set must be exactly the submitted uid — anything else means the scheduler
// coalesced, or the model is not attesting at all.
func checkAttestation(result triton.Result, member identity.LogicalRequest) error {
	if len(result.Membership) != 1 {
		return fmt.Errorf("execution attested %d members (%v); an unbatched execution has exactly one",
			len(result.Membership), result.Membership)
	}
	if got := result.Membership[0]; got != member.UID() {
		return fmt.Errorf("execution attested uid %d (%v), submitted %d (%v)",
			got, got.LogicalRequest(), member.UID(), member)
	}
	return nil
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func formatHistogram(h map[uint64]uint64) string {
	sizes := make([]uint64, 0, len(h))
	for size := range h {
		sizes = append(sizes, size)
	}
	sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })

	out := "{"
	for i, size := range sizes {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%d:%d", size, h[size])
	}
	return out + "}"
}
