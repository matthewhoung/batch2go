package runner

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/events/clockdomain"
	eventsparquet "github.com/matthewhoung/batch2go/internal/events/parquet"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/manifest"
	"github.com/matthewhoung/batch2go/internal/modelrepo"
	"github.com/matthewhoung/batch2go/internal/triton"
	"github.com/matthewhoung/batch2go/internal/validate"
	"github.com/matthewhoung/batch2go/internal/workload"
)

// Options tune how a run is executed without changing what is measured.
// Anything that changes what is measured belongs in the manifest.
type Options struct {
	// Logf receives operator progress lines. It is not evidence.
	Logf func(format string, args ...any)

	// ImageDigest is the pinned container digest, recorded in the bundle so the
	// server that produced a result is identifiable.
	ImageDigest string

	// BinDir holds the data-plane binaries the shared path runs as separate
	// processes. Defaults to DefaultBinDir.
	BinDir string
}

// Run executes a manifest and writes its bundle.
//
// The order is the model lifecycle of ARCHITECTURE §9: quiesce, verify nothing
// unexpected is ready, materialize and digest-verify, load the exact entry,
// verify what is actually being served, warm up, snapshot, execute, snapshot,
// finalize. Every step that could silently substitute something is a check.
func Run(ctx context.Context, m *manifest.Manifest, opts Options) (*Bundle, error) {
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	if err := checkCellImplemented(m.Cell); err != nil {
		return nil, err
	}

	clock, err := clockdomain.Establish()
	if err != nil {
		return nil, err
	}
	opts.Logf("clock domain %s (%s, resolution %dns, read %dns)",
		clock.ID, clock.Source, clock.ResolutionNanos, clock.ReadOverheadNanos)

	gc := PinGC(m.GC.GOGC, m.GC.GOMEMLIMIT, time.Duration(m.GC.SampleMillis)*time.Millisecond)

	entry, artifact, err := prepareModel(m)
	if err != nil {
		return nil, err
	}

	gw, err := triton.Dial(transportConfig(m))
	if err != nil {
		return nil, err
	}
	defer gw.Close()

	modelIdentity, server, err := prepareServer(ctx, gw, m, entry)
	if err != nil {
		return nil, err
	}
	server.ImageDigest = opts.ImageDigest
	opts.Logf("serving %s (artifact %s, config %s)", entry.Name, entry.ArtifactDigest, entry.ConfigDigest)

	layout, err := NewLayout(Dir(m))
	if err != nil {
		return nil, err
	}

	bundle := &Bundle{
		Experiment:         m.Experiment,
		Session:            m.Session,
		Run:                m.Run,
		Cell:               m.Cell,
		Manifest:           m,
		ClockDomain:        clock,
		Server:             server,
		ModelEntry:         entry,
		ModelGraph:         artifact.Graph,
		ModelIdentity:      modelIdentity,
		Transport:          gw.Config(),
		StartedAtWall:      wallClockStamp(),
		StartedAtMonotonic: clock.Now(),
	}

	if err := execute(ctx, m, opts, clock, gw, entry, layout, gc, bundle); err != nil {
		bundle.State = StateFailed
		bundle.Failure = err.Error()
		bundle.FinishedAtMonotonic = clock.Now()
		// A failed run still writes its bundle: the evidence of a failure is
		// evidence, and discarding it would hide exactly what needs looking at.
		if writeErr := WriteBundle(layout, bundle); writeErr != nil {
			return bundle, fmt.Errorf("%w (and the bundle could not be written: %v)", err, writeErr)
		}
		return bundle, err
	}

	bundle.State = StateCompleted
	bundle.FinishedAtMonotonic = clock.Now()
	if err := WriteBundle(layout, bundle); err != nil {
		return bundle, err
	}
	opts.Logf("bundle written to %s", layout.Root)

	// The runner's last act is to hand the bundle to the validator and record its
	// verdict (ARCHITECTURE §7.1). Judging the archive rather than in-memory state
	// is deliberate: it exercises the same path an offline re-run takes, so a
	// bundle that validates here validates for anyone who reads it later.
	verdict, err := finalizeVerdict(layout, opts)
	if err != nil {
		return bundle, err
	}
	if !verdict.Passed {
		bundle.State = StateFailed
		bundle.Failure = describeRejection(verdict)
		if err := WriteBundle(layout, bundle); err != nil {
			return bundle, err
		}
		return bundle, fmt.Errorf("runner: %s", bundle.Failure)
	}
	return bundle, nil
}

// describeRejection summarizes why the validator refused a run. A check can fail
// without producing a defect — a topology or chain lookup that could not resolve
// the cell fails the check itself — so the defect list is not assumed non-empty.
func describeRejection(v validate.Verdict) string {
	if defects := v.Defects(); len(defects) > 0 {
		return fmt.Sprintf("validator rejected the run: %d defects, first: %v", len(defects), defects[0])
	}
	for _, c := range v.Checks {
		if !c.Passed {
			return fmt.Sprintf("validator rejected the run: check %q failed (%s)", c.Name, c.Detail)
		}
	}
	return "validator rejected the run"
}

// finalizeVerdict validates the written bundle and stores the verdict with it.
func finalizeVerdict(layout Layout, opts Options) (validate.Verdict, error) {
	bundle, verdict, err := ValidateBundle(layout.Root)
	if err != nil {
		return verdict, err
	}
	if err := WriteVerdict(layout.Root, verdict); err != nil {
		return verdict, err
	}

	// The raw streams are kept until the bundle validates, so they can be judged
	// too. If the archive and the streams disagree, the conversion changed the
	// evidence and neither verdict can be trusted.
	streamVerdict, err := ValidateStreams(layout.Root, bundle)
	if err != nil {
		return verdict, err
	}
	if streamVerdict.Passed != verdict.Passed {
		return verdict, fmt.Errorf(
			"runner: the archive and the raw record streams disagree (archive passed=%v, streams passed=%v); the conversion altered the evidence",
			verdict.Passed, streamVerdict.Passed)
	}

	opts.Logf("verdict: passed=%v, max |residual| %.4f%% of path (tolerance %.2f%%)",
		verdict.Passed,
		verdict.Conservation.MaxAbsResidualFraction*100,
		verdict.Conservation.ToleranceFraction*100)
	for _, c := range verdict.Checks {
		opts.Logf("  %-18s %v  %s", c.Name, c.Passed, c.Detail)
	}
	return verdict, nil
}

func transportConfig(m *manifest.Manifest) triton.Config {
	return triton.Config{
		Endpoint:              m.Transport.TritonEndpoint,
		MaxMessageBytes:       m.Transport.MaxMessageBytes,
		InitialWindowSize:     m.Transport.InitialWindowSize,
		InitialConnWindowSize: m.Transport.InitialConnWindowSize,
		DialTimeout:           30 * time.Second,
	}
}

// prepareModel verifies the artifact against the manifest and the catalog, then
// materializes the runtime repository.
func prepareModel(m *manifest.Manifest) (modelrepo.Entry, modelrepo.Artifact, error) {
	catalog, err := modelrepo.LoadCatalog(m.Model.Catalog)
	if err != nil {
		return modelrepo.Entry{}, modelrepo.Artifact{}, err
	}
	artifact, err := catalog.Artifact(m.Model.ArtifactID)
	if err != nil {
		return modelrepo.Entry{}, modelrepo.Artifact{}, err
	}
	if m.Model.ExpectedDigest != "" && artifact.Digest != m.Model.ExpectedDigest {
		return modelrepo.Entry{}, artifact, fmt.Errorf(
			"runner: manifest names artifact digest %s but the catalog holds %s; the model was regenerated",
			m.Model.ExpectedDigest, artifact.Digest)
	}
	if !artifact.SelfAttesting() {
		return modelrepo.Entry{}, artifact, fmt.Errorf(
			"runner: artifact %s reports membership as %q; a run needs self-attesting evidence (ADR-0007)",
			artifact.ArtifactID, artifact.MembershipEvidence)
	}

	repo, err := modelrepo.Materialize(modelrepo.Request{
		Catalog:      catalog,
		ArtifactID:   m.Model.ArtifactID,
		ArtifactDir:  m.Model.ArtifactDir,
		RuntimeDir:   m.Model.RuntimeDir,
		Kinds:        []modelrepo.EntryKind{m.Model.Entry},
		MaxBatchSize: m.Cohort.Size,
	})
	if err != nil {
		return modelrepo.Entry{}, artifact, err
	}
	entry, err := repo.Entry(m.Model.Entry)
	return entry, artifact, err
}

// prepareServer walks the model lifecycle and verifies what is actually served.
func prepareServer(ctx context.Context, gw *triton.Gateway, m *manifest.Manifest, entry modelrepo.Entry) (triton.ModelIdentity, ServerRecord, error) {
	var id triton.ModelIdentity
	var rec ServerRecord

	if err := gw.WaitLive(ctx, 90*time.Second); err != nil {
		return id, rec, err
	}
	name, version, err := gw.ServerMetadata(ctx)
	if err != nil {
		return id, rec, err
	}
	rec = ServerRecord{Name: name, Version: version, Endpoint: m.Transport.TritonEndpoint}

	if err := gw.Quiesce(ctx); err != nil {
		return id, rec, err
	}
	if err := gw.LoadModel(ctx, entry.Name); err != nil {
		return id, rec, err
	}
	id, err = gw.ModelIdentity(ctx, entry.Name)
	if err != nil {
		return id, rec, err
	}
	if err := verifyServedEntry(id, entry, m); err != nil {
		return id, rec, err
	}
	return id, rec, nil
}

// verifyServedEntry refuses a served configuration that differs from what the
// manifest declared. A run whose model quietly differs from its manifest reports
// results about a condition nobody specified.
func verifyServedEntry(id triton.ModelIdentity, entry modelrepo.Entry, m *manifest.Manifest) error {
	if id.Name != entry.Name {
		return fmt.Errorf("runner: server is serving %q, manifest names %q", id.Name, entry.Name)
	}
	if id.InstanceCount != 1 {
		return fmt.Errorf("runner: %s runs %d instances; overlapping executions would dissolve the queue accounting",
			id.Name, id.InstanceCount)
	}
	for _, want := range []string{triton.InputData, triton.InputPadding, triton.InputUID} {
		if !containsString(id.Inputs, want) {
			return fmt.Errorf("runner: %s has no %q input; payload would not traverse every hop", id.Name, want)
		}
	}
	if !containsString(id.Outputs, triton.OutputUIDSet) {
		return fmt.Errorf("runner: %s has no %q output; membership could not be attested", id.Name, triton.OutputUIDSet)
	}
	if m.Model.Entry == modelrepo.EntryUnbatched {
		if id.MaxBatchSize != 0 {
			return fmt.Errorf("runner: unbatched entry %s serves max_batch_size=%d, want 0", id.Name, id.MaxBatchSize)
		}
		if id.DynamicBatchingEnabled {
			return fmt.Errorf("runner: unbatched entry %s has dynamic batching enabled; V=off would not hold", id.Name)
		}
	}
	return nil
}

// execute runs the workload and finalizes the evidence.
func execute(
	ctx context.Context,
	m *manifest.Manifest,
	opts Options,
	clock *clockdomain.Domain,
	gw *triton.Gateway,
	entry modelrepo.Entry,
	layout Layout,
	gc *GCRecorder,
	bundle *Bundle,
) error {
	graph := graphShape{
		FeatureWidth:  bundle.ModelGraph.FeatureWidth,
		PayloadFloats: bundle.ModelGraph.PayloadFloats,
	}

	// The shared path's proxy and adapter are separate processes; they must be up
	// before the first request and stopped before the streams are read, because
	// each flushes its event stream on shutdown.
	svc, err := startServices(ctx, m, clock, layout, entry, bundle.ModelGraph, opts)
	if err != nil {
		return err
	}
	defer svc.Stop()

	client, err := cellPath(m, gw, entry.Name, graph)
	if err != nil {
		return err
	}
	defer client.Close()

	header := events.RunHeader{
		Experiment:  m.Experiment,
		Session:     m.Session,
		Run:         m.Run,
		Cell:        m.Cell,
		ClockDomain: clock.ID,
	}

	loadgenHeader := header
	loadgenHeader.WriterID = 1
	loadgen, err := events.NewFileWriter(layout.StreamPath(identity.EmitterLoadGen), loadgenHeader)
	if err != nil {
		return err
	}
	defer loadgen.Close()

	tritonHeader := header
	// Writer ids follow the emitters: loadgen 1, proxy 2, adapter 3, triton 4.
	// (writer_id, seq) is the per-writer unique key, so reusing the proxy's id
	// here would collide with its rows in a shared-path archive.
	tritonHeader.WriterID = 4
	tritonStream, err := events.NewFileWriter(layout.StreamPath(identity.EmitterTriton), tritonHeader)
	if err != nil {
		return err
	}
	defer tritonStream.Close()

	// Warm-up traverses the same path and produces no evidence. Its cohort ids
	// come before the recorded ones so the two are separable in the trace stream.
	for i := 0; i < m.Cohort.WarmupCount; i++ {
		cohort := workload.NewCohort(identity.CohortID(i), m.Cohort.Size, true)
		if _, err := releaseCohort(ctx, m, clock, client, &cohort, nil); err != nil {
			return fmt.Errorf("runner: warm-up cohort %d: %w", i, err)
		}
	}
	opts.Logf("warm-up complete: %d cohorts", m.Cohort.WarmupCount)

	expectedTraces := m.Cohort.Count * m.Cohort.Size
	collector, err := triton.NewTraceCollector(ctx, gw, m.Tracing.TraceDir, expectedTraces)
	if err != nil {
		return err
	}
	before, err := gw.Statistics(ctx, entry.Name)
	if err != nil {
		return err
	}
	gc.Start()

	results := make(map[identity.LogicalRequest]memberOutcome, m.Cohort.Count*m.Cohort.Size)
	for i := 0; i < m.Cohort.Count; i++ {
		cohort := workload.NewCohort(identity.CohortID(m.Cohort.WarmupCount+i), m.Cohort.Size, false)
		outcomes, err := releaseCohort(ctx, m, clock, client, &cohort, loadgen)
		if err != nil {
			return err
		}
		for req, out := range outcomes {
			results[req] = out
		}
		bundle.Schedule = append(bundle.Schedule, cohort)

		if gap := m.InterCohortGap(); gap > 0 && i < m.Cohort.Count-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(gap):
			}
		}
	}
	opts.Logf("released %d cohorts of %d", m.Cohort.Count, m.Cohort.Size)

	bundle.GC = gc.Stop()

	// Stop the data plane before reading anything: the proxy and adapter flush
	// and close their streams on shutdown, and archiving first would capture a
	// partial stream and report evidence missing that was in fact recorded.
	if err := client.Close(); err != nil {
		return err
	}
	if err := svc.Stop(); err != nil {
		return err
	}

	after, err := gw.Statistics(ctx, entry.Name)
	if err != nil {
		return err
	}
	delta, err := triton.Delta(before, after)
	if err != nil {
		return err
	}
	bundle.TritonStats = delta

	released := make([]identity.LogicalRequest, 0, len(results))
	for req := range results {
		released = append(released, req)
	}
	traceEvents, traceFiles, err := collector.Collect(ctx, released)
	if err != nil {
		return err
	}
	if err := recordBackendTimestamps(traceEvents, results, tritonStream); err != nil {
		return err
	}
	opts.Logf("collected %d backend traces from %d files", len(traceEvents), len(traceFiles))

	for _, src := range traceFiles {
		dst := filepath.Join(layout.Traces, filepath.Base(src))
		if err := copyFile(dst, src); err != nil {
			return err
		}
		bundle.Files.Traces = append(bundle.Files.Traces, filepath.Join("traces", filepath.Base(src)))
	}

	// Close the streams before archiving: the archive must contain every record,
	// including whatever is still sitting in a buffer bank.
	if err := loadgen.Close(); err != nil {
		return err
	}
	if err := tritonStream.Close(); err != nil {
		return err
	}
	bundle.Streams = []StreamRecord{
		streamRecord(identity.EmitterLoadGen, loadgen.Stats()),
		streamRecord(identity.EmitterTriton, tritonStream.Stats()),
	}
	if m.Cell.UsesProxy() {
		// The proxy and adapter wrote their own streams from their own processes;
		// their counters live in the stream files rather than in this process.
		for _, emitter := range []identity.Emitter{identity.EmitterProxy, identity.EmitterAdapter} {
			rec, err := externalStreamRecord(layout, emitter)
			if err != nil {
				return err
			}
			bundle.Streams = append(bundle.Streams, rec)
		}
	}
	bundle.Files.Manifest = "manifest.json"
	for _, stream := range bundle.Streams {
		bundle.Files.EventStreams = append(bundle.Files.EventStreams, stream.File)
	}
	bundle.FirstRecordedCohort = identity.CohortID(m.Cohort.WarmupCount)

	if err := archive(layout, bundle); err != nil {
		return err
	}
	opts.Logf("archived %s", layout.Archive)
	return nil
}

func streamRecord(emitter identity.Emitter, stats events.Stats) StreamRecord {
	return StreamRecord{
		Emitter: emitter.String(),
		File:    filepath.Join("events", emitter.String()+".b2g"),
		Written: stats.Written,
		Dropped: stats.Dropped,
	}
}

// memberOutcome is what one logical request produced on the client side.
type memberOutcome struct {
	Result pathResult
	Status events.Status
	Seq    uint64
}

// releaseCohort mints a cohort's labels, releases its members through the
// barrier, and records the client-side stages. Passing a nil writer runs the
// cohort without producing evidence, which is what warm-up needs.
func releaseCohort(
	ctx context.Context,
	m *manifest.Manifest,
	clock *clockdomain.Domain,
	client path,
	cohort *workload.Cohort,
	writer *events.Writer,
) (map[identity.LogicalRequest]memberOutcome, error) {
	barrier, err := workload.NewBarrier(len(cohort.Members), clock.Now)
	if err != nil {
		return nil, err
	}

	cohort.ScheduledAt = clock.Now()
	outcomes := make(map[identity.LogicalRequest]memberOutcome, len(cohort.Members))

	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup

	for _, member := range cohort.Members {
		wg.Add(1)
		go func(member identity.LogicalRequest) {
			defer wg.Done()

			var rec events.Record
			rec.Emitter = identity.EmitterLoadGen
			rec.Cohort = member.Cohort
			rec.Ordinal = member.Ordinal
			rec.LogicalBytes = uint32(client.LogicalBytes())
			rec.SetStage(events.StageSched, cohort.ScheduledAt)

			// The barrier is the load generator's synchronization point, and at A=off
			// the only one anywhere. Every member leaves at the same instant, and at
			// A=off that instant is the cohort seal, owned here (ADR-0001). At A=on
			// the proxy owns the seal and this stage is not written here.
			seal := barrier.Arrive()
			rec.SetStage(events.StageCohortSeal, seal)

			reqCtx, cancel := context.WithTimeout(ctx, m.RequestTimeout())
			defer cancel()

			rec.SetStage(events.StageClientSend, clock.Now())
			result, err := client.Send(reqCtx, member, seal)
			rec.SetStage(events.StageClientRecv, clock.Now())

			status := events.StatusOK
			if err != nil {
				status = events.StatusError
				if reqCtx.Err() == context.DeadlineExceeded {
					status = events.StatusTimeout
				}
			}
			rec.Status = status
			if err == nil {
				rec.BatchSize = uint32(result.BatchSize)
				rec.SetMembership(result.Membership)
			}

			var seq uint64
			if writer != nil {
				var ok bool
				seq, ok = writer.Record(&rec)
				if !ok {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("runner: event record for %v was dropped; the run's evidence is incomplete", member)
					}
					mu.Unlock()
				}
			}

			mu.Lock()
			outcomes[member] = memberOutcome{Result: result, Status: status, Seq: seq}
			// A member that failed does not vanish from the record — it is recorded
			// with its status — but it does fail the run.
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("runner: request %v failed: %w", member, err)
			}
			mu.Unlock()
		}(member)
	}

	wg.Wait()
	cohort.SealedAt, _ = barrier.Released()
	if firstErr != nil {
		return outcomes, firstErr
	}
	return outcomes, nil
}

// recordBackendTimestamps writes the Triton-side event records, joined to
// logical requests by the request id the client set.
//
// Completeness is asserted rather than hoped for: every recorded request must
// have a trace carrying all three backend timestamps. A missing one is a missing
// timestamp — a validation failure — and the run must say so now rather than
// leave a hole for the analysis to trip over.
func recordBackendTimestamps(
	traces []triton.TraceEvent,
	results map[identity.LogicalRequest]memberOutcome,
	writer *events.Writer,
) error {
	byRequest := make(map[identity.LogicalRequest]triton.TraceEvent, len(traces))
	for _, tr := range traces {
		if _, recorded := results[tr.Request]; !recorded {
			continue // warm-up, or another run sharing the trace directory
		}
		byRequest[tr.Request] = tr
	}

	var missing []identity.LogicalRequest
	for req := range results {
		tr, ok := byRequest[req]
		if !ok || !tr.Complete() {
			missing = append(missing, req)
			continue
		}

		var rec events.Record
		rec.Emitter = identity.EmitterTriton
		rec.Cohort = req.Cohort
		rec.Ordinal = req.Ordinal
		rec.ExecutionID = identity.ExecutionID(tr.TraceID)
		rec.Status = results[req].Status
		rec.SetStage(events.StageQueueStart, tr.QueueStart)
		rec.SetStage(events.StageComputeStart, tr.ComputeStart)
		rec.SetStage(events.StageComputeEnd, tr.ComputeEnd)
		// No membership here: a trace carries timing, not attestation. Membership
		// is recorded by every process that actually received the model's uid set —
		// the load generator on both paths, and the adapter as well on the shared
		// one, where the two are independent observations the validator compares.

		if _, ok := writer.Record(&rec); !ok {
			return fmt.Errorf("runner: backend event record for %v was dropped", req)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"runner: %d of %d requests have no complete backend trace (first: %v); the schema's backend timestamps are missing",
			len(missing), len(results), missing[0])
	}
	return nil
}

// archive converts the binary streams into the Parquet archive analysis reads.
// The raw streams are kept alongside it until the bundle validates (ADR-0005).
func archive(layout Layout, bundle *Bundle) error {
	var all []events.Decoded
	for _, stream := range bundle.Streams {
		records, err := events.ReadFile(filepath.Join(layout.Root, stream.File))
		if err != nil {
			return err
		}
		all = append(all, records...)
	}
	// Warm-up traverses the same path and produces no evidence, so its records do
	// not enter the archive. They stay in the raw streams, which are kept beside
	// the archive until the bundle validates (ADR-0005).
	all = RecordedOnly(all, bundle.FirstRecordedCohort)
	if err := eventsparquet.Write(layout.Archive, all); err != nil {
		return err
	}
	bundle.Files.Archive = "events.parquet"
	return nil
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
