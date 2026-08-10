// Package triton is the gateway to the inference server: connection, readiness
// and metadata checks, model lifecycle under explicit model control, request
// building, response validation, and raw statistics snapshots.
//
// Its rules are negative ones. It never conflates a model execution with a
// kernel launch, never silently changes model variant or batching behavior, and
// never draws a conclusion — it returns evidence and lets the validator judge it
// offline (ARCHITECTURE §6.5).
package triton

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	tritonv2 "github.com/matthewhoung/batch2go/api/triton/v2"
)

// MaxMessageBytes is the gRPC message ceiling, fixed at 256 MiB on every hop
// including this client. It is a manifest constant, not a tuning knob: envelope
// packing cost is a declared constituent of the aggregation effect, so transport
// limits are part of the treatment definition (ADR-0003).
const MaxMessageBytes = 256 << 20

// Config is the gateway's transport configuration. Every field is recorded in
// the run bundle so a bundle describes the transport it was measured on.
type Config struct {
	Endpoint string `json:"endpoint"`

	MaxMessageBytes       int   `json:"max_message_bytes"`
	InitialWindowSize     int32 `json:"initial_window_size"`
	InitialConnWindowSize int32 `json:"initial_conn_window_size"`

	DialTimeout time.Duration `json:"dial_timeout"`
}

// DefaultConfig returns the pinned transport settings.
func DefaultConfig(endpoint string) Config {
	return Config{
		Endpoint:              endpoint,
		MaxMessageBytes:       MaxMessageBytes,
		InitialWindowSize:     4 << 20,
		InitialConnWindowSize: 16 << 20,
		DialTimeout:           30 * time.Second,
	}
}

// Gateway is one gRPC client to one Triton server. All singles-submitting cells
// share one gateway — identical request construction over one channel, differing
// only in target entry — so a measured path difference is a path difference and
// not two client implementations (ARCHITECTURE §6.4).
type Gateway struct {
	cfg    Config
	conn   *grpc.ClientConn
	client tritonv2.GRPCInferenceServiceClient
}

// Dial connects to Triton.
func Dial(cfg Config) (*Gateway, error) {
	if cfg.MaxMessageBytes == 0 {
		cfg.MaxMessageBytes = MaxMessageBytes
	}
	conn, err := grpc.NewClient(cfg.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cfg.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(cfg.MaxMessageBytes),
		),
		grpc.WithInitialWindowSize(cfg.InitialWindowSize),
		grpc.WithInitialConnWindowSize(cfg.InitialConnWindowSize),
	)
	if err != nil {
		return nil, fmt.Errorf("triton: dial %s: %w", cfg.Endpoint, err)
	}
	return &Gateway{cfg: cfg, conn: conn, client: tritonv2.NewGRPCInferenceServiceClient(conn)}, nil
}

// Config returns the transport configuration in force.
func (g *Gateway) Config() Config { return g.cfg }

// Close releases the connection.
func (g *Gateway) Close() error { return g.conn.Close() }

// WaitLive blocks until the server is serving its API, or the deadline passes.
//
// Liveness, not readiness, is the right gate here. Triton reports itself
// not-ready while any model in its repository is in a failed state, so a model
// that failed to load in an earlier session would block a fresh one forever —
// and clearing exactly that is what the lifecycle's quiesce step is for
// (ARCHITECTURE §9). Readiness is asserted after quiescing, by Quiesce.
func (g *Gateway) WaitLive(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		live, err := g.client.ServerLive(ctx, &tritonv2.ServerLiveRequest{})
		switch {
		case err != nil:
			lastErr = err
		case live.GetLive():
			return nil
		default:
			lastErr = fmt.Errorf("server reports not live")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("triton: server at %s not live within %s (last: %v)", g.cfg.Endpoint, timeout, lastErr)
}

// QuiesceTimeout bounds how long the lifecycle waits for the server to settle
// after unloading.
const QuiesceTimeout = 60 * time.Second

// Quiesce unloads every model and waits until the server reports ready with
// nothing loaded.
//
// This is the lifecycle's opening step, and it is a check rather than a
// convenience: a model left ready — or left failed — from earlier work is how a
// run ends up describing a model its manifest never named.
//
// The wait is a poll because Triton's unload returns before the model is fully
// torn down, so readiness sampled once immediately afterwards catches a
// transient not-ready window that has nothing to do with the state being waited
// for.
func (g *Gateway) Quiesce(ctx context.Context) error {
	if err := g.UnloadAll(ctx); err != nil {
		return err
	}

	deadline := time.Now().Add(QuiesceTimeout)
	var last string
	for {
		ready, err := g.client.ServerReady(ctx, &tritonv2.ServerReadyRequest{})
		if err != nil {
			return fmt.Errorf("triton: server readiness: %w", err)
		}
		loaded, err := g.ReadyModels(ctx)
		if err != nil {
			return err
		}
		switch {
		case !ready.GetReady():
			last = "server reports not ready"
		case len(loaded) != 0:
			last = fmt.Sprintf("models still ready: %v", loaded)
		default:
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("triton: server at %s did not quiesce within %s (%s)", g.cfg.Endpoint, QuiesceTimeout, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ServerMetadata reports the server's identity, which belongs in the session
// record alongside the container digest.
func (g *Gateway) ServerMetadata(ctx context.Context) (name, version string, err error) {
	md, err := g.client.ServerMetadata(ctx, &tritonv2.ServerMetadataRequest{})
	if err != nil {
		return "", "", fmt.Errorf("triton: server metadata: %w", err)
	}
	return md.GetName(), md.GetVersion(), nil
}

// LoadModel loads a model under explicit model control. Model switching is the
// runner calling this API, and every load is evidenced (ARCHITECTURE §9).
func (g *Gateway) LoadModel(ctx context.Context, name string) error {
	if _, err := g.client.RepositoryModelLoad(ctx, &tritonv2.RepositoryModelLoadRequest{ModelName: name}); err != nil {
		return fmt.Errorf("triton: load model %s: %w", name, err)
	}
	return nil
}

// UnloadModel unloads a model.
func (g *Gateway) UnloadModel(ctx context.Context, name string) error {
	if _, err := g.client.RepositoryModelUnload(ctx, &tritonv2.RepositoryModelUnloadRequest{ModelName: name}); err != nil {
		return fmt.Errorf("triton: unload model %s: %w", name, err)
	}
	return nil
}

// UnloadAll unloads every model in the repository, whatever state it is in.
//
// The index is deliberately unfiltered. Filtering to ready models would leave
// behind a model that failed to load earlier — and Triton reports itself
// not-ready while any model sits in a failed state, so the leftover would block
// every later session while being invisible to a ready-only sweep.
func (g *Gateway) UnloadAll(ctx context.Context) error {
	index, err := g.client.RepositoryIndex(ctx, &tritonv2.RepositoryIndexRequest{})
	if err != nil {
		return fmt.Errorf("triton: repository index: %w", err)
	}
	for _, m := range index.GetModels() {
		if err := g.UnloadModel(ctx, m.GetName()); err != nil {
			return fmt.Errorf("triton: unload %s (state %s): %w", m.GetName(), m.GetState(), err)
		}
	}
	return nil
}

// ReadyModels lists the models the server reports as ready.
func (g *Gateway) ReadyModels(ctx context.Context) ([]string, error) {
	index, err := g.client.RepositoryIndex(ctx, &tritonv2.RepositoryIndexRequest{Ready: true})
	if err != nil {
		return nil, fmt.Errorf("triton: repository index: %w", err)
	}
	names := make([]string, 0, len(index.GetModels()))
	for _, m := range index.GetModels() {
		names = append(names, m.GetName())
	}
	return names, nil
}

// ModelIdentity is what the server says it is actually serving. The runner
// checks it against the manifest before any request is sent.
type ModelIdentity struct {
	Name          string   `json:"name"`
	Versions      []string `json:"versions"`
	Platform      string   `json:"platform"`
	Backend       string   `json:"backend"`
	MaxBatchSize  int      `json:"max_batch_size"`
	InstanceCount int      `json:"instance_count"`
	InstanceKind  string   `json:"instance_kind"`

	// DynamicBatchingEnabled is read from the served configuration rather than
	// assumed from the entry's name: max_batch_size alone is never accepted as
	// evidence of V=off (M1 §2.1).
	DynamicBatchingEnabled bool     `json:"dynamic_batching_enabled"`
	Inputs                 []string `json:"inputs"`
	Outputs                []string `json:"outputs"`
}

// ModelIdentity fetches the metadata and configuration of a loaded model.
func (g *Gateway) ModelIdentity(ctx context.Context, name string) (ModelIdentity, error) {
	var id ModelIdentity
	md, err := g.client.ModelMetadata(ctx, &tritonv2.ModelMetadataRequest{Name: name})
	if err != nil {
		return id, fmt.Errorf("triton: model metadata %s: %w", name, err)
	}
	cfgResp, err := g.client.ModelConfig(ctx, &tritonv2.ModelConfigRequest{Name: name})
	if err != nil {
		return id, fmt.Errorf("triton: model config %s: %w", name, err)
	}
	cfg := cfgResp.GetConfig()

	id = ModelIdentity{
		Name:                   md.GetName(),
		Versions:               md.GetVersions(),
		Platform:               md.GetPlatform(),
		Backend:                cfg.GetBackend(),
		MaxBatchSize:           int(cfg.GetMaxBatchSize()),
		DynamicBatchingEnabled: cfg.GetDynamicBatching() != nil,
	}
	for _, in := range md.GetInputs() {
		id.Inputs = append(id.Inputs, in.GetName())
	}
	for _, out := range md.GetOutputs() {
		id.Outputs = append(id.Outputs, out.GetName())
	}
	for _, group := range cfg.GetInstanceGroup() {
		id.InstanceCount += int(group.GetCount())
		id.InstanceKind = group.GetKind().String()
	}
	return id, nil
}

// Snapshot is a raw reading of Triton's own counters for one model. Nothing here
// is a conclusion: the deltas and the histogram are the raw material the
// validator's contamination check judges (M2-PLAN §3).
type Snapshot struct {
	Model          string            `json:"model"`
	InferenceCount uint64            `json:"inference_count"`
	ExecutionCount uint64            `json:"execution_count"`
	BatchSizes     map[uint64]uint64 `json:"batch_sizes"`
}

// Statistics reads the server's counters for one model.
func (g *Gateway) Statistics(ctx context.Context, model string) (Snapshot, error) {
	resp, err := g.client.ModelStatistics(ctx, &tritonv2.ModelStatisticsRequest{Name: model})
	if err != nil {
		return Snapshot{}, fmt.Errorf("triton: model statistics %s: %w", model, err)
	}
	snap := Snapshot{Model: model, BatchSizes: map[uint64]uint64{}}
	for _, s := range resp.GetModelStats() {
		if s.GetName() != model {
			continue
		}
		snap.InferenceCount += s.GetInferenceCount()
		snap.ExecutionCount += s.GetExecutionCount()
		for _, b := range s.GetBatchStats() {
			// compute_infer's count is how many executions ran at this batch size;
			// the batch size itself is the histogram bucket.
			snap.BatchSizes[b.GetBatchSize()] += b.GetComputeInfer().GetCount()
		}
	}
	return snap, nil
}

// StatisticsDelta is the change in Triton's counters across a run. It is the
// evidence behind "n executions happened, at these batch sizes".
type StatisticsDelta struct {
	Model          string            `json:"model"`
	InferenceCount uint64            `json:"inference_count"`
	ExecutionCount uint64            `json:"execution_count"`
	BatchSizes     map[uint64]uint64 `json:"batch_sizes"`
}

// Delta subtracts two snapshots of the same model.
func Delta(before, after Snapshot) (StatisticsDelta, error) {
	if before.Model != after.Model {
		return StatisticsDelta{}, fmt.Errorf("triton: cannot subtract snapshots of %s and %s", before.Model, after.Model)
	}
	if after.InferenceCount < before.InferenceCount || after.ExecutionCount < before.ExecutionCount {
		return StatisticsDelta{}, fmt.Errorf(
			"triton: counters for %s went backwards (inference %d->%d, execution %d->%d); the server was restarted mid-run",
			before.Model, before.InferenceCount, after.InferenceCount, before.ExecutionCount, after.ExecutionCount)
	}
	d := StatisticsDelta{
		Model:          before.Model,
		InferenceCount: after.InferenceCount - before.InferenceCount,
		ExecutionCount: after.ExecutionCount - before.ExecutionCount,
		BatchSizes:     map[uint64]uint64{},
	}
	for size, count := range after.BatchSizes {
		if delta := count - before.BatchSizes[size]; delta > 0 {
			d.BatchSizes[size] = delta
		}
	}
	return d, nil
}

// float32SliceToBytes reinterprets a float32 slice as little-endian bytes.
// Triton's raw_input_contents wants exactly this layout, and building it once
// per request keeps serialization single-copy (ADR-0003).
func float32SliceToBytes(dst []byte, src []float32) []byte {
	for _, v := range src {
		dst = binary.LittleEndian.AppendUint32(dst, math.Float32bits(v))
	}
	return dst
}

func int64SliceToBytes(dst []byte, src []int64) []byte {
	for _, v := range src {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(v))
	}
	return dst
}

func bytesToInt64Slice(b []byte) ([]int64, error) {
	if len(b)%8 != 0 {
		return nil, fmt.Errorf("triton: int64 tensor payload of %d bytes is not a whole number of elements", len(b))
	}
	out := make([]int64, len(b)/8)
	for i := range out {
		out[i] = int64(binary.LittleEndian.Uint64(b[i*8:]))
	}
	return out, nil
}
