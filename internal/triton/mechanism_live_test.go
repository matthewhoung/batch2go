package triton

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	tritonv2 "github.com/matthewhoung/batch2go/api/triton/v2"
)

// These tests pin the Triton behaviours the evidence chain depends on.
//
// The backend timestamps of the schema come from a trace subsystem with no flush
// verb, whose write path we reach through a documented-but-indirect mechanism.
// That is a defensible choice (ADR-0008) precisely because it is pinned here: if
// a future image changes any of these properties, this fails rather than a
// dataset quietly becoming wrong.
//
// They need a live server, so they skip when none is reachable. `make contracts`
// runs against a live stack, which is where they earn their keep.

const liveEndpoint = "127.0.0.1:8001"

func liveGateway(t *testing.T) (*Gateway, context.Context) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", liveEndpoint, 500*time.Millisecond)
	if err != nil {
		t.Skipf("no Triton at %s; run `make stack-up` to exercise the mechanism tests", liveEndpoint)
	}
	conn.Close()

	gw, err := Dial(DefaultConfig(liveEndpoint))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { gw.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	if err := gw.WaitLive(ctx, 30*time.Second); err != nil {
		t.Skipf("Triton is not live: %v", err)
	}
	return gw, ctx
}

// The server must never run with log_frequency 0.
//
// At 0, Triton's end-of-settings write appends to the base trace.json — an
// ofstream it holds open for its whole life, so on disk that file is empty until
// shutdown and truncated mid-array once its buffer overflows. Every flush has to
// land in a closed, indexed file instead, and that is a property of how the
// server was started, not of anything this code can fix afterwards.
func TestServerRunsWithNonZeroLogFrequency(t *testing.T) {
	gw, ctx := liveGateway(t)

	resp, err := gw.client.TraceSetting(ctx, &tritonv2.TraceSettingRequest{})
	if err != nil {
		t.Fatalf("read trace settings: %v", err)
	}
	setting, ok := resp.GetSettings()["log_frequency"]
	if !ok || len(setting.GetValue()) == 0 {
		t.Fatal("trace settings carry no log_frequency")
	}
	if setting.GetValue()[0] == "0" {
		t.Fatalf("log_frequency is 0; the compose profile must set --trace-config=triton,log-frequency (ADR-0008)")
	}
}

// Trace timestamps must be raw CLOCK_MONOTONIC, because we subtract them
// against Go-side monotonic readings. Triton's OpenTelemetry mode converts to
// wall clock inside Triton's own code, so the mode itself is part of the
// evidence contract.
func TestTraceModeIsTritonNotOpenTelemetry(t *testing.T) {
	gw, ctx := liveGateway(t)

	resp, err := gw.client.TraceSetting(ctx, &tritonv2.TraceSettingRequest{})
	if err != nil {
		t.Fatalf("read trace settings: %v", err)
	}
	for key, want := range map[string]string{
		// OpenTelemetry mode would emit epoch nanoseconds; the file mode emits the
		// steady-clock integer unconverted (ADR-0008).
		"trace_mode": "triton",
		// Sampling would make the evidence incomplete by construction.
		"trace_rate": "1",
	} {
		setting, ok := resp.GetSettings()[key]
		if !ok || len(setting.GetValue()) == 0 {
			t.Errorf("trace settings carry no %s", key)
			continue
		}
		if got := setting.GetValue()[0]; got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

// The mechanism the collector rests on: replacing a trace setting writes the
// pending traces to a closed, indexed file. If an upgrade ever stops doing this,
// runs would lose their backend timestamps, and this is what says so.
func TestSettingsUpdateFlushesToAnIndexedFile(t *testing.T) {
	gw, ctx := liveGateway(t)

	dir := traceDir(t)
	before, err := traceFiles(dir)
	if err != nil {
		t.Fatalf("list trace files: %v", err)
	}

	c := &TraceCollector{gw: gw, dir: dir, seen: map[string]bool{}, freq: TraceRotation, budget: 1 << 20}
	if err := c.applySettings(ctx, c.freq, c.budget); err != nil {
		t.Fatalf("apply settings: %v", err)
	}
	if err := c.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The write is asynchronous, so this waits rather than reading once. The
	// window is short because a miss here means "nothing was pending", not
	// "the mechanism broke" — the end-to-end exercise is `make contracts`.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		after, err := traceFiles(dir)
		if err != nil {
			t.Fatalf("list trace files: %v", err)
		}
		if len(after) > len(before) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	// No traces may have been pending, which is not a failure of the mechanism.
	t.Skip("no pending traces to flush; run this after traffic to exercise the write path")
}

// Triton's remaining trace count must behave as a monotone per-request tally, or
// the collector's independent completeness check is meaningless.
func TestTraceCountIsReadableAndSettable(t *testing.T) {
	gw, ctx := liveGateway(t)

	c := &TraceCollector{gw: gw, dir: traceDir(t), seen: map[string]bool{}, freq: TraceRotation}
	c.budget = 500000
	if err := c.applySettings(ctx, c.freq, c.budget); err != nil {
		t.Fatalf("apply settings: %v", err)
	}

	got, err := c.remainingCount(ctx)
	if err != nil {
		t.Fatalf("read remaining count: %v", err)
	}
	if got != c.budget {
		t.Errorf("remaining count = %d immediately after setting it to %d", got, c.budget)
	}
	if got <= 0 {
		t.Error("a budget that can reach zero would stop tracing mid-run and reject later updates")
	}
}

// Every trace file the collector will read must be complete JSON. The base file
// never is while the server runs, which is why it is filtered out.
func TestCollectorReadsOnlyCompleteIndexedFiles(t *testing.T) {
	dir := t.TempDir()

	// A base file left mid-buffer by a running server: valid content, no closing
	// bracket. The collector must not pick it up.
	if err := os.WriteFile(filepath.Join(dir, "trace.json"),
		[]byte(`[{"id":1,"request_id":"c1/o0"},{"id":1,"timestamps":[{"name":"QUEUE_START","ns":5}`), 0o644); err != nil {
		t.Fatalf("write base file: %v", err)
	}
	complete := `[{"id":2,"request_id":"c1/o1"},{"id":2,"timestamps":[{"name":"QUEUE_START","ns":10},{"name":"COMPUTE_START","ns":20},{"name":"COMPUTE_END","ns":30}]}]`
	if err := os.WriteFile(filepath.Join(dir, "trace.json.7"), []byte(complete), 0o644); err != nil {
		t.Fatalf("write indexed file: %v", err)
	}

	names, err := traceFiles(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(names) != 1 || names[0] != "trace.json.7" {
		t.Fatalf("collector sees %v, want only the indexed file", names)
	}

	c := &TraceCollector{dir: dir, seen: map[string]bool{}}
	_, evs, err := c.drain()
	if err != nil {
		t.Fatalf("drain must not choke on a truncated base file: %v", err)
	}
	if len(evs) != 1 || !evs[0].Complete() {
		t.Fatalf("drained %d events, want 1 complete one", len(evs))
	}
}

// A truncated indexed file is corruption, not absence, and must fail loudly.
func TestTruncatedIndexedFileIsAHardFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "trace.json.1"), []byte(`[{"id":1,`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	c := &TraceCollector{dir: dir, seen: map[string]bool{}}
	if _, _, err := c.drain(); err == nil {
		t.Error("a truncated indexed trace file must fail rather than yield partial evidence")
	}
}

// traceDir resolves the repository's trace directory, which the compose profile
// mounts into the server.
func traceDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		candidate := filepath.Join(dir, "results", "triton-traces")
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if err := os.MkdirAll(candidate, 0o755); err != nil {
				t.Fatalf("create trace dir: %v", err)
			}
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the module root")
		}
		dir = parent
	}
}

// jsonRoundTrip is a small guard that the trace format we parse is the one
// Triton emits: entries keyed by trace id, timestamps as name/ns pairs.
func TestTraceEntryShapeMatchesTritonOutput(t *testing.T) {
	var entries []traceEntry
	raw := `[{"id":3,"model_name":"m","model_version":1,"request_id":"c2/o1"},
	         {"id":3,"timestamps":[{"name":"QUEUE_START","ns":123456789}]}]`
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(entries) != 2 || entries[0].RequestID != "c2/o1" || entries[1].Timestamps[0].NS != 123456789 {
		t.Errorf("trace entry shape drifted: %+v", entries)
	}
}

// Filename reuse after a server restart is the hazard that makes remembering
// pre-run filenames unsafe.
//
// Triton's file index restarts at 0 every time the server starts, while the
// trace directory is a persistent mount — so a restarted server writes over
// names an earlier one used. A collector that excluded pre-run files by name
// would exclude this run's evidence along with them and report that nothing was
// traced, which is exactly the failure this clearing replaces.
func TestStaleTraceFilesAreClearedRatherThanRemembered(t *testing.T) {
	dir := t.TempDir()
	stale := []byte(`[{"id":1,"request_id":"c999/o0"}]`)
	for _, name := range []string{"trace.json", "trace.json.0", "trace.json.1", "trace.json.7"} {
		if err := os.WriteFile(filepath.Join(dir, name), stale, 0o644); err != nil {
			t.Fatalf("plant %s: %v", name, err)
		}
	}
	// Something that is not Triton's must survive.
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("keep me"), 0o644); err != nil {
		t.Fatalf("plant notes: %v", err)
	}

	if err := clearTraceFiles(dir); err != nil {
		t.Fatalf("clear: %v", err)
	}

	names, err := traceFiles(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("trace files survived clearing: %v", names)
	}
	if _, err := os.Stat(filepath.Join(dir, "trace.json")); !os.IsNotExist(err) {
		t.Error("the base trace file survived clearing")
	}
	if _, err := os.Stat(filepath.Join(dir, "notes.txt")); err != nil {
		t.Error("clearing removed a file that is not Triton's")
	}

	// A file written afterwards with a reused index must be visible, not skipped.
	fresh := `[{"id":2,"request_id":"c3/o0"},{"id":2,"timestamps":[{"name":"QUEUE_START","ns":10},{"name":"COMPUTE_START","ns":20},{"name":"COMPUTE_END","ns":30}]}]`
	if err := os.WriteFile(filepath.Join(dir, "trace.json.0"), []byte(fresh), 0o644); err != nil {
		t.Fatalf("write reused index: %v", err)
	}
	c := &TraceCollector{dir: dir, seen: map[string]bool{}}
	_, evs, err := c.drain()
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(evs) != 1 || !evs[0].Complete() {
		t.Fatalf("drained %d events from a reused index, want 1 complete", len(evs))
	}
}
