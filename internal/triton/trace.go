package triton

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	tritonv2 "github.com/matthewhoung/batch2go/api/triton/v2"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// Triton's trace activity names. Only the three the schema owns are mapped into
// event records; the rest are retained inside S_comp as sub-spans (M2-PLAN §4.1).
const (
	activityRequestStart       = "REQUEST_START"
	activityQueueStart         = "QUEUE_START"
	activityComputeStart       = "COMPUTE_START"
	activityComputeInputEnd    = "COMPUTE_INPUT_END"
	activityComputeOutputStart = "COMPUTE_OUTPUT_START"
	activityComputeEnd         = "COMPUTE_END"
	activityRequestEnd         = "REQUEST_END"
)

// TraceEvent is one request's backend timestamps, joined to the logical request
// that produced it by the request id the client set — never by timestamp
// proximity, which is not identity.
//
// The timestamps are raw CLOCK_MONOTONIC nanoseconds. Triton writes them
// unconverted in this trace mode, which is the only reason they can be
// subtracted against the Go processes' readings; the OpenTelemetry mode converts
// to wall clock inside Triton and is unusable here (ADR-0008).
type TraceEvent struct {
	Request   identity.LogicalRequest `json:"request"`
	RequestID string                  `json:"request_id"`
	ModelName string                  `json:"model_name"`
	TraceID   uint64                  `json:"trace_id"`

	QueueStart   int64 `json:"t_queue_start"`
	ComputeStart int64 `json:"t_compute_start"`
	ComputeEnd   int64 `json:"t_compute_end"`

	// Sub-spans retained inside S_comp, recorded but not part of the 15.
	RequestStart       int64 `json:"request_start"`
	RequestEnd         int64 `json:"request_end"`
	ComputeInputEnd    int64 `json:"compute_input_end"`
	ComputeOutputStart int64 `json:"compute_output_start"`
}

// Complete reports whether the three schema timestamps are all present.
func (e TraceEvent) Complete() bool {
	return e.QueueStart != 0 && e.ComputeStart != 0 && e.ComputeEnd != 0
}

// indexedTraceFile matches the files Triton closes and completes. The base
// trace.json is deliberately excluded: it is an ofstream that Triton keeps open
// for the server's lifetime, so on disk it is empty until shutdown and, once its
// buffer overflows, truncated mid-array. Reading it during a run would either
// find nothing or fail a strict parse on valid data (ADR-0008).
var indexedTraceFile = regexp.MustCompile(`^trace\.json\.\d+$`)

// anyTraceFile matches the whole trace file family, base included, for clearing.
var anyTraceFile = regexp.MustCompile(`^trace\.json(\.\d+)?$`)

// TraceCollector gathers the backend timestamps produced during one run.
//
// Triton has no flush verb. Traces accumulate in memory and reach disk only on
// rotation, on trace-count exhaustion, or when a trace setting is replaced — the
// last of which is the maintainer-suggested way to write on demand, and the one
// used here. Two consequences shape this type:
//
//   - The write is asynchronous. The setting's destructor runs only once every
//     in-flight trace has released it, and the gRPC frontend releases after the
//     response is written, so the file appears tens of milliseconds after the
//     RPC returns. Collect therefore waits for the evidence rather than reading
//     once and hoping.
//   - Loss must be detectable. Triton's own remaining-count is read back as an
//     independent tally of how many requests it sampled, which catches the one
//     failure the trace file cannot reveal: a request the server never traced.
type TraceCollector struct {
	gw  *Gateway
	dir string

	seen map[string]bool
	freq uint32

	// budget is the trace_count handed to Triton, sized never to reach zero.
	// Exhausting it would stop tracing and make later setting updates that omit
	// trace_count fail, so the budget is generous and always re-sent.
	budget    int
	budgetAt  int
	collected []TraceEvent
}

// TraceRotation is how many traces Triton buffers before rotating to a new file
// on its own. It trades memory against file count: too small and a run scatters
// its evidence over hundreds of fragments, too large and a long run holds every
// trace in the server's memory until the end. At roughly 700 bytes per traced
// request this retains about 3 MiB.
const TraceRotation = 4096

// traceBudgetMargin is added to the expected trace count so the count can never
// reach zero during a run.
const traceBudgetMargin = 4096

// FlushTimeout bounds the wait for Triton to write out a run's traces.
const FlushTimeout = 30 * time.Second

// NewTraceCollector prepares trace collection for one run of expectedTraces
// requests.
//
// Setting the rotation and the budget first also flushes whatever Triton was
// still holding — warm-up traces, or another run's tail — so the file snapshot
// taken immediately afterwards separates this run's evidence from what came
// before it.
func NewTraceCollector(ctx context.Context, gw *Gateway, dir string, expectedTraces int) (*TraceCollector, error) {
	c := &TraceCollector{
		gw:     gw,
		dir:    dir,
		seen:   map[string]bool{},
		freq:   TraceRotation,
		budget: expectedTraces + traceBudgetMargin,
	}
	if err := c.applySettings(ctx, c.freq, c.budget); err != nil {
		return nil, err
	}

	// The pre-run flush is asynchronous, so let it land before clearing;
	// otherwise a warm-up file would arrive afterwards and be counted as this
	// run's evidence.
	if err := c.settle(ctx); err != nil {
		return nil, err
	}

	// Empty the directory rather than remember what was in it.
	//
	// Triton's file index restarts at 0 every time the server starts, but the
	// directory is a persistent mount — so a restarted server reuses, and
	// overwrites, filenames a previous one wrote. Excluding pre-run files by name
	// would then silently exclude this run's evidence too, which is exactly the
	// failure this replaces. The directory is per-run scratch: raw traces are
	// copied into the bundle at finalization, and warm-up produces no evidence
	// worth keeping.
	if err := clearTraceFiles(dir); err != nil {
		return nil, err
	}

	remaining, err := c.remainingCount(ctx)
	if err != nil {
		return nil, err
	}
	c.budgetAt = remaining
	return c, nil
}

// Collect flushes Triton's pending traces and waits until every expected request
// has appeared, returning the traces and the files they came from.
//
// A timeout returns whatever did arrive together with an error naming the
// shortfall: partial evidence plus a loud failure is strictly more useful than
// either silence or an empty result.
func (c *TraceCollector) Collect(ctx context.Context, expected []identity.LogicalRequest) ([]TraceEvent, []string, error) {
	want := make(map[identity.LogicalRequest]bool, len(expected))
	for _, r := range expected {
		want[r] = true
	}

	// Triton's tally is read BEFORE the flush. The flush re-sends the budget so
	// the setting update can never be rejected for omitting it, and that re-send
	// resets the remaining count — reading afterwards would always see a full
	// budget and report that nothing was sampled.
	if err := c.checkServerCount(ctx, len(expected)); err != nil {
		return nil, nil, err
	}

	if err := c.flush(ctx); err != nil {
		return nil, nil, err
	}
	// The flush restored the budget, so the next collection measures from there.
	c.budgetAt = c.budget

	var files []string
	found := map[identity.LogicalRequest]bool{}
	deadline := time.Now().Add(FlushTimeout)

	for {
		newFiles, newEvents, err := c.drain()
		if err != nil {
			return c.collected, files, err
		}
		files = append(files, newFiles...)
		c.collected = append(c.collected, newEvents...)
		for _, e := range newEvents {
			if want[e.Request] && e.Complete() {
				found[e.Request] = true
			}
		}
		if len(found) >= len(want) {
			break
		}
		if time.Now().After(deadline) {
			return c.collected, files, fmt.Errorf(
				"triton: only %d of %d expected traces were written within %s; Triton's trace flush is asynchronous and did not complete",
				len(found), len(want), FlushTimeout)
		}
		select {
		case <-ctx.Done():
			return c.collected, files, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}

	return c.collected, files, nil
}

// checkServerCount compares Triton's own tally of sampled requests against what
// the run released.
//
// This is the second, independent opinion on completeness. The trace file can
// only report requests Triton traced; it is silent about a request the server
// never sampled at all. The remaining-count delta is Triton's own answer to
// "how many did you see", so a disagreement means either a request never
// arrived or something else is sharing this trace setting.
func (c *TraceCollector) checkServerCount(ctx context.Context, released int) error {
	remaining, err := c.remainingCount(ctx)
	if err != nil {
		return err
	}
	if remaining <= 0 {
		return fmt.Errorf("triton: the trace budget was exhausted mid-run; evidence after that point was never collected")
	}
	sampled := c.budgetAt - remaining
	if sampled != released {
		return fmt.Errorf(
			"triton: the server sampled %d requests but the run released %d; the trace setting saw traffic this run did not release, or a request never reached the server",
			sampled, released)
	}
	return nil
}

// settle waits for a pending asynchronous flush to stop producing new files.
func (c *TraceCollector) settle(ctx context.Context) error {
	deadline := time.Now().Add(FlushTimeout)
	stable := 0
	last := -1
	for {
		names, err := traceFiles(c.dir)
		if err != nil {
			return err
		}
		if len(names) == last {
			stable++
			if stable >= 3 {
				return nil
			}
		} else {
			stable = 0
			last = len(names)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("triton: trace directory %s never settled within %s", c.dir, FlushTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// drain reads whichever indexed files have appeared since the last call.
func (c *TraceCollector) drain() ([]string, []TraceEvent, error) {
	names, err := traceFiles(c.dir)
	if err != nil {
		return nil, nil, err
	}
	var files []string
	var out []TraceEvent
	for _, name := range names {
		if c.seen[name] {
			continue
		}
		c.seen[name] = true

		path := filepath.Join(c.dir, name)
		evs, err := ParseTraceFile(path)
		if err != nil {
			return nil, nil, err
		}
		if len(evs) == 0 {
			continue
		}
		out = append(out, evs...)
		files = append(files, path)
	}
	return files, out, nil
}

// flush makes Triton write out the traces it is still holding.
//
// There is no flush verb and the trace file cannot be repointed at runtime, but
// replacing a trace setting destroys the old one, and its destructor writes the
// remainder. Changing log_frequency is therefore the flush — the approach
// Triton's maintainers suggest for writing on demand. The value alternates so
// that consecutive collections each constitute a real change rather than a no-op
// update, and the budget is always re-sent so the update can never be rejected
// for omitting it.
func (c *TraceCollector) flush(ctx context.Context) error {
	if c.freq == TraceRotation {
		c.freq = TraceRotation + 1
	} else {
		c.freq = TraceRotation
	}
	return c.applySettings(ctx, c.freq, c.budget)
}

func (c *TraceCollector) applySettings(ctx context.Context, freq uint32, budget int) error {
	settings := map[string]*tritonv2.TraceSettingRequest_SettingValue{
		"log_frequency": {Value: []string{strconv.FormatUint(uint64(freq), 10)}},
		"trace_count":   {Value: []string{strconv.Itoa(budget)}},
	}
	if _, err := c.gw.client.TraceSetting(ctx, &tritonv2.TraceSettingRequest{Settings: settings}); err != nil {
		return fmt.Errorf("triton: update trace settings: %w", err)
	}
	return nil
}

// remainingCount reads back Triton's remaining trace budget.
func (c *TraceCollector) remainingCount(ctx context.Context) (int, error) {
	resp, err := c.gw.client.TraceSetting(ctx, &tritonv2.TraceSettingRequest{})
	if err != nil {
		return 0, fmt.Errorf("triton: read trace settings: %w", err)
	}
	setting, ok := resp.GetSettings()["trace_count"]
	if !ok || len(setting.GetValue()) == 0 {
		return 0, fmt.Errorf("triton: trace settings carry no trace_count")
	}
	n, err := strconv.Atoi(setting.GetValue()[0])
	if err != nil {
		return 0, fmt.Errorf("triton: parse trace_count %q: %w", setting.GetValue()[0], err)
	}
	return n, nil
}

// clearTraceFiles empties the trace directory of Triton's file family.
//
// Removing the contents is safe underneath a running server: the indexed files
// are already closed, and the base file is held open, so unlinking it leaves
// Triton writing to an inode nobody reads.
func clearTraceFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("triton: read trace directory %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !anyTraceFile.MatchString(e.Name()) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("triton: clear stale trace file: %w", err)
		}
	}
	return nil
}

func traceFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("triton: read trace directory %s: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !indexedTraceFile.MatchString(e.Name()) {
			continue
		}
		names = append(names, e.Name())
	}
	return names, nil
}

// traceEntry is one record in Triton's trace file. Entries are emitted
// incrementally: some carry a trace's identity, others carry one timestamp, and
// all of them share the trace id.
type traceEntry struct {
	ID           uint64 `json:"id"`
	ModelName    string `json:"model_name"`
	ModelVersion any    `json:"model_version"`
	RequestID    string `json:"request_id"`
	Timestamps   []struct {
		Name string `json:"name"`
		NS   int64  `json:"ns"`
	} `json:"timestamps"`
}

// ParseTraceFile reads one Triton trace file into per-request events.
//
// The parse is strict on purpose. Triton writes a separator comma before
// iterating a trace's streams, so a request that dies before any activity fires
// emits an empty element and invalidates the array. That is loud corruption
// rather than silent loss — but only because this refuses to be lenient.
func ParseTraceFile(path string) ([]TraceEvent, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("triton: read trace file %s: %w", path, err)
	}
	if len(b) == 0 {
		return nil, nil
	}
	var entries []traceEntry
	if err := json.Unmarshal(b, &entries); err != nil {
		return nil, fmt.Errorf("triton: parse trace file %s: %w", path, err)
	}

	byTrace := map[uint64]*TraceEvent{}
	order := []uint64{}
	for _, e := range entries {
		ev, ok := byTrace[e.ID]
		if !ok {
			ev = &TraceEvent{TraceID: e.ID}
			byTrace[e.ID] = ev
			order = append(order, e.ID)
		}
		if e.ModelName != "" {
			ev.ModelName = e.ModelName
		}
		if e.RequestID != "" {
			ev.RequestID = e.RequestID
		}
		for _, ts := range e.Timestamps {
			switch ts.Name {
			case activityRequestStart:
				ev.RequestStart = ts.NS
			case activityQueueStart:
				ev.QueueStart = ts.NS
			case activityComputeStart:
				ev.ComputeStart = ts.NS
			case activityComputeInputEnd:
				ev.ComputeInputEnd = ts.NS
			case activityComputeOutputStart:
				ev.ComputeOutputStart = ts.NS
			case activityComputeEnd:
				ev.ComputeEnd = ts.NS
			case activityRequestEnd:
				ev.RequestEnd = ts.NS
			}
		}
	}

	out := make([]TraceEvent, 0, len(order))
	for _, id := range order {
		ev := byTrace[id]
		if ev.RequestID == "" {
			// A trace with no request id cannot be attributed to a logical request,
			// and attributing it by timing would be exactly the inference this
			// design forbids. It is dropped, and the run's completeness check is
			// what turns a dropped trace into a visible failure.
			continue
		}
		req, err := events.ParseTritonRequestID(ev.RequestID)
		if err != nil {
			continue
		}
		ev.Request = req
		out = append(out, *ev)
	}
	return out, nil
}
