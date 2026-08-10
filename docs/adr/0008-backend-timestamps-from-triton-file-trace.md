---
status: accepted
date: 2026-08-10
---

# Backend timestamps come from Triton's file trace mode

Three of the schema's fifteen timestamps — `t_queue_start`, `t_compute_start`, `t_compute_end` — are owned by the inference server, and the clock rules make their provenance load-bearing: they are subtracted against Go-side `CLOCK_MONOTONIC` readings, so they must be raw monotonic nanoseconds from the same kernel boot. That requirement, not convenience, decides the mechanism.

**OpenTelemetry mode is unusable, despite being the obvious choice.** It pushes spans to a collector and so has no flush problem at all, but Triton converts the captured monotonic timestamps to wall clock in its own code before the SDK is involved (`server/src/tracer.cc:596`, `time_offset_ + nanoseconds{raw_timestamp_ns}`), and `time_offset_` is a per-trace member sampled independently for every request (`tracer.h:319`). The exported values are therefore epoch nanoseconds carrying per-request offset noise, and the raw value survives only in a steady timestamp the OTLP wire format does not transmit. Triton's own source carries a `FIXME` acknowledging this. No SDK upgrade fixes it; the conversion is upstream of the SDK.

**No response-side alternative exists.** Per-request backend timings are not available in `ModelInferResponse`, in response parameters, in headers or trailers, or in the metrics endpoint; `ModelStatistics` is cumulative per model, and attributing it to a request would be inference from proximity, which this design forbids.

**The file mode writes the timestamp unconverted** (`tracer.cc:397` emits `"ns":<steady_clock ns>`), which is why it is the only mechanism that satisfies the clock rule. We verified empirically that these values are `CLOCK_MONOTONIC` — they track `/proc/uptime` and bracket against `clock_gettime` — and that the container shares the host's boot identity, which is what licenses the cross-process subtraction.

Its cost is that Triton has no flush verb. Traces accumulate in memory and reach disk on exactly three triggers: rotation at `log_frequency`, exhaustion of `trace_count`, and replacement of a trace setting, whose destructor writes the remainder. We use the third — the approach Triton's maintainers suggest for writing on demand, and one whose constituent paths are asserted by upstream's own `L0_trace` regression test.

We evaluated `trace_count` as a more documented alternative and rejected it as the primary mechanism. Its write path is conditioned on `(collected_ == sample_)`, which can only hold at `trace_rate == 1` — an undocumented precondition — and if fewer requests arrive than the count, nothing is ever written and nothing reports why. For an instrument built to fail loudly, that failure mode is worse than the one it replaces. It is adopted instead as an independent tally.

## Consequences

- `log_frequency` is set at server startup and must never be 0. At 0 the settings-replacement write appends to the base `trace.json`, an `ofstream` Triton holds open for its whole life: on disk that file is empty until shutdown and truncated mid-array once its buffer overflows. Non-zero from the first instant means every write lands in a closed, complete, indexed file.
- The collector reads only `trace.json.<n>`. The base file is never complete while the server runs.
- The trace directory is per-run scratch and is emptied before each run, rather than having pre-run files remembered by name. Triton's file index restarts at 0 every time the server starts while the directory persists across restarts, so a restarted server writes over names an earlier one used; excluding pre-run files by name would then exclude the new run's evidence along with them, and the run would report that nothing was traced. Raw traces are copied into the bundle at finalization, so nothing is lost by clearing.
- The write is asynchronous — the destructor runs only once every in-flight trace releases the setting, which the gRPC frontend does after the response is written. Collection is therefore a bounded wait for the expected request set, failing loudly on timeout, not a read-once-and-hope.
- `trace_count` is set to a budget that can never reach zero, and the remaining count is read back as Triton's own tally of requests it sampled. Disagreement with what the run released catches the one loss the trace file cannot reveal: a request the server never traced.
- Trace files are parsed strictly. Triton emits a separator before iterating a trace's streams, so a request that dies before any activity fires invalidates the array — loud corruption rather than silent loss, but only because the parse refuses to be lenient.
- The mechanism is pinned by tests against a live server, so an image change that alters any of these properties fails a test rather than a dataset.
- `rate=1` is part of the evidence contract, not a default: sampling would make the records incomplete by construction.
