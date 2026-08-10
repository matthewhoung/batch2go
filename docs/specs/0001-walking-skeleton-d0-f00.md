# Spec 0001 — Walking skeleton: D0 + F00 end to end

> Status: ready for implementation · 2026-08-10
> A *walking skeleton* is the smallest end-to-end implementation that proves the architecture works — every later capability is added to a skeleton that already walks. Vocabulary: [CONTEXT.md](../../CONTEXT.md); decisions: [docs/adr/](../adr/).

## Problem Statement

Every confirmatory number this project will ever report rests on the correctness of its measurement instrument: the timestamp schema, the clock rules, the evidence records, and the conservation checks. Errors in these — a mis-owned timestamp, an illegitimate cross-process clock subtraction, a membership claim that only looks like evidence — do not surface in unit tests of individual components. They surface only when real requests traverse the real path against a real Triton server and the resulting records are checked end to end.

Building the seven experimental cells layer by layer (all transport first, all executors next, schema last) would mean the first end-to-end run — and therefore the first exposure of schema and clock defects — happens weeks in, after every component has already hardened around untested assumptions. The archived project history records exactly this lesson: the analysis pipeline must be exercised on synthetic data before confirmatory runs.

## Solution

Build the smallest slice that exercises the entire evidence chain: the two simplest cells, **D0** (direct path, no proxy) and **F00** (full shared path, everything OFF), running end to end on the local validation environment at cohort size B=4, producing complete event records and evidence that a validator checks mechanically.

This slice stands up both permanent test seams:

1. **Live seam — manifest → validated run bundle:** a manifest fully determines a run; the run executes against real Triton; acceptance asserts on the resulting bundle (execution counts and shapes, self-attested membership, stage-presence masks, conservation residuals).
2. **Offline seam — event records → validator verdict:** the validator is exercised on fixtures the live stack cannot produce — injected known delays it must recover, and deliberately defective bundles it must reject.

Every subsequent cell (F10, F01, F11-D, F11-P, F00-seq) is then a horizontal extension of a skeleton that already walks, tested through the same two seams.

## User Stories

1. As the experimenter, I want one command to bring up the full validation stack (Triton, proxy, adapter) with pinned image digests, so that every run starts from a reproducible environment.
2. As the experimenter, I want a versioned manifest to fully determine a run (cell, B, model, payload, seeds, durations, tracing mode), so that no experimental behavior is decided by code defaults or shell arguments.
3. As the experimenter, I want the load generator to release each cohort's B requests through a release barrier and mint (cohort_id, ordinal) labels carried end to end, so that cohorts exist as accounting facts without any downstream synchronization.
4. As the experimenter, I want D0 to use an isolated direct client that is structurally unusable as a factorial-cell path, so that the diagnostic path can never be confused with the shared path.
5. As the experimenter, I want F00's proxy to be a pure pass-through — no joining, no cohort seal — so that the OFF/OFF baseline carries no formation wait.
6. As the experimenter, I want the adapter to dispatch every F00 envelope to the executor on arrival, so that waiting can occur only in the backend queue, where the design books it.
7. As the experimenter, I want D0 and F00 to share one single-request submission policy (identical request construction against the unbatched model entry), so that the proxy-path tax contrast compares paths, not client implementations.
8. As the experimenter, I want the synthetic model to return the full uid set of each execution to every member of that execution, so that membership is self-attesting physical evidence rather than a timestamp inference.
9. As the experimenter, I want a permanent counter-fixture proving that an own-uid-only echo fails the membership assertion, so that a future model change cannot silently reduce self-attestation to evidence-shaped noise.
10. As the experimenter, I want the run to fail visibly if the unbatched model entry ever coalesces two singles into one execution, so that V=OFF cells cannot silently become batched.
11. As the experimenter, I want every process to timestamp with direct monotonic-clock reads inside a verified clock domain (boot-identity check recorded per session), so that same-host cross-process subtraction is legitimate by construction.
12. As the experimenter, I want event records written allocation-free and binary on the hot path and archived as Parquet at run finalization, so that recording cost is small and the archive is analysis-ready.
13. As the experimenter, I want every event to carry a stage-presence mask, so that "absent by topology" (D0 has no proxy stages) is typed differently from "missing timestamp" — the former expected, the latter a validation failure.
14. As the experimenter, I want pinned GC settings recorded in the manifest and per-run GC statistics in the bundle, so that the GC covariate exists from the very first run.
15. As the experimenter, I want the conservation self-test to recover injected known delays within tolerance before any live conservation number is trusted, so that the instrument is validated before it testifies.
16. As the experimenter, I want live D0 and F00 runs to satisfy critical-path conservation within the declared tolerance, with residuals reported signed and never relabeled, so that the timestamp schema is demonstrated on real traffic.
17. As the experimenter, I want Triton run in explicit model-control mode with digest-verified model loading, so that the model actually serving is provably the model the manifest named.
18. As the experimenter, I want request payloads to include the declared padding input consumed inside the model, so that payload size is realized on every hop — including host-to-device — from the first slice onward.
19. As the analyst, I want run bundles to be self-describing (manifest, schema version, clock-domain record, evidence, event records), so that offline analysis needs no out-of-band context.
20. As the analyst, I want the validator to be pure functions over recorded data, so that any verdict is reproducible from the archived bundle alone.
21. As the artifact reviewer, I want one command that runs the walking-skeleton acceptance suite, so that the repository's claimed properties are checkable without reading internals.
22. As the artifact reviewer, I want the spec, glossary, and ADRs to use one vocabulary, so that terms in code, records, and paper drafts resolve to the same public definitions.
23. As the future implementer, I want the executor interface to accept a dispatch — the set of members released in one call — so that later cells (fan-out at A=ON, preformed batches) extend the seam without reshaping it.
24. As the future implementer, I want the F00 contract test to be a template (manifest + assertion set), so that adding a cell means adding a manifest and its expected evidence, not new harness machinery.

## Implementation Decisions

- **Modules stood up:** load generator (workload scheduling, release barrier, client-side events), proxy (pass-through mode only), backend adapter (envelope termination, dispatch-on-arrival), individual executor over a shared single-request submission engine, Triton gateway (readiness, request building, statistics snapshots), events module (schema, writers), model repository builder (synthetic model generation, unbatched entry), result validator (presence, membership, contamination, conservation), test kit (injected-delay and defect fixtures), and a minimal run harness that turns a manifest into a validated bundle.
- **Executor interface:** takes a *dispatch* (members released in one call — n=1 per envelope at A=OFF; n=B at A=ON in later slices) and returns results plus execution evidence, never conclusions. Individual and dynamic executors are thin policies over one shared submission engine; ordering guarantees apply to result↔member mapping, never submission order (ADR-0001 consequence).
- **Release barrier:** the load generator is the only runtime synchronization point; cohorts at A=OFF are accounting labels joined offline; the cohort-seal timestamp is owned by the load generator at A=OFF and by the proxy at A=ON (ADR-0001).
- **Envelope protocol v1:** versioned identity-carrying protobuf; at A=OFF each envelope carries exactly one logical request; canonical serialization is single-copy with preallocated pooled buffers; gRPC message ceiling and flow-control windows are manifest constants (ADR-0003).
- **Event records:** the 15-timestamp vocabulary with per-topology presence; optional monotonic-nanosecond fields plus a stage-presence bitmask; binary length-delimited hot path into per-process ring buffers with background flush; Parquet + zstd conversion at run finalization (ADR-0005).
- **Clock rules:** timestamps come from direct CLOCK_MONOTONIC reads (Go's wall-clock type strips its monotonic component across process boundaries, so it is never serialized); each session records a clock-domain identity including boot identity; cross-domain subtraction is a validator error.
- **Synthetic model:** an ONNX repeated-block graph — κ is the block count, a dimensionless graph parameter (ADR-0002); this slice uses one small level with no millisecond calibration. Inputs: data tensor, padding tensor consumed by a no-op slice, uid tensor. Output includes the Tile-expanded full-batch uid vector so each member receives its execution's complete membership (ADR-0007). Generated deterministically; digest recorded in the artifact catalog.
- **Triton:** pinned container digest, ONNX Runtime backend, explicit model-control mode; only the unbatched entry exists in this slice; the batch-size histogram and execution-count deltas are captured around every run and the coalescing contamination check is a hard failure.
- **Fixed parameters:** B=4, one payload level, one κ level, the local validation environment only. Timing on this environment is a correctness check, never a performance claim.
- **GC covariate:** GC target and memory-limit settings pinned in the manifest; per-run GC statistics (collections, total pause, pause p99, heap peak) recorded in the bundle (ADR-0004).
- **No CI/CD:** all acceptance is local make targets. All git operations are performed by the author personally.

## Testing Decisions

- **A good test asserts external behavior at a seam** — the contents of a validated bundle or a validator verdict — never internal state, goroutine structure, or private functions.
- **Live seam (acceptance):** the D0 and F00 contract tests — manifest in, validated bundle out, against real Triton via the compose stack. Assertions per cell: execution count n=B with all shapes `[1,…]`, J=B distinct executions, self-attested membership consistent with cohort labels, presence mask exactly matching the cell topology, contamination check clean, conservation within tolerance. One command runs the suite.
- **Offline seam (validator):** fixture bundles the live stack cannot produce — (a) injected known delays through mock components, which the self-test must recover per stage within tolerance before any live conservation number is trusted; (b) defective bundles (missing timestamp vs. absent-by-topology, membership set mismatched to cohort, own-uid-only echo, coalesced singles, cross-domain subtraction), each of which must produce a failing verdict naming the defect.
- **Permanent counter-fixtures:** the naive-echo model and the coalescing case stay in the suite forever; they guard the two silent-failure modes this design discovered.
- **Prior art:** none in this repository (first slice). The conservation self-test and per-cell contract matrix are mandated by the project's milestone plan; the public ADRs mirror the decisions they implement.
- Unit tests (codec round-trips, barrier state machine, request construction) exist beneath the seams but are not acceptance criteria.

## Out of Scope

- All other cells: F10, F01, F11-D, F11-P, F00-seq — specs 0002–0004.
- Exact-B conformance qualification and its gate; dynamic and explicit model entries.
- Overhead-bound freezing (tracing modes, GC bounds): recording starts now, calibration is a later slice.
- Real CV models and the gated membership graph surgery.
- Multi-flight, open-loop arrivals, and any deployment-shaped workload.
- The observability stack (Prometheus/Grafana) and the full experiment runner (scheduling, status endpoints, cross-block model lifecycle, S3 upload).
- **AWS and Terraform** — deliberately its own spec (0005) with its own third seam: apply → smoke → destroy → clean-account verdict, asserting via the provider API that nothing billable survives except the two S3 buckets and the Terraform state, plus budget alerts, dead-man auto-stop, and idle alarms. That seam activates before any cloud session; nothing in this slice touches the cloud.
- Preregistration content and Stage A matrices.

## Further Notes

- Planned successor specs: 0002 envelope aggregation (F10: one envelope, adapter fan-out, dispatch skew); 0003 scheduler-formed batching (F01/F11-D: dynamic entry, conformance recording); 0004 contract-matrix completion (F11-P, F00-seq, full gate G1 across B ∈ {4, 16}); 0005 AWS infrastructure (the third seam); 0006 observability. Each is written after the previous slice lands, carrying its lessons.
- "Walking skeleton" is used in its standard sense: the smallest end-to-end implementation that proves the architecture, grown thereafter — never a throwaway prototype.
- The scientific rationale for cell semantics is the project's internal design note; everything a reader needs publicly is in the glossary, the ADRs, and this spec.
