# Batch2go

Batch2go is a factorial performance-evaluation platform for GPU inference serving. It separates two mechanisms that prior evaluations conflate — transport-envelope aggregation (Factor A) and compute vectorization (Factor V) — and measures their separate effects, their interaction, and their crossover on a shared request path. Scientific authority lives in the internal design notes (not in this repository) and, once tagged, in the public preregistration under `experiments/prereg/`.

## Language

### Factors and cells

**Factor A (transport-envelope aggregation)**:
Whether the B logical requests of a cohort travel in one shared transport envelope (ON) or in B independent envelopes (OFF).
_Avoid_: transport-layer batching, request batching

**Factor V (compute vectorization)**:
Whether the B logical requests execute as one `[B,…]` tensor execution (ON) or as B individual `[1,…]` executions (OFF). Declared as a policy; realized scheduler compliance is measured, never assumed.
_Avoid_: Factor B, compute-layer batching

**Cell**:
One experimental condition named by its factor levels: F00, F01, F10, F11-D (primary factorial); F11-P (formation-location implementation contrast); D0, F00-seq (diagnostics).

**Batching mechanism**:
One of five distinct mechanisms: request aggregation, RPC aggregation, scheduler-side batch formation, tensor batching, GPU vectorized execution.
_Avoid_: "batching" unqualified

### Runtime concepts

**Logical request**:
The unit of work as the client sees it: one input, one result, one (cohort_id, ordinal) identity minted by the load generator.

**Cohort**:
The set of B logical requests released together by the load generator. At A=OFF a cohort is an accounting label joined offline, never a runtime object. At A=ON it is additionally a runtime object, held by the proxy until its members are complete and it can be sealed into one envelope.
_Avoid_: batch, group

**Release barrier**:
The load generator's simultaneous release of a cohort's B requests — the only synchronization point at the load generator, and the only one anywhere in A=OFF cells. At A=ON the proxy also synchronizes, by construction: envelope aggregation cannot begin until a cohort's members are all present, and the adapter's response side rejoins them because one envelope carries them all. The adapter never joins or synchronizes under A=OFF.
_Avoid_: cohort barrier, adapter barrier, A=OFF barrier

**Envelope**:
One transport-level message between proxy and adapter, carrying one logical request (A=OFF) or the whole cohort (A=ON).

**Envelope aggregation**:
The proxy packing a full cohort into one envelope — the A=ON treatment. Happens only at the proxy.

**Formation (W_form)**:
Waiting time to assemble a cohort at the proxy. It exists wherever the proxy aggregates — that is, in every A=ON cell — and is measured, not assumed absent. In identification cells the release barrier controls it to ≈0; under Stage C's open-loop arrivals it becomes a modeled term rather than a controlled constant.

**Executor**:
The adapter component that turns released work into Triton requests: IndividualExecutor (→ unbatched entry), DynamicBatchExecutor (→ dynamic entry), PreformedBatchExecutor (→ explicit entry). Executors return execution evidence, never conclusions.

**Model entry**:
One of three Triton model configurations backed by a single graph artifact: `_unbatched`, `_dynamic`, `_explicit`.

### Measurement

**Membership evidence**:
Proof of which logical requests belonged to which model execution. *Self-attesting* when the model itself returns the full uid set of its batch; *inferential* when reconstructed from counts, traces, and windows. Timestamp proximity is never identity.

**Exact-B conformance**:
The fraction of eligible cohorts whose members execute as exactly one execution of batch size B (F01/F11-D). Stage B entry requires ≥95% per combination.

**Session**:
One full stack recreation (containers, Triton, model load, warm-up with steady-state verification) — the independent statistical unit.

**Event record**:
The append-only scientific record implementing the 15-timestamp schema. Primary evidence; observability metrics never testify.

### Workload parameters

**κ (compute intensity)**:
A synthetic model's repeated-block count — a dimensionless graph parameter. The four levels κ₁..κ₄ are chosen to land near {5, 20, 50, 100} ms on the confirmatory environment; realized milliseconds are per-environment measurements.
_Avoid_: κ as a milliseconds constant

**P (payload)**:
Per-request payload size in MiB (0.25–8), realized via the declared padding input that traverses every hop.

**B (cohort size)**:
The number of logical requests per cohort. Fixed by the conformance gate (16 → 8 → 4 fallback ladder), never by effect size.
