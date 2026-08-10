# batch2go

**A factorial measurement platform for GPU inference serving — built to separate what "batching" actually means.**

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-building_the_instrument-orange.svg)](docs/specs/0001-walking-skeleton-d0-f00.md)

When an inference service "batches requests", at least five distinct mechanisms hide under that one word: a proxy aggregating requests into a cohort, multiple requests sharing one RPC envelope, a backend scheduler grouping concurrently pending requests, inputs being concatenated into one `[B,…]` tensor, and GPU kernels executing over that tensor. Most performance evaluations measure these conflated and attribute the outcome to "batching".

batch2go is the measurement instrument for a study that identifies two of those mechanisms separately — with per-run physical evidence that every experimental condition actually did what its label claims.

## The question

- **Factor A — transport-envelope aggregation:** do B logical requests travel in one shared transport envelope, or in B independent ones?
- **Factor V — compute vectorization:** do B logical requests execute as one `[B,…]` tensor execution, or as B individual `[1,…]` executions?

With a fixed effective batch size and one shared request path (LoadGen → Proxy → Adapter → Triton) for every factorial cell, the design identifies each factor's main effect, their interaction, and where the trade-off crosses over as payload size, compute intensity, and offered load vary.

| Cell | A | V | What happens |
|---|---|---|---|
| F00 | off | off | B independent RPCs, B individual executions — the baseline |
| F01 | off | on | B independent RPCs; the backend scheduler forms one `[B,…]` execution |
| F10 | on | off | one envelope carries B requests; still B individual executions |
| F11-D | on | on | one envelope; scheduler-formed `[B,…]` execution |
| F11-P | — | — | one envelope; pre-formed `[B,…]` tensor (formation-location contrast, outside the factorial) |
| D0 | — | — | direct path with no proxy (diagnostic: proxy-path tax) |

## Evidence, not labels

The platform's core rule: a factor level is an assigned code-level policy, and every run must physically prove the policy happened. Some of what that takes:

- **Self-attesting batch membership.** Triton's trace API has no execution-group ID — its own tooling infers "these requests were batched together" from identical timestamps, which is exactly the kind of inference this design forbids. Instead, the synthetic models carry a `uid` input and return each execution's *complete* uid set to every member (a naive echo fails silently: batched outputs are scattered back per request, so each request would just receive its own uid back). Membership becomes something the model itself attests, not something reconstructed from timing.
- **A 15-timestamp schema with conservation checks.** Stage timestamps across four processes, monotonic-clock domains with boot-identity verification, and per-topology conservation tests — additive for single-submission cells, critical-path for multi-RPC cells — with residuals reported signed, never relabeled. An instrument self-test must recover injected known delays before any live number is trusted.
- **Treatment-correlated contamination bounds.** Cells at A=off emit ~B× more trace events than A=on, and allocate B small envelopes where A=on allocates one large one — so both tracing overhead *and* Go GC behavior are measured per cell and frozen as bounds; an observed effect smaller than its bound is declared uninterpretable.
- **Serialization is part of the treatment.** Envelope packing cost is a declared constituent of the aggregation effect, so the canonical implementation is pinned (single-copy, preallocated buffers) and a sensitivity variant bounds how much of the measured effect is implementation artifact.
- **Absence is typed.** Every event carries a stage-presence mask; "this stage doesn't exist in this cell's topology" and "this timestamp is missing" are different facts, and the validator knows which one it is looking at.

## Architecture

```text
                        Control plane
            +-----------------------------------+
            | Experiment Runner                 |
            | manifest -> schedule -> sessions  |
            +----+-------------------------+----+
                 |                         |
                 v                         v
          Model Repository           Result Validator
          Builder                    (evidence, conservation,
                                      conformance gates)

                         Data plane
+---------+     +-------+     +-----------------+     +--------+
| LoadGen | --> | Proxy | --> | Backend Adapter | --> | Triton |
+---------+     +-------+     +-----------------+     +--------+
     |              |                  |                   |
     +--------------+---------+--------+-------------------+
                              v
              Scientific event records (primary evidence)

                  Observability plane (secondary)
   Prometheus / Grafana — operational telemetry; never testifies

                     Infrastructure plane
   Terraform on AWS — zero-inbound, budget guardrails,
   provably clean teardown after every session block
```

Runs are defined by versioned declarative manifests; models are digest-verified immutable artifacts; a wrong model, missing evidence, or protocol drift fails the run visibly. The observability stack exists so a running cloud experiment can be watched live — but no dashboard number is ever quoted as a result; evidence comes from the event records alone.

## How this repository is built

Docs-first, decisions on the record:

- [CONTEXT.md](CONTEXT.md) — the project's ubiquitous language, including the synonyms each term forbids ("batching" unqualified is banned vocabulary here).
- [docs/adr/](docs/adr/) — every settled design decision, with the trade-off that produced it.
- [docs/specs/](docs/specs/) — numbered implementation specs. [Spec 0001](docs/specs/0001-walking-skeleton-d0-f00.md) is a walking skeleton: the smallest end-to-end slice (two cells, full evidence chain) that proves the schema, clock rules, and validator before any horizontal growth.
- **Two permanent test seams.** Acceptance lives only at (1) manifest → validated run bundle against real Triton, and (2) recorded events → validator verdict on fixtures the live stack cannot produce. Unit tests exist beneath the seams; they are not acceptance.
- **Preregistration before confirmatory data.** Research questions, hypotheses, exclusion rules, and the statistical analysis plan will be frozen and publicly tagged before confirmatory data collection begins. The full internal design notes are maintained privately; every decision that shapes this public artifact lands here as an ADR, and the preregistration will be fully self-contained.

**There are no performance numbers in this repository yet, by design.** None will appear until the preregistered confirmatory runs complete — an instrument that reports results before it is validated is exactly the failure mode this project exists to avoid.

## Status

- [x] Public design record: glossary, ADR-0001…0007, spec 0001
- [ ] Spec 0001 — walking skeleton: D0 + F00 end to end *(in progress)*
- [ ] Spec 0002 — envelope aggregation (F10)
- [ ] Spec 0003 — scheduler-formed batching (F01, F11-D) + exact-B conformance
- [ ] Spec 0004 — full contract matrix, all seven cells
- [ ] Spec 0005 — AWS infrastructure (Terraform; provably clean teardown)
- [ ] Spec 0006 — observability stack
- [ ] Public preregistration tag → calibration and confirmatory stages

A frozen legacy prototype (separate repository) produced this project's exploratory pilot data; by explicit contract, not one file migrates — this codebase is built clean against the recorded design.

## Stack

Go (data & control planes) · gRPC / Protobuf · NVIDIA Triton Inference Server · ONNX Runtime · Docker Compose · Terraform + AWS · Prometheus + Grafana · Python / uv (offline analysis)

## Author

Research and engineering by [@matthewhoung](https://github.com/matthewhoung) · matthewhoung@gmail.com
Built as the measurement platform for a graduate research project in ML-serving performance evaluation, prepared for peer-reviewed submission.

## License

[Apache-2.0](LICENSE)
