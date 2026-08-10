---
status: accepted
date: 2026-08-10
---

# Go GC is a treatment-correlated covariate with a contamination bound

The two Factor-A levels allocate differently by construction (one B·P-sized envelope vs B separate P-sized envelopes per cohort), so GC pressure and pause timing differ systematically with treatment, at the millisecond scale being measured — the same threat class as treatment-correlated instrumentation overhead. We keep Go and treat GC as a first-class measured quantity:

- hot paths are allocation-free (`sync.Pool` buffer reuse for envelopes and event records);
- `GOGC` and `GOMEMLIMIT` are pinned per process and recorded in the manifest;
- every run records `runtime/metrics` GC statistics (collection count, total pause, pause p99, heap peak) as per-cell covariates;
- gate G3 gains a per-cell GC contamination bound alongside the tracing-overhead bound.

An observed δ_A smaller than the GC bound is not interpretable as a factor effect.
