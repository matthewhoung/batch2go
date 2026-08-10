---
status: accepted
date: 2026-08-10
---

# Single release barrier at the load generator

"Barrier" had been covering three different concepts, and the draft architecture placed a cohort barrier in the backend adapter for A=OFF cells. An adapter-side join would inject formation wait into F00 — the OFF/OFF baseline — and contaminate δ_A, contradicting the design note's requirements that W_form is controlled ≈0 in identification cells and that the fixed-cohort barrier is control-only *at the load generator*. We decided: the load generator's release barrier is the only runtime synchronization point; the proxy seals envelopes only under A=ON; the adapter never joins or synchronizes under A=OFF — it forwards on arrival. A cohort at A=OFF is an accounting label (cohort_id + ordinal minted by the load generator, carried end-to-end, joined offline by the validator), never a runtime object.

## Consequences

- `t_cohort_seal` ownership is conditional: A=ON → proxy (envelope seal); A=OFF → load generator (barrier release). The event schema records the emitter.
- The adapter loses its barrier state machine; contract tests assert the *absence* of adapter-side waiting at A=OFF.
- Stage C open-loop formation (W_form at the proxy, A=ON deployment cells) is unchanged.
