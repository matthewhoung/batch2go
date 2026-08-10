---
status: accepted
date: 2026-08-10
---

# Membership evidence is self-attesting where possible

Triton's trace API provides no execution-group ID; its own tooling infers co-batching from identical compute timestamps — exactly the timestamp-proximity inference this design forbids. And the naive fix fails silently: Triton scatters batched outputs back per request along the batch dimension, so a model that merely echoes its uid input returns each request its *own* uid — evidence-shaped, attesting nothing.

Decision: synthetic models carry a `uid` input and a Tile-expanded output (uid `[B,1]` → full-set `[B,B]`, second dim declared variable), so every response contains the complete uid set of its execution — membership is self-attesting, including under M>1 cross-cohort coalescing. Real CV models get graph-surgery variants (append uid input + Identity/Tile output; compute path untouched, verified by bitwise output equivalence on fixed inputs; digest change declared), adopted only if a pre-Stage-C A/B check at one anchor configuration shows overhead below half the frozen tracing bound; otherwise real models fall back to the M=1 counting argument (clean window: B requests in, one execution, batch histogram at B) plus three-source reconciliation, declared inferential at M>1.

## Consequences

- Contract tests assert the full-batch uid set; own-uid echo is a failure, not a pass.
- When the graph-surgery gate passes, the design note's declared M>1 membership limitation is upgraded from limitation to physical evidence.
