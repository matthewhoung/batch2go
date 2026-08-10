---
status: accepted
date: 2026-08-10
---

# κ is a repeated-block count, not a milliseconds value

κ was drafted as {5, 20, 50, 100} ms, but the same ONNX artifact cannot hold milliseconds constant across GPUs (validation RTX 4060 vs confirmatory L4), so "one digest serves all environments" and "κ calibrated in ms" were mutually exclusive. We define κ as a dimensionless repeated-block count of the synthetic model graph. The four levels are chosen so realized durations land near {5, 20, 50, 100} ms on the confirmatory environment (Env 1, L4); realized milliseconds per environment are measurements recorded in the environment sheet.

The synthetic block targets medium GPU occupancy (~30–50% at B=1): a single compute-bound giant kernel would make g(B) artificially flat (no vectorization headroom), a launch-bound chain of tiny kernels would make it artificially steep — either way g(B) would be an artifact of our own kernel choice. Stage A overlays the synthetic models' g(B) against the four real CV models' measured g(B) as explicit representativeness evidence.
