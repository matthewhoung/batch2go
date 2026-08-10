---
status: accepted
date: 2026-08-10
---

# F10's conservation is interval, not additive

The conservation check is defined per cell topology: an additive identity where a cohort's stages lie on one serial path, and interval/critical-path accounting where they do not, because stages of different requests legitimately overlap and summing them double-counts. F10 was classified additive. It should have been interval, and the classification is corrected here with the professor's ruling on the M1 erratum.

The discriminator is not how many transport envelopes a cohort uses; it is how many serial paths its work takes through the backend. F11-P submits one request that becomes one execution. F11-D submits B requests that the scheduler merges into one execution, so at M=1 there is again one. **F10 submits B requests that become B executions**, dispatched concurrently and serializing on the single model instance — the same backend behavior as F00, which is in the interval branch. One envelope, B paths. Summing the members' stages would count the same wall-clock interval once per member.

Two things made this hard to see. The design note assigns F10 to the interval group when defining the cycle ("for cells with B individual executions (D0/F00/F10) the per-cohort cycle is first release → last client completion, and the B executions serialize on the single model instance") and to the additive group when defining the conservation rule, in the same document. And the additive sentence still names the cell "F11" — the identifier that was renamed to F11-P everywhere else in the revision that introduced F11-D — which places it as text carried forward mechanically rather than re-derived.

The earlier architecture had licensed additivity explicitly and conditionally: "F00/F10 individual executions do not overlap; single-flight prevents transport/compute overlap **when component additivity is required**". It could say that because F10's dispatch was serial by construction. The revision that made F10's dispatch concurrent — so that backend serialization is matched across both levels of Factor A, and the waiting is booked in Q_backend at both — removed the precondition, in the same revision that moved F10 into the additive group. Nothing connected the two changes.

## Consequences

- F10 joins D0, F00 and F01 in interval/critical-path accounting. F11-P and F11-D at M=1 remain additive. The rule to apply to a future cell is the discriminator, not the list: additive only where a cohort's work takes one serial path.
- Envelope-granularity stages are counted once per cohort. At A=on a cohort's `A_pack` and its proxy-to-adapter transfer happen once but appear in all B per-request records, so any cohort-level quantity that summed per-record values would multiply a one-off cost by B.
- The per-request residual is unaffected. It is computed for every cell and it does fail when a stage is mis-attributed; nothing about this correction weakens it.
- The cohort-level test that was in place could not fail. It unioned the members' in-flight intervals and compared the union against the cohort makespan, but a release barrier makes those intervals overlap, so the union is always one contiguous span identically equal to the makespan — uncovered time measured zero in every cohort of every delivered run. It is replaced by a test of the mechanism this ADR turns on: the executions must not overlap, because that serialization is what licenses booking the wait in Q_backend. A second model instance, or a scheduler that stopped serializing, is invisible to the execution count, the batch histogram, the attested membership and the per-request residual, and visible only here.
- Cohort intervals are still measured and reported. They are informative about overlap structure; they simply make no claim.
