---
status: accepted
date: 2026-08-10
amends: 0001
---

# The proxy synchronizes when it aggregates

ADR-0001 settled where cohorts may be joined: the load generator's release barrier is the synchronization point, and the adapter never joins under A=OFF, because an adapter-side join would inject formation wait into the OFF/OFF baseline and contaminate the aggregation effect. That reasoning is untouched. But the sentence recording it — "the load generator's release barrier is the only runtime synchronization point" — was written as a general claim while the decision it records is about the adapter at A=OFF, and it was then repeated as general law in the architecture, the package map, a delivered spec and two source comments.

It is not general. Envelope aggregation is the A=ON treatment and it happens at the proxy: a cohort cannot be packed into one envelope before its members have arrived. F10 and F11-D are identification cells and both are A=ON, so under the unqualified reading the design contradicts itself. The adapter's response side rejoins the members too, because one envelope carries them all and its send timestamp is envelope-granularity.

So: the release barrier is the only synchronization point **at the load generator**, and the only one anywhere in **A=OFF** cells. At A=ON the proxy synchronizes by construction, and a cohort is a runtime object there — held by the proxy while it assembles — where at A=OFF it remains an accounting label joined offline.

## Cohort formation at the proxy

**Completeness** is by count against the declared cohort size, keyed by cohort id, with each member's ordinal admitted once. The proxy knows B from the manifest; it does not infer it from what arrives.

**A cohort that cannot form fails whole, and fails visibly.** No short envelope is ever sent. B is fixed by the conformance gate and never by what a run happened to receive, so shipping B−1 members would silently change the quantity the whole design holds constant — a wrong-model-class failure, not a degradation. The formation deadline runs from the **first member's arrival at the proxy**, which is when formation actually begins and is the only cohort-relative instant the proxy can observe; measuring from the scheduled release would fold the first hop's transfer into a bound meant to constrain assembly, and the bound would then drift with payload size.

The failure is scoped to the cohort, not the run: members are recorded failed, the run continues, and the run's verdict is not green. The member that never arrived keeps the timeout status the load generator recorded for it — that is the only diagnostic information the failure contains — and the members the proxy was holding record an error, because they did not themselves time out. The cause is named once at cohort level rather than B times.

Because the formation deadline shares a context with the client's request deadline, it must be strictly shorter than it. Otherwise the client cancels first, the held members' contexts die, and the cohort is torn down by cancellation through a path that has no name — leaving the formation-failure diagnosis unreachable.

## Consequences

- `internal/proxy` owns cohort accumulation, completeness detection, the seal timestamp, and the incomplete-cohort policy. The package map had dissolved that ownership on the premise this ADR retracts for A=ON, leaving it unassigned.
- W_form becomes a measured stage at A=ON rather than a controlled constant, and it is the proxy's — the interval from a member's arrival to the seal. The interval between the scheduled release and the barrier at A=OFF is a different quantity with a different name; it is the record of release jitter, and calling both W_form put two unrelated numbers in one archive column.
- **Formation wait enters the cycle at A=ON and structurally cannot at A=OFF**, because at A=ON it falls between the client send and the client completion and at A=OFF it precedes the send. The aggregation contrast is a contrast of measured throughputs, not a sum of stages, so there is nothing to add to it or remove from it: what changes is what the measured cycle at A=ON contains. Formation wait is therefore a declared constituent of that contrast, alongside the RPC multiplicity, the packing and unpacking, and the pipelining loss the design already names — a statement about what the number holds, not a term in an arithmetic. Its magnitude is an experimental result and is described from the data; the declaration that it is in there is made in advance, because a reader cannot recover it from the number afterwards.
- Exact-B conformance counts cohorts released at the barrier, not cohorts that reached executor release. A cohort that failed to form never reaches the executor, so the earlier denominator would have excluded exactly the failures that should lower the rate.
- A cohort failing to form costs one request at A=OFF and B at A=ON, and the probability rises with payload — treatment-correlated missingness, concentrated in the regime where the crossover claim lives. It is declared here so that it is bounded and reported rather than discovered in the data.
- ADR-0001 stands as written. Its reasoning about the adapter is correct and its decision is unchanged; this amends only the generality of how it was stated.
