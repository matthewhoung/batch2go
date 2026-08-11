# Spec 0002 — Envelope aggregation: F10 end to end

> Status: ready for implementation · 2026-08-11
> The first horizontal extension of the walking skeleton ([spec 0001](0001-walking-skeleton-d0-f00.md)). Vocabulary: [CONTEXT.md](../../CONTEXT.md); decisions: [docs/adr/](../adr/).

## Problem Statement

Factor A is the question the project exists to separate from the other four batching mechanisms: whether a cohort's B logical requests travel in **one** transport envelope or in **B** of them. Spec 0001 delivered the A=OFF side of that contrast and nothing of the A=ON side. Every A=ON cell — F10, F11-D, F11-P — is unimplemented, so the factor has one level.

The danger in adding the second level is that A=ON is easy to *appear* to implement. A proxy that sealed each member separately and sent B envelopes would produce, for every check the walking skeleton owns, exactly what a correct F10 run produces: n=B executions, J=B distinct executions, every execution of batch size 1, a clean coalescing histogram, correct self-attested membership, a matching presence mask, and a conservation residual within tolerance. It would carry F10's label on F00's behaviour, and no assertion in the repository would notice. The aggregation contrast would then be a comparison of one implementation against itself.

Three further hazards are specific to this slice, and each is invisible while every envelope carries exactly one member:

- **Formation is synchronization.** Envelope aggregation cannot begin until a cohort's members have arrived, so the proxy waits, and that wait is inside the measured cycle at A=ON in a way it structurally cannot be at A=OFF ([ADR-0010](../adr/0010-proxy-cohort-formation-at-envelope-aggregation.md)). A wait that is real and unmeasured is an unattributed constituent of the effect.
- **Cohort-granularity quantities appear B times.** One envelope's packing cost and one envelope's transfer happen once per cohort but land in all B per-member records. Any cohort-level arithmetic that summed per-record values would multiply a one-off cost by B ([ADR-0009](../adr/0009-f10-conservation-is-interval-not-additive.md)).
- **A cohort can fail to form.** Losing one member costs one request at A=OFF and B at A=ON, and the probability rises with payload — treatment-correlated missingness concentrated where the crossover claim lives. It has to be bounded, named, and reported rather than discovered in the data.

## Solution

Build F10 — A=ON, V=OFF — end to end at cohort size B=4 on the local validation environment, and build the assertions that could have caught a fake.

The proxy becomes what it is at A=ON: it holds a cohort while it assembles, seals it with its own clock, and sends one envelope. The adapter fans that envelope out to B concurrent single-item backend requests through the same executor F00 uses. The validator gains the **manipulation check for Factor A** — the cardinality and agreement assertions that separate one envelope from B — plus the fan-out judgement that separates a concurrent release from a serial one, and the cohort-level accounting that follows from F10's interval classification.

This slice keeps both permanent test seams from spec 0001 and states them for F10:

1. **Live seam — manifest → validated run bundle:** an F10 manifest fully determines the run, including the two parameters formation introduces; the run executes against real Triton through proxy and adapter; acceptance asserts on the resulting bundle — one envelope per cohort carrying exactly that cohort's members, B backend requests, n=B executions all of shape `[1,…]`, self-attested membership, the A=ON presence mask with the seal owned by the proxy, dispatch skew within its declared bound, and interval conservation.
2. **Offline seam — event records → validator verdict:** the validator is exercised on fixtures the live stack cannot produce — an A=ON cohort with injected known delays whose cohort residual must come out at exactly the injected unnamed time, and deliberately defective bundles it must reject: members carrying different envelope ids, members carrying different seals or different envelope-granularity timestamps, a fan-out whose submissions did not overlap, an adapter whose reported skew disagrees with its own dispatch timestamps, and a cohort that never formed.

The second seam carries a lesson from spec 0001's fixtures, which modelled the backend as fully parallel — contradicting the serialization the design declares — and went unnoticed until a check was written that could fail. An A=ON fixture that gave each member its own seal would pass every F10 assertion in this spec while describing evidence no proxy could emit, so the fixture builder must produce cohort-granularity evidence and assert that it did.

## User Stories

1. As the experimenter, I want the client protocol to carry no cohort seal and its tag reserved, so that the seal has exactly one owner at each factor level and no process claims a stage it does not own.
2. As the experimenter, I want an aggregate envelope to carry each of its cohort's ordinals exactly once and in canonical order, so that a cohort is a membership fact rather than a count, and the adapter's fan-out order reflects release cost rather than arrival order.
3. As the analyst, I want `auxiliary_bytes` to measure the envelope's framing overhead and the run bundle to name the envelope contract, so that the per-envelope cost Factor A changes is measured rather than restated, and an archived run says which protocol carried its payloads.
4. As the analyst, I want the adapter's dispatch evidence — size, skew, CPU and the scope of that CPU — in the records and the archive, with a measured zero distinguishable from a never-measured value, so that the evidence the fan-out cells are judged on survives the run.
5. As the artifact reviewer, I want the acceptance suite declared in a file the runner reads, so that adding a cell is adding a manifest and its expected evidence rather than new machinery.
6. As the experimenter, I want an F10 manifest at B=4 declaring n=B, J=B and batch size 1, and supplying the formation deadline and the dispatch-skew bound, so that no F10 parameter is decided by a code default.
7. As the experimenter, I want a manifest whose formation deadline is not strictly shorter than its request deadline refused with the reason, so that a cohort is never torn down by client cancellation through a path that has no name.
8. As the experimenter, I want a data-plane service that fails to start or dies mid-run to fail the run naming which one, so that a missing process produces a verdict rather than a hang.
9. As the experimenter, I want the proxy to collect a cohort by count against the declared B, keyed by cohort id with each ordinal admitted once, seal it with its own clock, and send exactly one envelope, so that envelope aggregation is what distinguishes F10 from F00.
10. As the analyst, I want every member of a cohort to observe the same seal, the same envelope id and the same envelope-granularity timestamps, so that cohort-level quantities are recoverable from any member's record and disagreement is detectable.
11. As the experimenter, I want each caller to receive its own member's result even when the backend returns results in a different order, so that the response fan-out maps by identity and never by position.
12. As the analyst, I want formation wait recorded per member and the seal written by the proxy and by nobody else, so that the wait A=ON introduces is measured where it happens rather than assumed absent.
13. As the experimenter, I want a member arriving twice, or with an ordinal outside the cohort, to fail the cohort rather than be absorbed, so that a malformed cohort cannot be shipped as a well-formed one.
14. As the experimenter, I want B−1 members to fail whole at a bounded, manifest-configured time with no envelope reaching the backend, so that a short envelope never silently changes the B the conformance gate fixes.
15. As the experimenter, I want a member arriving after its cohort failed to receive that same named failure rather than open a fresh formation, so that a late arrival cannot resurrect a cohort that has already been judged.
16. As the experimenter, I want a formation failure scoped to its cohort — the run continues, the verdict is not green, and shutdown never waits on an unformed cohort — so that treatment-correlated missingness is reported rather than converted into a hang or a lost run.
17. As the analyst, I want the absent member's timeout preserved in the archive alongside the held members' errors, so that the only diagnostic information the failure contains survives it.
18. As the future implementer, I want the offline fixture builder to give an A=ON cohort one seal, one envelope id and one set of envelope-granularity timestamps — and a V=ON cohort one shared execution window — and to assert those properties of what it produced, so that offline F10 assertions are tested against evidence a proxy could actually emit.
19. As the analyst, I want envelope-granularity stages counted once per cohort, with a test showing that the naive per-member sum would differ by a factor of B, so that a one-off cost is never multiplied by cohort size.
20. As the analyst, I want formation wait reported at both granularities — per member inside the chain, and once per cohort as the earliest arrival to the seal — with the relationship between them stated, so that their agreement is never mistaken for corroboration.
21. As the experimenter, I want an injected-delay F10 fixture's cohort residual to come out at exactly the injected unnamed time, and the execution-serialization check to hold for F10 as it does for F00, so that F10's interval accounting is validated before it testifies.
22. As the experimenter, I want a cohort whose members carry different envelope ids to fail, naming the cardinality, so that B envelopes wearing F10's label are caught by the assertion that exists for exactly that.
23. As the experimenter, I want a cohort whose members carry different seals or different envelope-granularity timestamps to fail, naming the disagreement, so that per-member sealing is distinguishable from cohort sealing.
24. As the experimenter, I want a cohort that failed to form to produce its own named defect, displacing rather than adding to the missing-timestamp symptoms it would otherwise generate, so that a diagnosis is not buried under its own consequences.
25. As the experimenter, I want dispatch skew beyond the manifest's bound to fail, naming the bound and the observed value, so that the fan-out is judged against a declared expectation rather than described.
26. As the experimenter, I want a fan-out whose submissions did not overlap to fail distinctly from an over-bound skew, and the adapter's reported skew cross-checked against the recorded dispatch timestamps, so that a serial fan-out cannot pass on the adapter's own word.
27. As the analyst, I want the fan-out evidence carried in the verdict as structured fields, and a process-scoped CPU value never admitted into a comparison across Factor A levels, so that a number whose definition changes between conditions cannot enter a contrast between them.
28. As the artifact reviewer, I want one command to run the F10 manifest against real Triton and another to judge the resulting bundle from the archive alone, with the presence mask matching the A=ON topology and the seal owned by the proxy, so that the cell's claimed properties are checkable without reading internals.
29. As the analyst, I want the adapter to record its own transport and model configuration into the bundle, and one command to compare an F00 bundle with an F10 bundle by a digest over the properties that must match, so that "the two cells differ in transport aggregation and in nothing else" is an assertion over archives rather than an article of faith.

### Story → assertion → owner

Every story names one assertion, and every assertion is owned by exactly one ticket. A ticket may own several; no assertion is owned twice.

| Story | Assertion | Ticket |
|---|---|---|
| 1 | `envelope.client_carries_no_seal` | 03 |
| 2 | `envelope.cohort_membership_and_order` | 03 |
| 3 | `envelope.byte_accounting_and_declared_contract` | 03 |
| 4 | `events.dispatch_evidence_archived` | 04 |
| 5 | `suite.contracts_are_declared` | 05 |
| 6 | `manifest.f10_parameters` | 07 |
| 7 | `manifest.formation_deadline_ordering` | 07 |
| 8 | `runner.service_death_fails_the_run` | 07 |
| 9 | `proxy.one_sealed_envelope_per_cohort` | 08 |
| 10 | `proxy.members_share_envelope_stages` | 08 |
| 11 | `proxy.results_map_by_identity` | 08 |
| 12 | `proxy.formation_wait_measured` | 08 |
| 13 | `proxy.malformed_cohort_refused` | 08 |
| 14 | `proxy.incomplete_cohort_fails_whole` | 09 |
| 15 | `proxy.late_arrival_gets_the_same_failure` | 09 |
| 16 | `proxy.formation_failure_scoped_to_cohort` | 09 |
| 17 | `events.absent_member_timeout_survives` | 09 |
| 18 | `testkit.fixtures_carry_cohort_granularity` | 10 |
| 19 | `validate.envelope_stages_counted_once` | 11 |
| 20 | `validate.formation_wait_at_both_granularities` | 11 |
| 21 | `validate.f10_interval_accounting` | 11 |
| 22 | `validate.aggregation_cardinality` | 12 |
| 23 | `validate.envelope_stage_agreement` | 12 |
| 24 | `validate.formation_failure_named` | 12 |
| 25 | `validate.dispatch_skew_bounded` | 13 |
| 26 | `validate.fan_out_overlapped` | 13 |
| 27 | `validate.fan_out_evidence_structured` | 13 |
| 28 | `contract.f10_manifest_to_validated_bundle` | 14 |
| 29 | `contract.f00_and_f10_same_implementation` | 15 |

Tickets 01 and 02 carry no story: they are the groundwork this slice was built on — one authority for which cells run, and an offline harness for the two packages F10 lands in — and they assert nothing about F10 that a story above does not restate.

## Implementation Decisions

- **The proxy synchronizes when it aggregates.** At A=ON a cohort is a runtime object, held by the proxy while it assembles; at A=OFF it remains an accounting label joined offline. `internal/proxy` owns accumulation, completeness detection, the seal timestamp, and the incomplete-cohort policy ([ADR-0010](../adr/0010-proxy-cohort-formation-at-envelope-aggregation.md), amending the generality of [ADR-0001](../adr/0001-single-release-barrier-at-loadgen.md), whose decision about the adapter at A=OFF is unchanged).
- **Completeness is by declared count.** Keyed by cohort id, each ordinal admitted once, against the B the manifest declares. The proxy never infers B from what arrives.
- **A cohort that cannot form fails whole, and fails visibly.** No short envelope is ever sent: B is fixed by the conformance gate and never by what a run happened to receive, so shipping B−1 members would silently change the quantity the design holds constant. The deadline runs from the **first member's arrival at the proxy** — measuring from the scheduled release would fold the first hop's transfer into a bound meant to constrain assembly, and the bound would then drift with payload size. It is strictly shorter than the request deadline, enforced by the manifest.
- **The failure is scoped to the cohort, not the run.** Members are recorded failed, the run continues, and the run's verdict is not green. The member that never arrived keeps its load-generator timeout; the members the proxy held record an error, because they did not themselves time out. The cause is named once at cohort level rather than B times.
- **Formation wait is a declared constituent of the aggregation contrast.** It enters the measured cycle at A=ON and structurally cannot at A=OFF, because at A=ON it falls between the client send and the client completion and at A=OFF it precedes the send. The contrast is a contrast of measured throughputs, not a sum of stages: the declaration states what the number holds, and the magnitude is an experimental result described from the data ([ADR-0010](../adr/0010-proxy-cohort-formation-at-envelope-aggregation.md)).
- **F10 is interval-accounted.** B requests become B executions, dispatched concurrently and serializing on the single model instance — one envelope, B serial paths through the backend. Envelope-granularity stages are counted once per cohort ([ADR-0009](../adr/0009-f10-conservation-is-interval-not-additive.md)).
- **Exact-B conformance counts cohorts released at the barrier**, not cohorts that reached executor release: a cohort that failed to form never reaches the executor, and the narrower denominator would exclude exactly the failures that should lower the rate.
- **F10 and F00 resolve to the same implementation.** Same executor constructor, same downstream client and channel configuration, same model entry, same graph digest — the individual executor over the unbatched entry, exactly as F00 uses it. What differs is how many logical requests one envelope carries and how many members one dispatch releases. The adapter records its own configuration into the bundle so this is checkable from archives rather than from the runner's intent.
- **Two new manifest parameters:** the cohort formation deadline and the bound on dispatch skew. Both are experimental quantities and therefore manifest constants with no code default, like every other number that affects what is measured.
- **The envelope protocol is amended at v1, not versioned forward.** The client's cohort-seal tag is reserved rather than freed, so it cannot acquire a second meaning while the version stands ([ADR-0003](../adr/0003-canonical-envelope-serialization.md) pins the serialization the amendment leaves untouched).
- **Fixed parameters:** B=4, one payload level, one κ level, the local validation environment only. Timing on this environment is a correctness check, never a performance claim.

## Testing Decisions

- **A good test asserts external behavior at a seam** — the contents of a validated bundle, a validator verdict, or a response at the proxy's own boundary — never internal state, goroutine structure, or private functions.
- **Live seam (acceptance):** the F10 contract test joins D0 and F00 in the declared acceptance suite — manifest in, validated bundle out, against real Triton via the compose stack. Per cohort: one envelope in, B backend requests out, n=B executions all of shape `[1,…]`, J=B distinct executions, self-attested membership consistent with cohort labels, presence mask exactly matching the A=ON topology with the seal owned by the proxy, dispatch skew within the manifest's bound, contamination check clean, interval conservation within tolerance. The same command runs the whole suite.
- **Offline seam (validator):** fixture bundles the live stack cannot produce. (a) An A=ON cohort with injected known delays, whose per-stage recovery and cohort residual the self-test must reproduce before any live F10 conservation number is trusted. (b) Defective bundles, each of which must produce a failing verdict naming the defect: members carrying different envelope ids; members carrying different seals; members carrying different envelope-granularity timestamps; a fan-out whose submissions did not overlap; an adapter whose reported skew disagrees with the dispatch timestamps beside it; a cohort that never formed. Each new defect has a fixture that fails it and a control that passes.
- **The proxy is verifiable without Triton.** Cohort formation, sealing, response fan-out and the incomplete-cohort policy are asserted at the proxy's seams against a fake backend, under the race detector — the concurrency this slice introduces is the proxy's, and it must not need a GPU to be tested.
- **Permanent counter-fixtures:** the per-member-sealing bundle and the serial fan-out stay in the suite forever. They are the two ways F10 can be faked while passing every check the walking skeleton owns.
- **The fixture builder asserts its own output.** An A=ON fixture that gave each member its own seal would satisfy every F10 assertion above while describing evidence no proxy could emit; the builder therefore checks that a cohort it produced shares one seal, one envelope id and one set of envelope-granularity timestamps.
- **Prior art in this repository:** spec 0001's conservation self-test and defect fixtures, extended rather than replaced; the A=OFF fixtures and their recovered delays are unchanged.
- Unit tests (formation state machine, membership validation, codec round-trips) exist beneath the seams but are not acceptance criteria.

## Out of Scope

- **Cells:** F01, F11-D, F11-P and F00-seq. F01 and F11-D need the dynamic model entry and the scheduler-formed batching this slice does not build; F11-P needs the preformed-tensor executor; F00-seq needs serial release. They are specs 0003 and 0004.
- **Gates:** the exact-B conformance gate and its 16 → 8 → 4 fallback ladder. This slice fixes the denominator that gate will count against ([ADR-0010](../adr/0010-proxy-cohort-formation-at-envelope-aggregation.md)) and implements no part of the gate itself, because conformance is a property of the V=ON cells and none exist yet. Gate G1 — the full contract matrix across B ∈ {4, 16} — is spec 0004; the overhead-calibration gate remains a later slice.
- **The second cohort size.** B=16 is where the treatment-correlated formation-failure risk this spec declares actually bites; it arrives with the contract matrix.
- **Overhead-bound freezing.** Tracing-mode and GC contamination bounds are recorded from every run and calibrated later; the dispatch-skew bound this slice adds is a correctness bound on the fan-out, not a contamination bound.
- **A per-dispatch CPU measurement.** The adapter's CPU is process-scoped and stays so: Go's scheduler migrates goroutines across threads, so no available primitive bounds one dispatch. The scope travels with the value and the validator refuses to compare it across Factor A levels; a comparable measurement, if one appears, coexists with this one rather than replacing it.
- **Real CV models, gated membership graph surgery, multi-flight and open-loop arrivals, the observability stack, AWS and Terraform** — unchanged from spec 0001's exclusions, and each has its own slice.
- **Any performance claim.** F10 produces its first end-to-end timings in this slice and none of them are results. No number leaves this repository before the preregistered confirmatory runs.

## Further Notes

- Planned successor specs are unchanged: 0003 scheduler-formed batching (F01/F11-D: dynamic entry, conformance recording); 0004 contract-matrix completion (F11-P, F00-seq, gate G1 across B ∈ {4, 16}); 0005 AWS infrastructure; 0006 observability.
- This slice amends three things spec 0001 delivered, rather than adding beside them: the envelope protocol at v1, the record vocabulary that gained the fan-out evidence, and the acceptance suite that became a declared list. Each is a numbered story above and each is owned by a ticket, because a change to a delivered contract is a change someone has to have asserted.
- The vocabulary is [CONTEXT.md](../../CONTEXT.md)'s. In particular *cohort*, *envelope*, *envelope aggregation*, *formation (W_form)*, *release barrier*, *membership evidence* and *exact-B conformance* are used only as defined there; where this spec needs a distinction the glossary does not draw, it draws it explicitly rather than overloading a term.
