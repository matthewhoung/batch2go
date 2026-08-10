package testkit

import (
	"fmt"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// The defect fixtures. Each plants exactly one fault into an otherwise
// well-formed bundle, so a failing verdict points at that fault and nothing
// else. Two of them — the own-uid echo and the coalesced singles — are permanent:
// they guard the silent failure modes this design went looking for and found,
// and a future change that quietly reintroduces either must break a test rather
// than a dataset.

// WithMissingTimestamp removes a timestamp the cell's topology requires.
//
// The point is the contrast with absence by topology: D0 legitimately has no
// t_proxy_recv, and that must not be a defect, while a stage the topology does
// have going missing must be.
func (b Bundle) WithMissingTimestamp(emitter identity.Emitter, stage events.Stage) (Bundle, error) {
	out := b.Clone()
	req := out.FirstRequest()
	i, err := out.find(emitter, req)
	if err != nil {
		return Bundle{}, err
	}

	rec := out.Records[i].Record
	if !rec.Presence.Has(stage) {
		return Bundle{}, fmt.Errorf("testkit: %s does not carry %s, so it cannot go missing there", emitter, stage)
	}
	rec.Presence = rec.Presence.Without(stage)
	rec.TS[stage] = 0
	out.Records[i].Record = rec
	return out, nil
}

// WithUnexpectedStage adds a stage the cell's topology does not have — a proxy
// timestamp on the direct path, for instance, which no process could have
// legitimately observed.
func (b Bundle) WithUnexpectedStage(stage events.Stage, at int64) (Bundle, error) {
	out := b.Clone()
	req := out.FirstRequest()
	i, err := out.find(identity.EmitterLoadGen, req)
	if err != nil {
		return Bundle{}, err
	}
	rec := out.Records[i].Record
	rec.SetStage(stage, at)
	out.Records[i].Record = rec
	return out, nil
}

// mutateMembership rewrites the attestation on every record that carries one for
// a request.
//
// Every source is rewritten together on purpose: changing one would plant a
// disagreement between sources, which is a different defect. These fixtures are
// meant to test what the attestation says, not whether its observers agree.
func (b Bundle) mutateMembership(req identity.LogicalRequest, fn func([]identity.UID) []identity.UID) (Bundle, error) {
	out := b.Clone()
	var touched int
	for i, d := range out.Records {
		if d.Record.Request() != req || len(d.Record.MembershipUIDs()) == 0 {
			continue
		}
		rec := d.Record
		rec.SetMembership(fn(append([]identity.UID(nil), rec.MembershipUIDs()...)))
		out.Records[i].Record = rec
		touched++
	}
	if touched == 0 {
		return Bundle{}, fmt.Errorf("testkit: no record carries membership for %v", req)
	}
	return out, nil
}

// WithForeignCohortMembership makes an execution attest a member from a
// different cohort — evidence that the cohort accounting and the physical
// execution disagree.
func (b Bundle) WithForeignCohortMembership() (Bundle, error) {
	req := b.FirstRequest()
	foreign := identity.LogicalRequest{Cohort: req.Cohort + 999, Ordinal: 0}
	return b.mutateMembership(req, func(uids []identity.UID) []identity.UID {
		uids[len(uids)-1] = foreign.UID()
		return uids
	})
}

// WithOwnUIDEcho replaces every execution's attested set with just the
// requesting member's own uid.
//
// This is the naive-echo failure at the record level: Triton scatters a batched
// output back per request, so a model that merely echoes its uid input hands
// each request its own uid and nothing else. The result has the shape of
// membership evidence and the content of none, and at V=on it must fail
// (ADR-0007).
func (b Bundle) WithOwnUIDEcho() Bundle {
	out := b.Clone()
	for i, d := range out.Records {
		if len(d.Record.MembershipUIDs()) == 0 {
			continue
		}
		rec := d.Record
		rec.SetMembership([]identity.UID{rec.Request().UID()})
		out.Records[i].Record = rec
	}
	return out
}

// WithCoalescedSingles reports the backend having merged two single-request
// executions into one batched execution.
//
// On the unbatched entry this must never happen, and if it did, a V=off cell
// would have run a batched execution while still being labeled V=off. That is
// the failure mode the contamination check exists for (M1 §2.1).
func (b Bundle) WithCoalescedSingles() Bundle {
	out := b.Clone()
	if out.Expectation.ExecutionCountDelta >= 1 {
		out.Expectation.ExecutionCountDelta--
	}
	out.Expectation.BatchSizeHistogram = map[uint64]uint64{
		1: uint64(out.Expectation.Executions) - 2,
		2: 1,
	}
	return out
}

// WithCrossClockDomain moves one process's records into a different clock
// domain, so subtracting across the two would be illegitimate.
//
// CLOCK_MONOTONIC restarts at boot, so timestamps from two domains are unrelated
// numbers. Subtracting them yields a duration that looks entirely plausible,
// which is exactly why this has to be caught structurally rather than by
// noticing an odd value.
func (b Bundle) WithCrossClockDomain(emitter identity.Emitter) (Bundle, error) {
	out := b.Clone()
	var touched int
	for i, d := range out.Records {
		if d.Record.Emitter != emitter {
			continue
		}
		out.Records[i].Header.ClockDomain = "cd-otherboot0000000000"
		touched++
	}
	if touched == 0 {
		return Bundle{}, fmt.Errorf("testkit: no %s records to move to another clock domain", emitter)
	}
	return out, nil
}

// WithNonMonotonicPath makes a stage end before it starts, which means the
// schema's ownership or the clock assumption is wrong rather than that a
// duration is small.
func (b Bundle) WithNonMonotonicPath(emitter identity.Emitter, stage events.Stage, delta int64) (Bundle, error) {
	out := b.Clone()
	req := out.FirstRequest()
	i, err := out.find(emitter, req)
	if err != nil {
		return Bundle{}, err
	}
	rec := out.Records[i].Record
	ts, ok := rec.Stage(stage)
	if !ok {
		return Bundle{}, fmt.Errorf("testkit: %s does not carry %s", emitter, stage)
	}
	rec.SetStage(stage, ts-delta)
	out.Records[i].Record = rec
	return out, nil
}

// WithDroppedRecords reports that the instrument lost evidence.
func (b Bundle) WithDroppedRecords(n uint64) Bundle {
	out := b.Clone()
	out.Expectation.DroppedRecords = n
	return out
}

// WithMissingRequest deletes every record for one logical request, so the
// request vanishes from the evidence entirely.
func (b Bundle) WithMissingRequest() Bundle {
	out := b.Clone()
	req := out.FirstRequest()

	kept := make([]events.Decoded, 0, len(out.Records))
	for _, d := range out.Records {
		if d.Record.Request() == req {
			continue
		}
		kept = append(kept, d)
	}
	out.Records = kept
	return out
}

// WithTruncatedMembership reports an execution that claimed more members than
// the record could hold, so the evidence is incomplete rather than smaller.
func (b Bundle) WithTruncatedMembership() (Bundle, error) {
	out := b.Clone()
	req := out.FirstRequest()
	var touched int
	for i, d := range out.Records {
		if d.Record.Request() != req || len(d.Record.MembershipUIDs()) == 0 {
			continue
		}
		rec := d.Record
		rec.MembershipCount = uint32(len(rec.MembershipUIDs())) + 7
		out.Records[i].Record = rec
		touched++
	}
	if touched == 0 {
		return Bundle{}, fmt.Errorf("testkit: no record carries membership for %v", req)
	}
	return out, nil
}

// WithAdapterJoin makes every member of a cohort dispatch at the same instant —
// the last arrival's — which is exactly what an adapter-side cohort barrier
// would produce.
//
// This is the fixture behind the "no waiting at A=off" assertion. An adapter
// that joined would inject formation wait into the OFF/OFF baseline and
// contaminate the aggregation effect, and the failure is invisible in aggregate
// timing: the cohort still completes, just with its wait moved somewhere the
// cycle model books differently (ADR-0001).
func (b Bundle) WithAdapterJoin() Bundle {
	out := b.Clone()

	// The instant the whole cohort would be released: its last arrival.
	release := map[identity.CohortID]int64{}
	for _, d := range out.Records {
		if d.Record.Emitter != identity.EmitterAdapter {
			continue
		}
		if ts, ok := d.Record.Stage(events.StageAdapterRecv); ok && ts > release[d.Record.Cohort] {
			release[d.Record.Cohort] = ts
		}
	}

	for i, d := range out.Records {
		if d.Record.Emitter != identity.EmitterAdapter {
			continue
		}
		rec := d.Record
		if _, ok := rec.Stage(events.StageAdapterDispatch); !ok {
			continue
		}
		rec.SetStage(events.StageAdapterDispatch, release[rec.Cohort])
		out.Records[i].Record = rec
	}
	return out
}

// WithSealFromWrongEmitter moves t_cohort_seal onto the proxy's record.
//
// At A=off the load generator owns the seal and the proxy emits none: a proxy
// that sealed anything there would be joining, which is what ADR-0001 forbids at
// the OFF/OFF baseline. The value itself is unchanged, so only an ownership
// check can catch this — every timestamp is present, ordered, and in the right
// clock domain.
func (b Bundle) WithSealFromWrongEmitter() (Bundle, error) {
	out := b.Clone()
	req := out.FirstRequest()

	owner, err := out.find(identity.EmitterLoadGen, req)
	if err != nil {
		return Bundle{}, err
	}
	seal, ok := out.Records[owner].Record.Stage(events.StageCohortSeal)
	if !ok {
		return Bundle{}, fmt.Errorf("testkit: the load generator does not carry a cohort seal to move")
	}

	usurper, err := out.find(identity.EmitterProxy, req)
	if err != nil {
		return Bundle{}, err
	}

	ownerRec := out.Records[owner].Record
	ownerRec.Presence = ownerRec.Presence.Without(events.StageCohortSeal)
	ownerRec.TS[events.StageCohortSeal] = 0
	out.Records[owner].Record = ownerRec

	usurperRec := out.Records[usurper].Record
	usurperRec.SetStage(events.StageCohortSeal, seal)
	out.Records[usurper].Record = usurperRec
	return out, nil
}

// WithDisagreeingMembershipSources makes two processes attest different uid sets
// for the same execution.
//
// On the shared path the client and the adapter each observe the model's
// attestation independently, and the proxy maps results back to members in
// between. A mapping that handed a member the wrong execution's set would be
// internally consistent at every process — only comparing the two sources
// catches it.
func (b Bundle) WithDisagreeingMembershipSources() (Bundle, error) {
	out := b.Clone()
	req := out.FirstRequest()

	i, err := out.find(identity.EmitterAdapter, req)
	if err != nil {
		return Bundle{}, err
	}
	rec := out.Records[i].Record
	if len(rec.MembershipUIDs()) == 0 {
		return Bundle{}, fmt.Errorf("testkit: the adapter attested nothing to disagree with")
	}

	// Attest a neighbouring cohort's set: a plausible mapping error, not noise.
	uids := make([]identity.UID, 0, len(rec.MembershipUIDs()))
	for _, uid := range rec.MembershipUIDs() {
		member := uid.LogicalRequest()
		uids = append(uids, identity.LogicalRequest{Cohort: member.Cohort + 1, Ordinal: member.Ordinal}.UID())
	}
	rec.SetMembership(uids)
	out.Records[i].Record = rec
	return out, nil
}

// WithMembershipOnlyFromOneSource drops every process's attestation but one, so
// a test can establish that a single source is still judged rather than skipped.
func (b Bundle) WithMembershipOnlyFromOneSource(keep identity.Emitter) Bundle {
	out := b.Clone()
	for i, d := range out.Records {
		if d.Record.Emitter == keep || len(d.Record.MembershipUIDs()) == 0 {
			continue
		}
		rec := d.Record
		rec.SetMembership(nil)
		out.Records[i].Record = rec
	}
	return out
}

// WithOverlappingExecutions makes a cohort's model executions run in parallel.
//
// This is what a second model instance, or a scheduler that stopped serializing,
// would look like. It matters because it is invisible to every other check: the
// execution count, the batch-size histogram, the attested membership and the
// per-request residual are all unchanged. Only the cohort-level layout betrays
// it — and the wait the cycle model books in Q_backend would be describing a
// queue that was not there (M1 §2.2).
func (b Bundle) WithOverlappingExecutions() Bundle {
	out := b.Clone()

	// Give every member of the first cohort the same execution window.
	req := out.FirstRequest()
	var start, end int64
	for _, d := range out.Records {
		if d.Record.Request() != req {
			continue
		}
		if s, ok := d.Record.Stage(events.StageComputeStart); ok {
			start = s
		}
		if e, ok := d.Record.Stage(events.StageComputeEnd); ok {
			end = e
		}
	}
	for i, d := range out.Records {
		if d.Record.Cohort != req.Cohort {
			continue
		}
		rec := d.Record
		if _, ok := rec.Stage(events.StageComputeStart); !ok {
			continue
		}
		rec.SetStage(events.StageComputeStart, start)
		rec.SetStage(events.StageComputeEnd, end)
		out.Records[i].Record = rec
	}
	return out
}
