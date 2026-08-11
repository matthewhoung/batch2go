package proxy

// Cohort formation is what the proxy does at A=on and structurally does not do
// at A=off. A cohort here is a runtime object — held while its members arrive,
// sealed by the proxy's own clock, packed into one envelope — where at A=off it
// stays an accounting label joined offline (ADR-0010).
//
// Two rules shape everything below. Completeness is by count against the B the
// manifest declared, keyed by cohort id, each ordinal admitted once; the proxy
// never infers B from what arrives. And a cohort that cannot form fails whole:
// no short envelope is ever sent, because B is fixed by the conformance gate and
// shipping B−1 members would silently change the quantity the whole design holds
// constant.
//
// What lives where: a formation carries only what one cohort knows about itself,
// so anything needing the registry, the clock, the builder or the backend is a
// method on the Service instead. That is why join and sealAndSend hang off the
// service while assembled and await do not — a formation cannot admit a member,
// because admitting one means deciding whether its cohort id is already spoken
// for, and only the registry knows that.

import (
	"context"
	"fmt"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// slot is one ordinal's place in a forming cohort. admitted is separate from the
// payload because an admitted member is a membership fact, and testing the
// payload for emptiness instead would make "did this ordinal arrive" depend on
// what it carried.
type slot struct {
	payload  []byte
	admitted bool
}

// formation is one cohort being assembled at the proxy.
type formation struct {
	cohort  identity.CohortID
	targetB int

	// done closes exactly once, when the cohort reaches a terminal state: sealed
	// and answered, or failed. Every held member waits on it and reads the fields
	// below afterwards, and the close is what orders the closer's writes against
	// every reader's — so those fields carry no lock of their own. A formation
	// that was never assembled at all, the verdict sealedAlready leaves on a
	// cohort id, is born with this already closed and nothing to read.
	done chan struct{}

	// Admission state, guarded by the service's mutex. slots is indexed by
	// ordinal so that "admitted once" is a property of the structure rather than
	// a search: an occupied slot is a duplicate, whatever order arrivals took.
	slots   []slot
	arrived int
	sealAt  int64

	// terminal is the cohort-level cause when formation failed. It is named once,
	// here, rather than B times at the members it takes down with it: B members
	// failing for one reason is one diagnosis (ADR-0010).
	terminal error

	// What the sealing member observed, written before done closes. All B members
	// report these, because they describe the envelope rather than the member.
	env           *envelopev1.RequestEnvelope
	envelopeBytes uint32
	sendAt        int64
	respRecvAt    int64
	resp          *envelopev1.ResponseEnvelope
	sendErr       error
}

func newFormation(cohort identity.CohortID, targetB int) *formation {
	return &formation{
		cohort:  cohort,
		targetB: targetB,
		done:    make(chan struct{}),
		slots:   make([]slot, targetB),
	}
}

// assembled is the cohort in ordinal order, which is also canonical order.
//
// The builder sorts what it is given, and handing it a sorted cohort is not a
// duplicate of that sort: it is what the slot array is for. An ordinal admitted
// once sits at its own index, so assembling the cohort is a walk rather than a
// search, and no arrival order survives into the envelope to be mistaken for
// release cost by the adapter's fan-out.
func (f *formation) assembled() ([]identity.LogicalRequest, [][]byte) {
	members := make([]identity.LogicalRequest, f.targetB)
	payloads := make([][]byte, f.targetB)
	for ord := range f.slots {
		members[ord] = identity.LogicalRequest{Cohort: f.cohort, Ordinal: identity.Ordinal(ord)}
		payloads[ord] = f.slots[ord].payload
	}
	return members, payloads
}

// join admits a member to its cohort, opening the formation if this is the first
// arrival, and reports whether the cohort is now complete.
//
// Counting members is not membership. Two copies of one ordinal are two members
// by count, so a proxy that counted would ship a cohort of B whose attested
// membership names a request nobody released — the same evidence a correct run
// produces, for work that was never done. Both malformations therefore fail the
// cohort rather than being absorbed into it.
func (s *Service) join(member identity.LogicalRequest, payload []byte) (*formation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, open := s.forming[member.Cohort]
	if !open {
		f = newFormation(member.Cohort, s.cfg.TargetB)
		s.forming[member.Cohort] = f
	}
	if f.terminal != nil {
		// The cohort has already been judged. A member arriving now inherits that
		// judgement instead of opening a fresh formation, which would let a late
		// arrival resurrect a cohort that has already failed.
		return nil, false, f.terminal
	}

	switch {
	case int(member.Ordinal) >= f.targetB:
		return nil, false, f.failLocked(fmt.Errorf(
			"cohort %d cannot form: ordinal %d is outside [0,%d), the cohort size the run declared",
			f.cohort, member.Ordinal, f.targetB))
	case f.slots[member.Ordinal].admitted:
		return nil, false, f.failLocked(fmt.Errorf(
			"cohort %d cannot form: ordinal %d arrived twice, so %d arrivals cover only %d of its %d ordinals",
			f.cohort, member.Ordinal, f.arrived+1, f.arrived, f.targetB))
	}

	f.slots[member.Ordinal] = slot{payload: payload, admitted: true}
	f.arrived++
	if f.arrived < f.targetB {
		return f, false, nil
	}

	// Complete. The seal is taken here, under the lock that admitted the last
	// member, because that admission is when formation ends — any instant read
	// after the lock is released would also contain whatever the proxy did next.
	f.sealAt = s.now()

	// The cohort id is spoken for from here on. What stays in the registry is not
	// the sealed formation — its members are still reading that one — but a
	// verdict on the id, so that an ordinal arriving afterwards is refused by name
	// instead of opening a second formation under an id that has already
	// travelled and waiting there until its own deadline killed it.
	//
	// One marker per cohort released is a map that grows at A=on and not at
	// A=off, which is the shape ADR-0004 watches for. It is far below the noise
	// floor of what the treatment already differs by: a run releases tens of
	// cohorts, and at A=off every one of their B members allocates an envelope of
	// its own.
	s.forming[f.cohort] = sealedAlready(f.cohort, f.targetB)
	return f, true, nil
}

// closedGate is the done channel the terminal markers share. Nothing ever waits
// on one — join refuses a member before it can reach the wait — but the field is
// never nil, so no later path can block on it forever.
var closedGate = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// sealedAlready is the verdict a cohort leaves behind on its own id when it
// seals.
//
// A member arriving now is a duplicate of one that already travelled, or a B+1st
// release. Both are the same fault a duplicate is before the seal, seen later,
// and both are refused rather than absorbed. Neither can come from a correct
// load generator, whose barrier releases exactly B — which is why this is worded
// as a diagnosis rather than as a condition being handled.
func sealedAlready(cohort identity.CohortID, targetB int) *formation {
	return &formation{
		cohort:  cohort,
		targetB: targetB,
		done:    closedGate,
		terminal: fmt.Errorf(
			"cohort %d cannot form: its %d members were already sealed into an envelope and sent, so this arrival is a duplicate or a B+1st release",
			cohort, targetB),
	}
}

// failLocked judges a cohort and wakes everyone it was holding. The caller holds
// the service's mutex; this is the one place a formation becomes terminal
// without sealing.
//
// The failed formation stays in the registry, because it is now this cohort id's
// verdict: a member arriving late must receive the same named failure rather
// than begin assembling a cohort that has already been judged. What it stops
// holding is the payloads, which belong to requests that are about to return.
func (f *formation) failLocked(cause error) error {
	f.terminal = cause
	f.slots = nil
	close(f.done)
	return cause
}

// sealAndSend packs the complete cohort into one envelope and sends it once.
//
// It runs on the member that completed the cohort — between completeness and the
// close of done that member is the formation's only writer, and the mutex it
// held while admitting itself orders every earlier member's payload against this
// read. What it stores here is what all B members go on to report.
//
// The send therefore runs on that member's context, and so a cancellation there
// does fail the whole cohort. That is the one asymmetry in this file and it is
// the defensible end of it: the B members left one barrier together with one
// per-request deadline, so the member that completed the cohort is holding the
// latest of the B, and the envelope travels under the most generous deadline the
// cohort has.
func (s *Service) sealAndSend(ctx context.Context, f *formation) {
	members, payloads := f.assembled()

	f.sendAt = s.now()
	env, err := s.builder.Aggregate(members, payloads, f.sealAt, f.sendAt)
	if err != nil {
		// Unreachable while admission and the builder agree on B: admission passes
		// exactly B distinct in-range ordinals of one cohort, which is what the
		// builder checks for. If they ever disagree it is the cohort that is wrong,
		// not the send, so it takes the same path any malformed cohort takes.
		s.mu.Lock()
		f.failLocked(fmt.Errorf("cohort %d cannot form: %w", f.cohort, err))
		s.mu.Unlock()
		return
	}
	f.env = env

	resp, err := s.backend.Execute(ctx, env)
	f.respRecvAt = s.now()
	f.resp = resp

	// Measured once per cohort rather than once per member, and after the
	// response rather than before the send. Once, because re-measuring a
	// B-member envelope B times would put a cost that grows with B on the path
	// whose cost is being compared across B; after, because the measurement then
	// falls in the fan-out interval as it does at A=off, instead of inflating the
	// transfer term on one side of the contrast only.
	f.envelopeBytes = uint32(envelope.WireBytes(env))
	if err != nil {
		f.sendErr = fmt.Errorf("proxy: backend rejected envelope %d carrying cohort %d: %w",
			env.GetEnvelopeId(), f.cohort, err)
	}
	close(f.done)
}

// await blocks until the cohort has been answered or judged.
//
// A member whose own context dies while waiting leaves alone. The cohort is not
// its to fail: the others may still be held, and the envelope may already be in
// flight, so a waiter's deadline takes out the waiter and nothing else. The one
// context that does decide the cohort is the sealing member's, for the reason
// sealAndSend gives.
func await(ctx context.Context, f *formation) error {
	select {
	case <-f.done:
		return nil
	case <-ctx.Done():
		select {
		case <-f.done:
			// The cohort's outcome landed in the same instant. It is better evidence
			// than the cancellation that raced it.
			return nil
		default:
			return ctx.Err()
		}
	}
}
