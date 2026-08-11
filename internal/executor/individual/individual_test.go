package individual

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matthewhoung/batch2go/internal/executor"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/triton"
)

// fakeBackend records what it was asked to submit and when.
//
// It holds each submission for a fixed duration so that concurrency is
// observable: if the executor submitted serially, B submissions of d each would
// span B·d, and their windows would not overlap.
type fakeBackend struct {
	hold time.Duration

	// failFor makes one member's submission fail, to check that a failure does
	// not corrupt its neighbours.
	failFor *identity.LogicalRequest

	// membershipFor overrides what a member's execution attests, so that a
	// mismatched mapping is distinguishable from a correct one.
	membershipFor func(identity.LogicalRequest) []identity.UID

	// holdFor makes completion order differ from submission order, which is the
	// only way the result-to-member mapping can be observed at all.
	holdFor func(identity.LogicalRequest) time.Duration

	mu          sync.Mutex
	inFlight    int
	maxInFlight int
	calls       []call
	clock       func() int64
}

type call struct {
	members    []identity.LogicalRequest
	start, end int64
}

func (f *fakeBackend) LogicalBytes() int { return 1024 }

func (f *fakeBackend) Submit(ctx context.Context, model string, members []identity.LogicalRequest) (triton.Result, error) {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxInFlight {
		f.maxInFlight = f.inFlight
	}
	start := f.clock()
	f.mu.Unlock()

	hold := f.hold
	if f.holdFor != nil {
		hold = f.holdFor(members[0])
	}
	if hold > 0 {
		time.Sleep(hold)
	}

	f.mu.Lock()
	f.inFlight--
	f.calls = append(f.calls, call{members: members, start: start, end: f.clock()})
	f.mu.Unlock()

	if f.failFor != nil && len(members) == 1 && members[0] == *f.failFor {
		return triton.Result{}, fmt.Errorf("backend refused %v", members[0])
	}

	membership := []identity.UID{members[0].UID()}
	if f.membershipFor != nil {
		membership = f.membershipFor(members[0])
	}
	return triton.Result{
		Members:      members,
		Membership:   membership,
		BatchSize:    len(membership),
		DataOutBytes: 256,
	}, nil
}

func newExecutor(t *testing.T, b *fakeBackend) *Executor {
	t.Helper()
	var counter atomic.Int64
	b.clock = func() int64 { return counter.Add(1000) }

	e, err := New(b, func() int64 { return time.Now().UnixNano() })
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	return e
}

func cohort(size int) []identity.LogicalRequest {
	out := make([]identity.LogicalRequest, size)
	for i := range out {
		out[i] = identity.LogicalRequest{Cohort: 7, Ordinal: identity.Ordinal(i)}
	}
	return out
}

// At A=off the adapter releases one member per envelope, so a dispatch carries
// one member and there is nothing to skew.
func TestSingleMemberDispatchSubmitsOnce(t *testing.T) {
	backend := &fakeBackend{}
	e := newExecutor(t, backend)

	members := cohort(1)
	result, evidence, err := e.Execute(context.Background(), executor.Dispatch{Model: "m", Members: members})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if len(backend.calls) != 1 {
		t.Fatalf("backend saw %d submissions, want 1", len(backend.calls))
	}
	if evidence.Dispatched != 1 {
		t.Errorf("dispatched = %d, want 1", evidence.Dispatched)
	}
	if evidence.DispatchSkewNanos != 0 {
		t.Errorf("skew = %dns; one member cannot skew against itself", evidence.DispatchSkewNanos)
	}
	// Zero is a measurement here, not an absence: the scope says what the CPU
	// number counted, and it is recorded even when the skew is trivially zero.
	if evidence.CPUScope != executor.CPUScopeProcess {
		t.Errorf("cpu scope = %q, want %q", evidence.CPUScope, executor.CPUScopeProcess)
	}
	if len(result.Members) != 1 || result.Members[0].Member != members[0] {
		t.Errorf("result does not describe the submitted member: %+v", result.Members)
	}
}

// The property F10 rests on: at A=on the executor recreates the arrival
// concurrency that the release barrier produces at A=off. Requests are never
// serialized inside an executor; the waiting they do belongs to the backend
// queue, where the cycle model books it.
func TestFanOutSubmitsConcurrentlyRatherThanInSequence(t *testing.T) {
	const hold = 40 * time.Millisecond
	backend := &fakeBackend{hold: hold}
	e := newExecutor(t, backend)

	members := cohort(4)
	start := time.Now()
	_, evidence, err := e.Execute(context.Background(), executor.Dispatch{Model: "m", Members: members})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if len(backend.calls) != len(members) {
		t.Fatalf("backend saw %d submissions, want %d", len(backend.calls), len(members))
	}
	if backend.maxInFlight != len(members) {
		t.Errorf("at most %d submissions were in flight at once, want %d — the fan-out serialized",
			backend.maxInFlight, len(members))
	}
	// Serial submission of B holds would take B·hold; concurrent takes about one.
	if elapsed > time.Duration(len(members)-1)*hold {
		t.Errorf("fan-out took %v for %d submissions of %v each; it did not overlap them",
			elapsed, len(members), hold)
	}
	if evidence.Dispatched != len(members) {
		t.Errorf("dispatched = %d, want %d", evidence.Dispatched, len(members))
	}
	// Contract tests bound skew well below one submission's service time; here
	// the fixture makes that concrete.
	if evidence.DispatchSkewNanos >= int64(hold) {
		t.Errorf("dispatch skew %dns is not far below one submission of %v", evidence.DispatchSkewNanos, hold)
	}
}

// Ordering guarantees apply to the result-to-member mapping and never to
// submission order. A backend that completes members out of order must still
// have each result paired with the member that produced it.
func TestResultsPairWithTheirOwnMemberWhateverTheCompletionOrder(t *testing.T) {
	const unit = 15 * time.Millisecond
	const size = 4

	// Completion order is the reverse of submission order: ordinal 0 is held
	// longest and finishes last. If results were collected as they arrived, the
	// mapping would come back reversed.
	backend := &fakeBackend{
		holdFor: func(m identity.LogicalRequest) time.Duration {
			return time.Duration(size-int(m.Ordinal)) * unit
		},
	}
	e := newExecutor(t, backend)

	members := cohort(size)
	result, _, err := e.Execute(context.Background(), executor.Dispatch{Model: "m", Members: members})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(result.Members) != len(members) {
		t.Fatalf("got %d results, want %d", len(result.Members), len(members))
	}

	// The fixture has to actually reorder, or this test asserts nothing.
	completion := append([]call(nil), backend.calls...)
	if len(completion) != size || completion[0].members[0] != members[size-1] {
		t.Fatalf("the backend did not complete out of order: first completion was %v", completion[0].members[0])
	}

	for i, m := range result.Members {
		if m.Member != members[i] {
			t.Errorf("result %d describes %v, want %v — results followed completion order, not submission order",
				i, m.Member, members[i])
		}
		if len(m.Membership) != 1 || m.Membership[0] != m.Member.UID() {
			t.Errorf("%v carries membership %v, which is not its own uid", m.Member, m.Membership)
		}
	}
}

// A member that fails is reported failed and its neighbours are untouched — a
// member missing from the record is indistinguishable from one never released.
func TestOneFailingMemberDoesNotCorruptItsNeighbours(t *testing.T) {
	members := cohort(4)
	backend := &fakeBackend{failFor: &members[2]}
	e := newExecutor(t, backend)

	result, evidence, err := e.Execute(context.Background(), executor.Dispatch{Model: "m", Members: members})
	if err != nil {
		t.Fatalf("execute returned an error for a per-member failure: %v", err)
	}
	if evidence.Dispatched != len(members) {
		t.Errorf("dispatched = %d, want %d — a failure does not un-dispatch a member", evidence.Dispatched, len(members))
	}
	if len(result.Members) != len(members) {
		t.Fatalf("got %d results, want %d — the failed member vanished", len(result.Members), len(members))
	}

	failed := result.Failed()
	if failed == nil {
		t.Error("Failed() reported nothing for a dispatch containing a failure")
	}
	for i, m := range result.Members {
		if i == 2 {
			if m.Err == nil {
				t.Error("the failing member is reported as succeeding")
			}
			continue
		}
		if m.Err != nil {
			t.Errorf("%v failed because a neighbour did: %v", m.Member, m.Err)
		}
		if m.BatchSize != 1 {
			t.Errorf("%v reports batch size %d, want 1", m.Member, m.BatchSize)
		}
	}
}

// Dispatch timestamps must bracket the submission they describe, or the skew and
// the adapter's stage boundaries would be describing something else.
func TestDispatchTimestampsBracketTheSubmission(t *testing.T) {
	backend := &fakeBackend{hold: 10 * time.Millisecond}
	e := newExecutor(t, backend)

	result, _, err := e.Execute(context.Background(), executor.Dispatch{Model: "m", Members: cohort(3)})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	for _, m := range result.Members {
		if m.ResultAt <= m.DispatchedAt {
			t.Errorf("%v: result at %d is not after dispatch at %d", m.Member, m.ResultAt, m.DispatchedAt)
		}
		if elapsed := m.ResultAt - m.DispatchedAt; elapsed < int64(backend.hold) {
			t.Errorf("%v: %dns elapsed across a submission held for %v", m.Member, elapsed, backend.hold)
		}
	}
}

func TestExecutorRejectsAnEmptyOrUnaddressedDispatch(t *testing.T) {
	e := newExecutor(t, &fakeBackend{})

	if _, _, err := e.Execute(context.Background(), executor.Dispatch{Model: "m"}); err == nil {
		t.Error("a dispatch with no members should be refused")
	}
	if _, _, err := e.Execute(context.Background(), executor.Dispatch{Members: cohort(1)}); err == nil {
		t.Error("a dispatch naming no model entry should be refused")
	}
	if _, err := New(nil, func() int64 { return 0 }); err == nil {
		t.Error("an executor without a submission engine should be refused")
	}
	if _, err := New(&fakeBackend{}, nil); err == nil {
		t.Error("an executor without a clock should be refused")
	}
}
