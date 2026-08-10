// Package workload owns the load generator's scheduling and the release
// barrier — the only synchronization point at the load generator, and the only
// one anywhere in A=OFF cells (ADR-0001). At A=ON the proxy synchronizes too,
// because it cannot aggregate a cohort it has not finished receiving.
//
// What this package deliberately does not do: it never decides envelope
// aggregation or compute semantics, and it never joins a cohort after release. A
// cohort at A=off is an accounting label carried end to end and joined offline;
// the barrier's job is to make the B members leave together, and then to get out
// of the way.
package workload

import (
	"fmt"
	"sync"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// Barrier releases a cohort's B logical requests simultaneously.
//
// Every member arrives and blocks; when the last one arrives the barrier takes
// one timestamp and releases all of them at once. At A=off that timestamp is
// t_cohort_seal, owned here; at A=on the proxy owns the seal instead, at
// envelope seal, and this instant is the load generator's barrier release
// (ADR-0001).
type Barrier struct {
	size int
	now  func() int64

	mu      sync.Mutex
	arrived int
	gate    chan struct{}
	sealAt  int64
}

// NewBarrier builds a barrier for cohorts of the given size. now is the clock
// domain's reader, injected so that tests can drive the state machine with a
// clock they control.
func NewBarrier(size int, now func() int64) (*Barrier, error) {
	if size <= 0 {
		return nil, fmt.Errorf("workload: barrier size must be positive, got %d", size)
	}
	if now == nil {
		return nil, fmt.Errorf("workload: barrier needs a clock reader")
	}
	return &Barrier{size: size, now: now, gate: make(chan struct{})}, nil
}

// Size is the cohort size the barrier releases.
func (b *Barrier) Size() int { return b.size }

// Arrive blocks until the whole cohort has arrived, then returns the seal
// timestamp — the instant the cohort was released, identical for every member.
//
// Arriving more times than the cohort's size is a programming error, not a
// recoverable condition: it would mean a cohort contained more members than the
// accounting says, and every downstream count would be wrong.
func (b *Barrier) Arrive() int64 {
	b.mu.Lock()
	if b.arrived >= b.size {
		b.mu.Unlock()
		panic(fmt.Sprintf("workload: %d members arrived at a barrier of size %d", b.arrived+1, b.size))
	}
	b.arrived++
	if b.arrived == b.size {
		b.sealAt = b.now()
		close(b.gate)
	}
	gate := b.gate
	b.mu.Unlock()

	<-gate
	// Reading sealAt after the gate closes is ordered by the channel close, so
	// every member observes the same instant.
	return b.sealAt
}

// Released reports whether the cohort has been released, and its seal timestamp.
func (b *Barrier) Released() (int64, bool) {
	select {
	case <-b.gate:
		return b.sealAt, true
	default:
		return 0, false
	}
}

// Cohort is one released cohort's accounting record.
type Cohort struct {
	ID      identity.CohortID         `json:"cohort_id"`
	Members []identity.LogicalRequest `json:"members"`

	// Warmup cohorts traverse the same path but produce no evidence.
	Warmup bool `json:"warmup"`

	// ScheduledAt is when the generator intended to release, and SealedAt when
	// the barrier actually did. Recording both is what makes the realized
	// schedule a measurement rather than a restatement of the plan.
	ScheduledAt int64 `json:"scheduled_at"`
	SealedAt    int64 `json:"sealed_at"`
}

// NewCohort mints a cohort's labels. Identity is minted here, once, and carried
// unchanged to the response.
func NewCohort(id identity.CohortID, size int, warmup bool) Cohort {
	members := make([]identity.LogicalRequest, size)
	for i := range members {
		members[i] = identity.LogicalRequest{Cohort: id, Ordinal: identity.Ordinal(i)}
	}
	return Cohort{ID: id, Members: members, Warmup: warmup}
}
