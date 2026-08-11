// Package executor defines the seam between the adapter and the backend.
//
// The seam is dispatch-shaped on purpose. A Dispatch is the set of cohort
// members the adapter releases in one call — n=1 per envelope at A=off, n=B at
// A=on — so adding the later cells means adding a policy over this interface
// rather than reshaping it (ARCHITECTURE §6.4).
//
// Executors return evidence, never conclusions. Whether a cohort met exact-B
// conformance is computed offline by the validator; an executor that decided it
// would be judging its own work.
package executor

import (
	"context"
	"fmt"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// Dispatch is the members released to an executor in one call.
type Dispatch struct {
	// Model is the Triton entry to submit against. Cells differ in which entry
	// they target; they do not differ in how a request is built.
	Model string

	Members []identity.LogicalRequest
}

// MemberResult is one member's outcome and the evidence its execution produced.
type MemberResult struct {
	Member identity.LogicalRequest

	// Membership is the complete uid set the execution attested to, and BatchSize
	// its realized shape as the model reported it (ADR-0007).
	Membership   []identity.UID
	BatchSize    int
	DataOutBytes int

	// DispatchedAt and ResultAt are the adapter's own monotonic timestamps around
	// the submission — schema timestamps 7 and 11.
	DispatchedAt int64
	ResultAt     int64

	Err error
}

// Result is a dispatch's per-member outcomes, in the order the members were
// given. Ordering guarantees apply to this result↔member mapping and never to
// submission order (ADR-0001 consequence).
type Result struct {
	Members []MemberResult
}

// Failed reports the first member error, if any.
func (r Result) Failed() error {
	for _, m := range r.Members {
		if m.Err != nil {
			return fmt.Errorf("executor: member %v failed: %w", m.Member, m.Err)
		}
	}
	return nil
}

// Evidence is what the executor observed about its own dispatch.
//
// It is events.DispatchEvidence because that is where it ends up: the adapter
// writes it into the record stream unchanged, and the archive is where the
// numbers are finally read. Restating the shape here would create two
// definitions that have to be kept in agreement by hand — and the CPU scope, in
// particular, is only worth carrying if it reaches the reader attached to its
// value (M1 Rev 4 decision 1).
type Evidence = events.DispatchEvidence

// Executor turns a dispatch into results and evidence.
type Executor interface {
	Execute(context.Context, Dispatch) (Result, Evidence, error)
}

// Clock reads the executor's monotonic clock domain.
type Clock func() int64
