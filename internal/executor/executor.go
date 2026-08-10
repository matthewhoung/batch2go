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

// CPUScope names what a CPU measurement actually counted. It travels with the
// value because the two are not separable: a number whose definition changed
// between conditions is not a measurement of those conditions.
type CPUScope string

const (
	// CPUScopeProcess is whole-process CPU sampled around a dispatch, via
	// getrusage(RUSAGE_SELF).
	//
	// It is NOT comparable across Factor A levels, and that is a property of the
	// measurement rather than of the treatment. At A=off a cohort's B dispatches
	// run concurrently and each attributes the entire process's CPU over its own
	// overlapping window, so the same work is counted B times; at A=on one
	// dispatch attributes it once. Differencing the two would produce a number
	// that moves with B for reasons that have nothing to do with envelope
	// aggregation — exactly the treatment-correlated artifact this project
	// measures GC and tracing overhead in order to bound.
	//
	// So it is recorded as a diagnostic, and the validator must not admit it into
	// a cross-level contrast while it carries this scope.
	CPUScopeProcess CPUScope = "process"

	// CPUScopeDispatch is reserved for a measurement that counts only the work of
	// one dispatch and is therefore comparable across A levels. Nothing produces
	// it yet: Go's scheduler migrates goroutines across threads, so
	// RUSAGE_THREAD does not bound a dispatch either. When something does, it
	// coexists with the process scope in the archive rather than replacing it,
	// and a reader can tell which definition produced each number.
	CPUScopeDispatch CPUScope = "dispatch"
)

// ComparableAcrossFactorA reports whether a scope may enter a contrast between
// A=on and A=off.
func (s CPUScope) ComparableAcrossFactorA() bool { return s == CPUScopeDispatch }

// Evidence is what the executor observed about its own dispatch.
type Evidence struct {
	Dispatched int

	// DispatchSkewNanos is first-to-last submit within one fan-out call. At n=1
	// there is nothing to skew and it is zero — recorded as zero rather than
	// omitted, so "no skew" and "not measured" stay distinguishable.
	DispatchSkewNanos int64

	// CPUNanos is the adapter's cost for the dispatch, and CPUScope says what that
	// number counted. It is mandatory evidence for the fan-out cells (M1 Rev 4
	// decision 1), but only within one Factor A level while the scope is
	// process-wide.
	CPUNanos int64
	CPUScope CPUScope
}

// Executor turns a dispatch into results and evidence.
type Executor interface {
	Execute(context.Context, Dispatch) (Result, Evidence, error)
}

// Clock reads the executor's monotonic clock domain.
type Clock func() int64
