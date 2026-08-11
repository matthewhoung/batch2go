// Package individual is the executor for the V=off cells: D0, F00 and F10.
//
// It is a thin policy over the shared single-request submission engine. F00 and
// F10 resolve to this same constructor, the same gRPC client and channel, and
// the same model entry — which is what makes their contrast an envelope contrast
// rather than a comparison of two client implementations (M1 Rev 4 decision 1).
package individual

import (
	"context"
	"fmt"
	"sync"
	"syscall"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/executor"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/triton"
)

// Backend is the submission engine this executor drives.
//
// The interface is declared here, at the point of use, so that the fan-out can be
// exercised without a live inference server. That matters more than it sounds:
// concurrency, result-to-member mapping and dispatch skew are the whole of this
// executor's contribution, and until they could be tested offline the only way to
// observe them was to run the real stack and read the timestamps afterwards.
//
// *triton.Submitter satisfies it; nothing else does in a real run.
type Backend interface {
	Submit(ctx context.Context, model string, members []identity.LogicalRequest) (triton.Result, error)
	LogicalBytes() int
}

// Executor submits each member as its own single-item request to the unbatched
// entry.
type Executor struct {
	submitter Backend
	now       executor.Clock
}

// New builds the individual executor over a shared submission engine.
func New(submitter Backend, now executor.Clock) (*Executor, error) {
	if submitter == nil {
		return nil, fmt.Errorf("executor/individual: needs a submission engine")
	}
	if now == nil {
		return nil, fmt.Errorf("executor/individual: needs a clock reader")
	}
	return &Executor{submitter: submitter, now: now}, nil
}

// Execute submits the dispatch's members.
//
// At n=1 — every envelope at A=off — this is one submission. At n=B the members
// are submitted concurrently rather than in sequence: at A=off the concurrency
// arrives from the network as the barrier's B simultaneous envelopes, so at A=on
// the executor has to recreate it, or the two factor levels would differ in
// backend serialization as well as in transport (M1 Rev 4 decision 1). Requests
// are never serialized behind one another inside an executor; the waiting they
// do belongs to the backend queue, where the cycle model books it.
func (e *Executor) Execute(ctx context.Context, d executor.Dispatch) (executor.Result, executor.Evidence, error) {
	if len(d.Members) == 0 {
		return executor.Result{}, executor.Evidence{}, fmt.Errorf("executor/individual: dispatch carries no members")
	}
	if d.Model == "" {
		return executor.Result{}, executor.Evidence{}, fmt.Errorf("executor/individual: dispatch names no model entry")
	}

	cpuStart := processCPUNanos()
	results := make([]executor.MemberResult, len(d.Members))

	var wg sync.WaitGroup
	for i, member := range d.Members {
		wg.Add(1)
		go func(i int, member identity.LogicalRequest) {
			defer wg.Done()

			res := executor.MemberResult{Member: member}
			res.DispatchedAt = e.now()
			out, err := e.submitter.Submit(ctx, d.Model, []identity.LogicalRequest{member})
			res.ResultAt = e.now()

			if err != nil {
				res.Err = err
			} else {
				res.Membership = out.Membership
				res.BatchSize = out.BatchSize
				res.DataOutBytes = out.DataOutBytes
			}
			results[i] = res
		}(i, member)
	}
	wg.Wait()

	evidence := executor.Evidence{
		Dispatched: uint32(len(d.Members)),
		SkewNanos:  dispatchSkew(results),
		CPUNanos:   processCPUNanos() - cpuStart,
		CPUScope:   events.CPUScopeProcess,
	}
	return executor.Result{Members: results}, evidence, nil
}

// dispatchSkew is first-to-last submit across a fan-out. Contract tests bound it
// well below one execution's service time, which is what establishes that a
// fan-out cell's members really did arrive at the backend together.
func dispatchSkew(results []executor.MemberResult) int64 {
	if len(results) < 2 {
		return 0
	}
	first, last := results[0].DispatchedAt, results[0].DispatchedAt
	for _, r := range results[1:] {
		if r.DispatchedAt < first {
			first = r.DispatchedAt
		}
		if r.DispatchedAt > last {
			last = r.DispatchedAt
		}
	}
	return last - first
}

// processCPUNanos reads the process's consumed CPU time.
//
// RUSAGE_SELF counts the whole process, so what this bounds is "CPU the adapter
// burned while this dispatch was outstanding", not "CPU this dispatch cost".
// With concurrent dispatches the windows overlap and the same work is counted
// once per dispatch. The value is therefore reported under CPUScopeProcess and
// is comparable only within one Factor A level; see that constant for why a
// per-dispatch measurement is not available yet.
func processCPUNanos() int64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return timevalNanos(usage.Utime) + timevalNanos(usage.Stime)
}

func timevalNanos(tv syscall.Timeval) int64 {
	return tv.Sec*1_000_000_000 + int64(tv.Usec)*1_000
}
