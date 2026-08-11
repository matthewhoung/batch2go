// Package envelope owns the proxy↔adapter transport contract: construction,
// invariants, and byte accounting.
//
// It must not build Triton tensors and must not choose compute semantics. The
// proxy decides how many logical requests share an envelope — that is Factor A —
// and the adapter's executor decides how they execute. Keeping those two
// decisions in different packages is what stops one from being inferred from the
// other (ARCHITECTURE §3.3).
package envelope

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// SchemaVersion is the envelope protocol version.
const SchemaVersion = 1

// Config is the run-scoped identity every envelope of a run carries.
type Config struct {
	Experiment  identity.ExperimentID
	Session     identity.SessionID
	Run         identity.RunID
	Cell        identity.Cell
	ClockDomain identity.ClockDomainID
	TargetB     int
}

// Validate rejects a configuration that could not produce attributable envelopes.
func (c Config) Validate() error {
	switch {
	case c.Run == "":
		return fmt.Errorf("envelope: config needs a run id")
	case c.Cell == "":
		return fmt.Errorf("envelope: config needs a cell")
	case c.ClockDomain == "":
		return fmt.Errorf("envelope: config needs a clock domain id")
	case c.TargetB <= 0:
		return fmt.Errorf("envelope: config needs a positive target B, got %d", c.TargetB)
	}
	return nil
}

// Builder mints envelopes for one run.
type Builder struct {
	cfg    Config
	nextID atomic.Uint64
}

// NewBuilder returns a builder for the run.
func NewBuilder(cfg Config) (*Builder, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Builder{cfg: cfg}, nil
}

// Independent builds the A=off envelope: exactly one logical request, no
// joining, and no cohort seal — the proxy is a pass-through and owns no seal at
// this factor level (ADR-0001).
func (b *Builder) Independent(member identity.LogicalRequest, payload []byte, sentAt int64) *envelopev1.RequestEnvelope {
	env := b.base()
	env.CohortId = uint32(member.Cohort)
	env.ExpectedMembers = 1
	env.Aggregate = false
	env.TProxySend = sentAt
	env.Requests = []*envelopev1.LogicalRequest{{
		CohortId: uint32(member.Cohort),
		Ordinal:  uint32(member.Ordinal),
		Uid:      int64(member.UID()),
		Payload:  payload,
	}}
	accountBytes(env, uint64(len(payload)))
	return env
}

// Aggregate builds the A=on envelope: a whole cohort in one message, sealed by
// the proxy. It is here so the adapter's seam is the same shape for both factor
// levels; the cells that use it arrive in spec 0002.
func (b *Builder) Aggregate(members []identity.LogicalRequest, payloads [][]byte, seal, sentAt int64) (*envelopev1.RequestEnvelope, error) {
	if len(members) != len(payloads) {
		return nil, fmt.Errorf("envelope: %d members but %d payloads", len(members), len(payloads))
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("envelope: an aggregate envelope carries at least one member")
	}

	cohort := members[0].Cohort
	env := b.base()
	env.CohortId = uint32(cohort)
	env.ExpectedMembers = uint32(len(members))
	env.Aggregate = true
	env.TProxySend = sentAt
	env.TCohortSeal = &seal

	var logical uint64
	env.Requests = make([]*envelopev1.LogicalRequest, 0, len(members))
	for _, i := range canonicalOrder(members) {
		m := members[i]
		if m.Cohort != cohort {
			return nil, fmt.Errorf("envelope: aggregate envelope mixes cohorts %d and %d", cohort, m.Cohort)
		}
		env.Requests = append(env.Requests, &envelopev1.LogicalRequest{
			CohortId: uint32(m.Cohort),
			Ordinal:  uint32(m.Ordinal),
			Uid:      int64(m.UID()),
			Payload:  payloads[i],
		})
		logical += uint64(len(payloads[i]))
	}
	if err := checkCohortMembership(env.Requests, env.GetCohortId(), b.cfg.TargetB); err != nil {
		return nil, err
	}
	accountBytes(env, logical)
	return env, nil
}

// canonicalOrder is the permutation that puts a cohort's members in ordinal
// order, whatever order they arrived in.
//
// Order is part of the contract because the adapter fans a dispatch out in the
// order the envelope lists. Left as arrival order, dispatch skew would measure
// which member reached the proxy first rather than what it costs to release a
// cohort — and at A=on that skew is the quantity the fan-out is judged by.
func canonicalOrder(members []identity.LogicalRequest) []int {
	order := make([]int, len(members))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return members[order[i]].Ordinal < members[order[j]].Ordinal
	})
	return order
}

func (b *Builder) base() *envelopev1.RequestEnvelope {
	return &envelopev1.RequestEnvelope{
		SchemaVersion: SchemaVersion,
		ExperimentId:  string(b.cfg.Experiment),
		SessionId:     string(b.cfg.Session),
		RunId:         string(b.cfg.Run),
		Cell:          string(b.cfg.Cell),
		ClockDomainId: string(b.cfg.ClockDomain),
		EnvelopeId:    b.nextID.Add(1),
		Vectorize:     b.cfg.Cell.VectorizesCompute(),
		TargetB:       uint32(b.cfg.TargetB),
	}
}

// Expectation is what the adapter requires of an arriving envelope.
type Expectation struct {
	Run         identity.RunID
	Cell        identity.Cell
	ClockDomain identity.ClockDomainID
	TargetB     int
}

// Validate checks an arriving envelope against the run the adapter is serving.
//
// Protocol drift fails the run visibly rather than being tolerated. In
// particular the envelope's declared factor levels are checked against the cell
// the adapter was configured for: an envelope's shape is never taken as evidence
// of its factor level, because a one-request envelope is exactly what A=off and a
// misconfigured A=on both look like.
func Validate(env *envelopev1.RequestEnvelope, exp Expectation) error {
	if env == nil {
		return fmt.Errorf("envelope: nil envelope")
	}
	if env.GetSchemaVersion() != SchemaVersion {
		return fmt.Errorf("envelope: schema version %d, this build speaks %d", env.GetSchemaVersion(), SchemaVersion)
	}
	if got := identity.RunID(env.GetRunId()); got != exp.Run {
		return fmt.Errorf("envelope: run %q, adapter is serving %q", got, exp.Run)
	}
	if got := identity.Cell(env.GetCell()); got != exp.Cell {
		return fmt.Errorf("envelope: cell %q, adapter is serving %q", got, exp.Cell)
	}
	if got := identity.ClockDomainID(env.GetClockDomainId()); got != exp.ClockDomain {
		return fmt.Errorf("envelope: clock domain %q, adapter is in %q; their timestamps cannot be subtracted", got, exp.ClockDomain)
	}
	if got := int(env.GetTargetB()); got != exp.TargetB {
		return fmt.Errorf("envelope: target B %d, run declares %d", got, exp.TargetB)
	}

	if want := exp.Cell.AggregatesEnvelopes(); env.GetAggregate() != want {
		return fmt.Errorf("envelope: declares aggregate=%v, cell %s is aggregate=%v", env.GetAggregate(), exp.Cell, want)
	}
	if want := exp.Cell.VectorizesCompute(); env.GetVectorize() != want {
		return fmt.Errorf("envelope: declares vectorize=%v, cell %s is vectorize=%v", env.GetVectorize(), exp.Cell, want)
	}

	members := env.GetRequests()
	if len(members) != int(env.GetExpectedMembers()) {
		return fmt.Errorf("envelope: carries %d members, declares %d", len(members), env.GetExpectedMembers())
	}
	if env.GetAggregate() {
		if len(members) != exp.TargetB {
			return fmt.Errorf("envelope: aggregate envelope carries %d members, target B is %d", len(members), exp.TargetB)
		}
		if env.TCohortSeal == nil {
			return fmt.Errorf("envelope: aggregate envelope carries no cohort seal; at A=on the proxy owns it")
		}
	} else {
		if len(members) != 1 {
			return fmt.Errorf("envelope: independent envelope carries %d members, want exactly 1", len(members))
		}
		if env.TCohortSeal != nil {
			return fmt.Errorf("envelope: independent envelope carries a proxy cohort seal; at A=off the load generator owns it (ADR-0001)")
		}
	}

	for _, m := range members {
		req := identity.LogicalRequest{Cohort: identity.CohortID(m.GetCohortId()), Ordinal: identity.Ordinal(m.GetOrdinal())}
		if got := identity.UID(m.GetUid()); got != req.UID() {
			return fmt.Errorf("envelope: member %v carries uid %d, its identity encodes %d", req, got, req.UID())
		}
		if m.GetCohortId() != env.GetCohortId() {
			return fmt.Errorf("envelope: member %v is not in the envelope's cohort %d", req, env.GetCohortId())
		}
		if len(m.GetPayload()) == 0 {
			return fmt.Errorf("envelope: member %v carries no payload; the declared padding must traverse every hop", req)
		}
	}

	if env.GetAggregate() {
		if err := checkCohortMembership(members, env.GetCohortId(), exp.TargetB); err != nil {
			return err
		}
	}
	return checkCanonicalOrder(members)
}

// checkCohortMembership requires an envelope to carry each of its cohort's B
// ordinals exactly once.
//
// Counting members is not membership. Four copies of one ordinal is four
// members, and an envelope that dropped one member and repeated another would
// pass a count check — then execute as a batch of B whose attested membership
// names requests that were never released. The message names every fault it
// found rather than the first, because an envelope this wrong is a bug being
// diagnosed, not a condition being handled.
func checkCohortMembership(members []*envelopev1.LogicalRequest, cohort uint32, targetB int) error {
	seen := make([]int, targetB)
	var strays []identity.Ordinal
	for _, m := range members {
		ord := identity.Ordinal(m.GetOrdinal())
		if int(ord) >= targetB {
			strays = append(strays, ord)
			continue
		}
		seen[ord]++
	}

	var repeated, missing []identity.Ordinal
	for ord, n := range seen {
		switch {
		case n == 0:
			missing = append(missing, identity.Ordinal(ord))
		case n > 1:
			repeated = append(repeated, identity.Ordinal(ord))
		}
	}

	var faults []string
	if len(repeated) > 0 {
		faults = append(faults, fmt.Sprintf("ordinals %v appear more than once", repeated))
	}
	if len(missing) > 0 {
		faults = append(faults, fmt.Sprintf("ordinals %v are missing", missing))
	}
	if len(strays) > 0 {
		faults = append(faults, fmt.Sprintf("ordinals %v are outside [0,%d)", strays, targetB))
	}
	if len(faults) == 0 {
		return nil
	}
	return fmt.Errorf("envelope: cohort %d is not a cohort of %d: %s", cohort, targetB, strings.Join(faults, "; "))
}

// checkCanonicalOrder requires members to travel in ascending ordinal order, so
// that the adapter's fan-out order is the cohort's own order and not the order
// its members happened to reach the proxy.
func checkCanonicalOrder(members []*envelopev1.LogicalRequest) error {
	for i := 1; i < len(members); i++ {
		if prev, ord := members[i-1].GetOrdinal(), members[i].GetOrdinal(); ord <= prev {
			return fmt.Errorf("envelope: members are not in canonical order: ordinal %d follows %d", ord, prev)
		}
	}
	return nil
}

// accountBytes fills the envelope's two byte counters so that they partition its
// marshaled size exactly: logical_bytes is the payload the experiment asked to
// move, auxiliary_bytes everything the protocol added to move it.
//
// The overhead is measured rather than estimated, which makes the counter part
// of the message it measures: writing it grows the size it reports. Each pass
// re-measures and rewrites, and the loop terminates because the value only ever
// grows and only grows when its varint gains a byte — of at most ten.
func accountBytes(env *envelopev1.RequestEnvelope, logical uint64) {
	env.LogicalBytes = logical
	env.AuxiliaryBytes = 0
	for {
		auxiliary := uint64(proto.Size(env)) - logical
		if auxiliary == env.AuxiliaryBytes {
			return
		}
		env.AuxiliaryBytes = auxiliary
	}
}

// Members extracts the logical request identities an envelope carries.
func Members(env *envelopev1.RequestEnvelope) []identity.LogicalRequest {
	out := make([]identity.LogicalRequest, 0, len(env.GetRequests()))
	for _, m := range env.GetRequests() {
		out = append(out, identity.LogicalRequest{
			Cohort:  identity.CohortID(m.GetCohortId()),
			Ordinal: identity.Ordinal(m.GetOrdinal()),
		})
	}
	return out
}

// bufferPool holds marshal buffers so that encoding an envelope reuses memory
// instead of allocating a fresh one per message.
//
// This is the allocation half of ADR-0003 and ADR-0004: at A=off a cohort emits
// B envelopes where A=on emits one, so a per-envelope allocation would be a cost
// that moves with the treatment — inside the effect being measured.
var bufferPool = sync.Pool{New: func() any { b := make([]byte, 0, 1<<20); return &b }}

// Encode marshals an envelope into a pooled buffer and hands both to fn. The
// buffer returns to the pool when fn returns, so callers must not retain it.
func Encode(env *envelopev1.RequestEnvelope, fn func([]byte) error) error {
	buf := bufferPool.Get().(*[]byte)
	defer bufferPool.Put(buf)

	out, err := proto.MarshalOptions{}.MarshalAppend((*buf)[:0], env)
	if err != nil {
		return fmt.Errorf("envelope: marshal: %w", err)
	}
	*buf = out
	return fn(out)
}

// WireBytes reports an envelope's marshaled size, measured rather than estimated.
func WireBytes(env *envelopev1.RequestEnvelope) int { return proto.Size(env) }
