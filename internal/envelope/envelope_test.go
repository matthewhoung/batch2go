package envelope

import (
	"strings"
	"testing"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/identity"
)

func f00Config() Config {
	return Config{
		Experiment:  "exp-1",
		Session:     "sess-1",
		Run:         "run-1",
		Cell:        identity.CellF00,
		ClockDomain: "cd-abcdef0123456789abcd",
		TargetB:     4,
	}
}

func f00Expectation() Expectation {
	return Expectation{
		Run:         "run-1",
		Cell:        identity.CellF00,
		ClockDomain: "cd-abcdef0123456789abcd",
		TargetB:     4,
	}
}

func newTestBuilder(t *testing.T) *Builder {
	t.Helper()
	b, err := NewBuilder(f00Config())
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}
	return b
}

// At A=off an envelope carries exactly one logical request and no cohort seal:
// the proxy does no joining, and the load generator owns the seal (ADR-0001).
func TestIndependentEnvelopeCarriesOneMemberAndNoSeal(t *testing.T) {
	b := newTestBuilder(t)
	member := identity.LogicalRequest{Cohort: 7, Ordinal: 2}
	env := b.Independent(member, []byte("payload"), 12345)

	if got := len(env.GetRequests()); got != 1 {
		t.Fatalf("envelope carries %d members, want 1", got)
	}
	if env.GetAggregate() {
		t.Error("an A=off envelope must not declare aggregation")
	}
	if env.TCohortSeal != nil {
		t.Error("at A=off the proxy emits no cohort seal")
	}
	if env.GetTProxySend() != 12345 {
		t.Errorf("t_proxy_send = %d, want 12345", env.GetTProxySend())
	}
	if got := identity.UID(env.GetRequests()[0].GetUid()); got != member.UID() {
		t.Errorf("uid = %d, want %d", got, member.UID())
	}
	if err := Validate(env, f00Expectation()); err != nil {
		t.Errorf("a freshly built envelope should validate: %v", err)
	}
}

// Envelope ids are unique within a run, so an envelope is attributable.
func TestEnvelopeIDsAreUnique(t *testing.T) {
	b := newTestBuilder(t)
	seen := map[uint64]bool{}
	for i := 0; i < 100; i++ {
		env := b.Independent(identity.LogicalRequest{Cohort: 1, Ordinal: identity.Ordinal(i)}, []byte("p"), 0)
		if seen[env.GetEnvelopeId()] {
			t.Fatalf("envelope id %d reused", env.GetEnvelopeId())
		}
		seen[env.GetEnvelopeId()] = true
	}
}

// Protocol drift fails visibly. Each of these would otherwise produce a run
// whose records describe a condition nobody configured.
func TestValidateRejectsDrift(t *testing.T) {
	b := newTestBuilder(t)
	member := identity.LogicalRequest{Cohort: 7, Ordinal: 2}

	cases := map[string]struct {
		mutate func(*envelopev1.RequestEnvelope)
		want   string
	}{
		"wrong run": {
			func(e *envelopev1.RequestEnvelope) { e.RunId = "run-other" }, "run",
		},
		"wrong cell": {
			func(e *envelopev1.RequestEnvelope) { e.Cell = string(identity.CellF10) }, "cell",
		},
		"another clock domain": {
			func(e *envelopev1.RequestEnvelope) { e.ClockDomainId = "cd-otherboot0000000000" }, "clock domain",
		},
		"wrong target B": {
			func(e *envelopev1.RequestEnvelope) { e.TargetB = 16 }, "target B",
		},
		"schema version": {
			func(e *envelopev1.RequestEnvelope) { e.SchemaVersion = 99 }, "schema version",
		},
		"member count disagrees with declaration": {
			func(e *envelopev1.RequestEnvelope) { e.ExpectedMembers = 4 }, "declares",
		},
		"uid does not match identity": {
			func(e *envelopev1.RequestEnvelope) { e.Requests[0].Uid = 999 }, "uid",
		},
		"no payload": {
			func(e *envelopev1.RequestEnvelope) { e.Requests[0].Payload = nil }, "payload",
		},
	}

	for name, tc := range cases {
		env := b.Independent(member, []byte("payload"), 1)
		tc.mutate(env)

		err := Validate(env, f00Expectation())
		if err == nil {
			t.Errorf("%s: should have been refused", name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q should mention %q", name, err, tc.want)
		}
	}
}

// An envelope's shape is never taken as evidence of its factor level. A
// one-request envelope is what A=off looks like and also what a misconfigured
// A=on looks like, so the declared level is checked against the cell.
func TestDeclaredFactorLevelsAreCheckedAgainstTheCell(t *testing.T) {
	b := newTestBuilder(t)
	env := b.Independent(identity.LogicalRequest{Cohort: 1, Ordinal: 0}, []byte("p"), 1)
	env.Aggregate = true

	err := Validate(env, f00Expectation())
	if err == nil {
		t.Fatal("an envelope declaring aggregation in an A=off cell must be refused")
	}
	if !strings.Contains(err.Error(), "aggregate") {
		t.Errorf("the error should name the factor mismatch, got: %v", err)
	}
}

// A seal arriving at A=off means something joined upstream.
func TestSealOnAnIndependentEnvelopeIsRefused(t *testing.T) {
	b := newTestBuilder(t)
	env := b.Independent(identity.LogicalRequest{Cohort: 1, Ordinal: 0}, []byte("p"), 1)
	seal := int64(500)
	env.TCohortSeal = &seal

	err := Validate(env, f00Expectation())
	if err == nil {
		t.Fatal("a cohort seal on an A=off envelope must be refused")
	}
	if !strings.Contains(err.Error(), "seal") {
		t.Errorf("the error should name the seal, got: %v", err)
	}
}

// The aggregate builder is the A=on seam. It is not used by this slice's cells,
// but its invariants are pinned now so spec 0002 extends the seam rather than
// reshaping it.
func TestAggregateEnvelopeCarriesTheWholeCohortAndItsSeal(t *testing.T) {
	cfg := f00Config()
	cfg.Cell = identity.CellF10
	b, err := NewBuilder(cfg)
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}

	members := []identity.LogicalRequest{
		{Cohort: 3, Ordinal: 0}, {Cohort: 3, Ordinal: 1},
		{Cohort: 3, Ordinal: 2}, {Cohort: 3, Ordinal: 3},
	}
	payloads := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}

	env, err := b.Aggregate(members, payloads, 900, 1000)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if !env.GetAggregate() || env.TCohortSeal == nil || *env.TCohortSeal != 900 {
		t.Error("an A=on envelope aggregates and carries the proxy's seal")
	}
	if got := len(env.GetRequests()); got != 4 {
		t.Errorf("carries %d members, want 4", got)
	}

	exp := f00Expectation()
	exp.Cell = identity.CellF10
	if err := Validate(env, exp); err != nil {
		t.Errorf("a freshly built aggregate envelope should validate: %v", err)
	}

	// Mixing cohorts would dissolve the accounting the design rests on.
	mixed := append([]identity.LogicalRequest(nil), members...)
	mixed[2] = identity.LogicalRequest{Cohort: 4, Ordinal: 2}
	if _, err := b.Aggregate(mixed, payloads, 900, 1000); err == nil {
		t.Error("an aggregate envelope must not mix cohorts")
	}
}

// f10Builder builds aggregate envelopes for a cohort of four.
func f10Builder(t *testing.T) (*Builder, Expectation) {
	t.Helper()
	cfg := f00Config()
	cfg.Cell = identity.CellF10
	b, err := NewBuilder(cfg)
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}
	exp := f00Expectation()
	exp.Cell = identity.CellF10
	return b, exp
}

// cohortOf returns the members and payloads of a whole cohort, each payload
// distinguishable so that a member and its payload can be checked to have
// travelled together.
func cohortOf(cohort identity.CohortID, ordinals ...identity.Ordinal) ([]identity.LogicalRequest, [][]byte) {
	members := make([]identity.LogicalRequest, 0, len(ordinals))
	payloads := make([][]byte, 0, len(ordinals))
	for _, o := range ordinals {
		members = append(members, identity.LogicalRequest{Cohort: cohort, Ordinal: o})
		payloads = append(payloads, []byte{byte(o), 0xAA})
	}
	return members, payloads
}

// Counting members is not membership. Four copies of one ordinal is four
// members, and an envelope that lost one member and duplicated another would
// pass a count check, then execute as a batch of B whose membership evidence
// names the wrong requests.
func TestAggregateEnvelopeMustCarryEachOrdinalExactlyOnce(t *testing.T) {
	b, _ := f10Builder(t)

	cases := map[string]struct {
		ordinals []identity.Ordinal
		want     string
	}{
		"a duplicated ordinal":          {[]identity.Ordinal{0, 1, 1, 3}, "more than once"},
		"every member the same":         {[]identity.Ordinal{2, 2, 2, 2}, "more than once"},
		"a missing ordinal":             {[]identity.Ordinal{0, 1, 2, 2}, "missing"},
		"an ordinal outside the cohort": {[]identity.Ordinal{0, 1, 2, 9}, "outside"},
		"a short cohort":                {[]identity.Ordinal{0, 1, 2}, "missing"},
	}

	for name, tc := range cases {
		members, payloads := cohortOf(3, tc.ordinals...)
		_, err := b.Aggregate(members, payloads, 900, 1000)
		if err == nil {
			t.Errorf("%s: should have been refused", name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q should say %q", name, err, tc.want)
		}
	}
}

// The adapter is the other side of the same invariant: an envelope built
// elsewhere, or damaged in flight, must not validate as a cohort it does not
// carry.
func TestValidateRefusesAnEnvelopeThatIsNotItsCohort(t *testing.T) {
	b, exp := f10Builder(t)
	members, payloads := cohortOf(3, 0, 1, 2, 3)

	t.Run("duplicate", func(t *testing.T) {
		env, err := b.Aggregate(members, payloads, 900, 1000)
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		env.Requests[3] = env.Requests[0]

		err = Validate(env, exp)
		if err == nil {
			t.Fatal("an envelope carrying one ordinal twice is not a cohort of four")
		}
		if !strings.Contains(err.Error(), "more than once") {
			t.Errorf("the error should name the duplication, got: %v", err)
		}
	})

	t.Run("gap", func(t *testing.T) {
		env, err := b.Aggregate(members, payloads, 900, 1000)
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		stray := identity.LogicalRequest{Cohort: 3, Ordinal: 9}
		env.Requests[3] = &envelopev1.LogicalRequest{
			CohortId: uint32(stray.Cohort),
			Ordinal:  uint32(stray.Ordinal),
			Uid:      int64(stray.UID()),
			Payload:  []byte("p"),
		}

		err = Validate(env, exp)
		if err == nil {
			t.Fatal("an envelope missing an ordinal of its cohort must be refused")
		}
		if !strings.Contains(err.Error(), "missing") || !strings.Contains(err.Error(), "outside") {
			t.Errorf("the error should name both the gap and the stray ordinal, got: %v", err)
		}
	})
}

// Members travel in ordinal order whatever order they arrived in. The adapter
// fans a dispatch out in the order the envelope lists, so arrival order would
// otherwise leak into dispatch skew — a measurement of the proxy's intake rather
// than of the cost of releasing a cohort.
func TestAggregateMembersTravelInCanonicalOrder(t *testing.T) {
	b, exp := f10Builder(t)
	members, payloads := cohortOf(3, 2, 0, 3, 1)

	env, err := b.Aggregate(members, payloads, 900, 1000)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if err := Validate(env, exp); err != nil {
		t.Fatalf("a freshly built aggregate envelope should validate: %v", err)
	}

	carried := Members(env)
	for i, m := range env.GetRequests() {
		if got := identity.Ordinal(m.GetOrdinal()); int(got) != i {
			t.Errorf("member %d carries ordinal %d, canonical order puts %d there", i, got, i)
		}
		// A payload must follow its own member through the reordering.
		if want := byte(i); len(m.GetPayload()) == 0 || m.GetPayload()[0] != want {
			t.Errorf("member %d carries payload %v, want the one built for ordinal %d", i, m.GetPayload(), i)
		}
		if got := identity.UID(m.GetUid()); got != carried[i].UID() {
			t.Errorf("member %d carries uid %d, its identity encodes %d", i, got, carried[i].UID())
		}
	}
}

// An envelope whose members arrived out of order is refused rather than
// dispatched in that order.
func TestValidateRefusesMembersOutOfCanonicalOrder(t *testing.T) {
	b, exp := f10Builder(t)
	members, payloads := cohortOf(3, 0, 1, 2, 3)

	env, err := b.Aggregate(members, payloads, 900, 1000)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	env.Requests[0], env.Requests[3] = env.Requests[3], env.Requests[0]

	err = Validate(env, exp)
	if err == nil {
		t.Fatal("members out of canonical order must be refused")
	}
	if !strings.Contains(err.Error(), "order") {
		t.Errorf("the error should name the ordering, got: %v", err)
	}
}

// The two byte counters partition the marshaled size: logical_bytes is what the
// experiment asked to move, auxiliary_bytes is what the protocol added to move
// it. The second is the per-envelope cost Factor A changes — paid B times at
// A=off and once at A=on — so it is measured against the real marshaled size
// rather than estimated, and it is emphatically not a copy of the first.
func TestByteAccountingPartitionsTheMarshaledSize(t *testing.T) {
	b, _ := f10Builder(t)

	for _, payloadBytes := range []int{1, 100, 4096, 1 << 16, 1 << 20} {
		members, _ := cohortOf(5, 0, 1, 2, 3)
		payloads := make([][]byte, len(members))
		for i := range payloads {
			payloads[i] = make([]byte, payloadBytes)
		}

		env, err := b.Aggregate(members, payloads, 900, 1000)
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		checkByteAccounting(t, env, uint64(len(members)*payloadBytes))

		single := b.Independent(members[0], payloads[0], 1000)
		checkByteAccounting(t, single, uint64(payloadBytes))
	}
}

func checkByteAccounting(t *testing.T, env *envelopev1.RequestEnvelope, wantLogical uint64) {
	t.Helper()
	if got := env.GetLogicalBytes(); got != wantLogical {
		t.Errorf("logical_bytes = %d, the payloads are %d bytes", got, wantLogical)
	}
	if got, want := env.GetLogicalBytes()+env.GetAuxiliaryBytes(), uint64(WireBytes(env)); got != want {
		t.Errorf("logical_bytes + auxiliary_bytes = %d, the envelope marshals to %d", got, want)
	}
	if env.GetAuxiliaryBytes() == 0 {
		t.Error("auxiliary_bytes is zero; an envelope always costs some framing")
	}
	if env.GetAuxiliaryBytes() >= wantLogical && wantLogical > 4096 {
		t.Errorf("auxiliary_bytes = %d for %d payload bytes; it is measuring the payload, not the framing",
			env.GetAuxiliaryBytes(), wantLogical)
	}
}

// The client's cohort seal was removed at v1: the load generator owns the seal
// at A=off and records it itself, and at A=on the proxy mints its own. The tag
// stays reserved so it cannot be quietly reused for a different meaning while
// the protocol version stays 1.
func TestClientRequestReservesTheCohortSealTag(t *testing.T) {
	const sealTag = 6

	d := (&envelopev1.ClientRequest{}).ProtoReflect().Descriptor()
	if f := d.Fields().ByNumber(sealTag); f != nil {
		t.Fatalf("tag %d carries field %q; the client carries no cohort seal", sealTag, f.Name())
	}
	if f := d.Fields().ByName("t_cohort_seal"); f != nil {
		t.Errorf("the client request still carries %q", f.Name())
	}

	ranges := d.ReservedRanges()
	for i := 0; i < ranges.Len(); i++ {
		if r := ranges.Get(i); r[0] <= sealTag && sealTag < r[1] {
			return
		}
	}
	t.Errorf("tag %d is not reserved; it could be reused for a different meaning at v1", sealTag)
}

func TestMembersRoundTripThroughTheEnvelope(t *testing.T) {
	b := newTestBuilder(t)
	want := identity.LogicalRequest{Cohort: 12, Ordinal: 3}
	env := b.Independent(want, []byte("payload"), 1)

	got := Members(env)
	if len(got) != 1 || got[0] != want {
		t.Errorf("members = %v, want [%v]", got, want)
	}
}

// Encoding reuses pooled buffers rather than allocating per envelope: at A=off a
// cohort encodes B envelopes where A=on encodes one, so a per-envelope
// allocation would move with the treatment (ADR-0003, ADR-0004).
func TestEncodeReusesBuffersAndRoundTrips(t *testing.T) {
	b := newTestBuilder(t)
	env := b.Independent(identity.LogicalRequest{Cohort: 1, Ordinal: 0}, make([]byte, 4096), 1)

	var size int
	if err := Encode(env, func(buf []byte) error {
		size = len(buf)
		return nil
	}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if size != WireBytes(env) {
		t.Errorf("encoded %d bytes, WireBytes reports %d", size, WireBytes(env))
	}

	if allocs := testing.AllocsPerRun(100, func() {
		_ = Encode(env, func([]byte) error { return nil })
	}); allocs > 1 {
		t.Errorf("Encode allocated %.1f times per call; the buffer pool is not being reused", allocs)
	}
}

func TestBuilderRejectsIncompleteConfig(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"no run":          func(c *Config) { c.Run = "" },
		"no cell":         func(c *Config) { c.Cell = "" },
		"no clock domain": func(c *Config) { c.ClockDomain = "" },
		"no target B":     func(c *Config) { c.TargetB = 0 },
	} {
		cfg := f00Config()
		mutate(&cfg)
		if _, err := NewBuilder(cfg); err == nil {
			t.Errorf("%s: should have been refused", name)
		}
	}
}
