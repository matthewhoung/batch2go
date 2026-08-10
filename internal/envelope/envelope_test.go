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
