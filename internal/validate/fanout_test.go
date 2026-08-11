package validate_test

import (
	"strings"
	"testing"
	"time"

	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/testkit"
	"github.com/matthewhoung/batch2go/internal/validate"
)

// The fan-out judgement. A serial release — the adapter submitting its members
// one after another instead of together — produces the same execution count, the
// same batch-size histogram and the same attested membership as a correct one.
// Only the dispatch timestamps tell them apart, and only if something reads them.

func fanOutSpec() testkit.Spec { return testkit.NewSpec(identity.CellF10) }

// The control. A concurrent release skews by nothing, and nothing about it is a
// finding — which is what makes the fixtures below mean something.
func TestAConcurrentFanOutPasses(t *testing.T) {
	fixture := fanOutSpec().MustBuild()
	v := validate.Validate(fixture.Expectation, fixture.Records)
	if !v.Passed {
		t.Fatalf("a concurrent fan-out must validate green: %v", v.Defects())
	}
	if len(v.FanOut.Releases) != fixture.Spec.CohortCount {
		t.Fatalf("%d releases reported, want one per cohort (%d)", len(v.FanOut.Releases), fixture.Spec.CohortCount)
	}
	for _, rel := range v.FanOut.Releases {
		if rel.ObservedSkewNanos != 0 {
			t.Errorf("envelope %d skewed by %dns; a concurrent release skews by nothing", rel.Envelope, rel.ObservedSkewNanos)
		}
		if !rel.Overlapped {
			t.Errorf("envelope %d's members did not overlap", rel.Envelope)
		}
		if rel.Members != fixture.Spec.CohortSize || int(rel.DeclaredMembers) != fixture.Spec.CohortSize {
			t.Errorf("envelope %d released %d members and declared %d, want %d",
				rel.Envelope, rel.Members, rel.DeclaredMembers, fixture.Spec.CohortSize)
		}
	}
}

// Skew beyond the manifest's bound fails, and the message carries both numbers:
// a bound without an observation is unactionable, and an observation without its
// bound is not a finding.
func TestDispatchSkewBeyondTheBoundFails(t *testing.T) {
	spec := fanOutSpec()
	spec.DispatchStagger = 200 * time.Microsecond
	fixture := spec.MustBuild()

	v := validate.Validate(fixture.Expectation, fixture.Records)
	if v.Passed {
		t.Fatal("a fan-out whose members reached the backend far apart was accepted")
	}
	if !v.HasDefect(validate.DefectDispatchSkew) {
		t.Fatalf("the skew was not named: %v", v.Defects())
	}

	bound := fixture.Expectation.MaxDispatchSkewNanos
	observed := int64(spec.CohortSize-1) * int64(spec.DispatchStagger)
	var named bool
	for _, d := range v.Defects() {
		if d.Kind != validate.DefectDispatchSkew {
			continue
		}
		if strings.Contains(d.Message, itoa(bound)) && strings.Contains(d.Message, itoa(observed)) {
			named = true
		}
	}
	if !named {
		t.Errorf("no defect names both the %dns bound and the %dns observed: %v", bound, observed, v.Defects())
	}

	// Still a fan-out, just a wide one. The members overlapped, so this must not
	// also be reported as a sequence of releases — the two findings are different
	// faults and a run that conflated them would send the reader to the wrong one.
	if v.HasDefect(validate.DefectFanOutSerial) {
		t.Error("an over-bound skew was also reported as a serial fan-out; they are distinct findings")
	}
}

// A fan-out whose members did not overlap is a sequence of releases wearing the
// shape of one call. It is its own finding, distinct from a skew that is merely
// too wide.
func TestAFanOutWhoseSubmissionsDidNotOverlapFails(t *testing.T) {
	spec := fanOutSpec()
	// Wider than a member's whole downstream, so each is submitted only after the
	// one before it has come back.
	spec.DispatchStagger = 20 * time.Millisecond
	fixture := spec.MustBuild()

	v := validate.Validate(fixture.Expectation, fixture.Records)
	if v.Passed {
		t.Fatal("a serial fan-out was accepted")
	}
	if !v.HasDefect(validate.DefectFanOutSerial) {
		t.Fatalf("the absence of overlap was not named: %v", v.Defects())
	}
	for _, rel := range v.FanOut.Releases {
		if rel.Overlapped {
			t.Errorf("envelope %d is reported as overlapped; its members ran one after another", rel.Envelope)
		}
	}
}

// The adapter's account of its own release is evidence, not testimony. Its
// reported skew and the dispatch timestamps beside it come from the same clock
// readings, so they cannot legitimately disagree — and an adapter believed on
// its own word could report any skew it liked.
func TestReportedSkewMustAgreeWithTheRecordedTimestamps(t *testing.T) {
	spec := fanOutSpec()
	// A release that really was concurrent, reported as though it were not.
	lie := int64(999)
	spec.ReportedSkewNanos = &lie
	fixture := spec.MustBuild()

	v := validate.Validate(fixture.Expectation, fixture.Records)
	if v.Passed {
		t.Fatal("an adapter whose reported skew contradicted its own timestamps was believed")
	}
	if !v.HasDefect(validate.DefectDispatchEvidence) {
		t.Fatalf("the disagreement was not named: %v", v.Defects())
	}

	// And the mirror image: an adapter under-reporting a real skew. This is the
	// direction that matters, because it is the one that would let a serial
	// fan-out pass a bound check on its own say-so.
	spec = fanOutSpec()
	spec.DispatchStagger = 20 * time.Millisecond
	quiet := int64(0)
	spec.ReportedSkewNanos = &quiet
	understated := spec.MustBuild()

	v = validate.Validate(understated.Expectation, understated.Records)
	if !v.HasDefect(validate.DefectDispatchEvidence) {
		t.Errorf("an adapter reporting zero skew over a serial release was believed: %v", v.Defects())
	}
	if !v.HasDefect(validate.DefectDispatchSkew) {
		t.Errorf("the real skew was not judged against the bound: %v", v.Defects())
	}
}

// The verdict carries the fan-out as structured fields. A number a reader has to
// extract from a sentence is a number the analysis cannot use.
func TestTheVerdictCarriesFanOutEvidenceAsFields(t *testing.T) {
	spec := fanOutSpec()
	spec.DispatchStagger = 50 * time.Microsecond
	fixture := spec.MustBuild()

	v := validate.Validate(fixture.Expectation, fixture.Records)
	if v.FanOut.BoundNanos != fixture.Expectation.MaxDispatchSkewNanos {
		t.Errorf("the report carries a %dns bound, the expectation declared %dns",
			v.FanOut.BoundNanos, fixture.Expectation.MaxDispatchSkewNanos)
	}
	want := int64(spec.CohortSize-1) * int64(spec.DispatchStagger)
	if v.FanOut.MaxSkewNanos != want {
		t.Errorf("widest observed skew %dns, the fixture staggered by %dns across %d members",
			v.FanOut.MaxSkewNanos, want, spec.CohortSize)
	}
	for _, rel := range v.FanOut.Releases {
		if rel.ObservedSkewNanos != rel.ReportedSkewNanos {
			t.Errorf("envelope %d: observed %dns, reported %dns", rel.Envelope, rel.ObservedSkewNanos, rel.ReportedSkewNanos)
		}
		if rel.CPUScope == "" {
			t.Errorf("envelope %d carries a CPU number with no scope; the value is not interpretable without it", rel.Envelope)
		}
	}
}

// A process-scoped CPU value never enters a comparison across Factor A. The
// restriction is enforced by the only function that could perform one, so it
// cannot be forgotten by a caller who did not read the comment.
func TestProcessScopedCPUIsRefusedAcrossFactorA(t *testing.T) {
	f00 := testkit.NewSpec(identity.CellF00).MustBuild()
	f10 := testkit.NewSpec(identity.CellF10).MustBuild()

	on := validate.Validate(f10.Expectation, f10.Records)
	off := validate.Validate(f00.Expectation, f00.Records)

	// Both runs measured CPU at the process scope, which is the only scope
	// anything produces today.
	for _, rel := range on.FanOut.Releases {
		if rel.CPUScope != "process" {
			t.Fatalf("expected the process scope, got %q", rel.CPUScope)
		}
	}

	if _, err := validate.AdapterCPUAcrossFactorA(on.FanOut, off.FanOut); err == nil {
		t.Fatal("a process-scoped CPU value was admitted to a comparison across Factor A")
	} else if !strings.Contains(err.Error(), "process") {
		t.Errorf("the refusal does not name the scope that caused it: %v", err)
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [24]byte
	i := len(buf)
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
