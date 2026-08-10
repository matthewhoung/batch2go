package clockdomain

import (
	"testing"
	"time"
)

func TestEstablishRecordsBootIdentity(t *testing.T) {
	d, err := Establish()
	if err != nil {
		t.Fatalf("establish clock domain: %v", err)
	}
	if d.ID == "" {
		t.Error("clock domain needs an id; records reference it to make subtraction legitimate")
	}
	if d.BootID == "" {
		t.Error("clock domain needs a boot identity: CLOCK_MONOTONIC restarts at boot")
	}
	if d.BootTimeUnix <= 0 {
		t.Errorf("boot time = %d, want a positive unix second", d.BootTimeUnix)
	}
	if d.ResolutionNanos <= 0 {
		t.Errorf("clock resolution = %d, want positive", d.ResolutionNanos)
	}
	if d.ReadOverheadNanos <= 0 {
		t.Errorf("read overhead = %d, want a measured positive cost", d.ReadOverheadNanos)
	}
}

// The whole point of establishing a domain is that two processes agree on it.
// Two Establish calls on one host and boot must land in the same domain, or
// cross-process subtraction inside a session would be refused.
func TestEstablishIsStableWithinABoot(t *testing.T) {
	a, err := Establish()
	if err != nil {
		t.Fatalf("establish: %v", err)
	}
	b, err := Establish()
	if err != nil {
		t.Fatalf("establish: %v", err)
	}
	if a.ID != b.ID {
		t.Errorf("two domains on one host disagree: %q vs %q", a.ID, b.ID)
	}
	if !a.SameDomain(b) {
		t.Error("SameDomain should hold for two domains established on one host and boot")
	}
}

// A domain whose id differs must not be treated as subtractable.
func TestDifferentDomainsAreNotSubtractable(t *testing.T) {
	a, err := Establish()
	if err != nil {
		t.Fatalf("establish: %v", err)
	}
	other := *a
	other.BootID = "00000000-0000-0000-0000-000000000000"
	other.ID = deriveID(&other)

	if a.SameDomain(&other) {
		t.Error("a domain from a different boot must not be subtractable against this one")
	}
	if a.SameDomain(nil) {
		t.Error("a missing domain is never subtractable")
	}
}

func TestNowIsMonotonicAndAdvances(t *testing.T) {
	d, err := Establish()
	if err != nil {
		t.Fatalf("establish: %v", err)
	}
	prev := d.Now()
	for i := 0; i < 1000; i++ {
		now := d.Now()
		if now < prev {
			t.Fatalf("clock went backwards: %d then %d", prev, now)
		}
		prev = now
	}

	start := d.Now()
	time.Sleep(5 * time.Millisecond)
	if elapsed := d.Now() - start; elapsed < 4*int64(time.Millisecond) {
		t.Errorf("5ms sleep measured as %dns; the clock is not advancing in real time", elapsed)
	}
}

// The fast reader is only usable because it returns the same absolute
// CLOCK_MONOTONIC value the syscall returns. If that ever stops holding, the
// domain must fall back rather than silently mix two clocks.
func TestFastClockAgreesWithTheSyscallClock(t *testing.T) {
	if !certifyFastClock() {
		t.Skip("fast clock did not certify on this host; the domain falls back to the syscall reader")
	}
	for i := 0; i < 100; i++ {
		before := syscallNow()
		fast := nanotime()
		after := syscallNow()
		if fast < before || fast > after {
			t.Fatalf("fast read %d outside syscall bracket [%d, %d]", fast, before, after)
		}
	}
}

// Whichever reader certified, a domain must actually use it.
func TestSyscallSourceUsesTheSyscallReader(t *testing.T) {
	d, err := Establish()
	if err != nil {
		t.Fatalf("establish: %v", err)
	}
	forced := *d
	forced.Source = SourceSyscall

	before := syscallNow()
	got := forced.Now()
	after := syscallNow()
	if got < before || got > after {
		t.Fatalf("syscall-sourced read %d outside bracket [%d, %d]", got, before, after)
	}
}
