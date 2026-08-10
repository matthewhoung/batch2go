package workload

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// The barrier's contract: nobody leaves until everybody has arrived, and all of
// them observe one shared release instant. That single instant is t_cohort_seal.
func TestBarrierReleasesEveryMemberAtOneInstant(t *testing.T) {
	const size = 4
	var clock atomic.Int64
	clock.Store(1000)

	b, err := NewBarrier(size, func() int64 { return clock.Load() })
	if err != nil {
		t.Fatalf("new barrier: %v", err)
	}

	seals := make([]int64, size)
	var released atomic.Int32
	var wg sync.WaitGroup
	// Every member but the last arrives; the cohort is incomplete.
	for i := 0; i < size-1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seals[i] = b.Arrive()
			released.Add(1)
		}(i)
	}

	// None may proceed while the cohort is incomplete — this is the property that
	// makes a cohort a released group rather than a sequence of independent sends.
	time.Sleep(50 * time.Millisecond)
	if _, ok := b.Released(); ok {
		t.Fatal("barrier released before every member arrived")
	}
	if got := released.Load(); got != 0 {
		t.Fatalf("%d members proceeded before the cohort was complete", got)
	}

	seals[size-1] = b.Arrive()
	released.Add(1)
	wg.Wait()
	if got := released.Load(); got != size {
		t.Fatalf("%d members released, want %d", got, size)
	}
	for i, seal := range seals {
		if seal != 1000 {
			t.Errorf("member %d observed seal %d, want the shared instant 1000", i, seal)
		}
	}
	sealAt, ok := b.Released()
	if !ok || sealAt != 1000 {
		t.Errorf("Released() = (%d, %v), want (1000, true)", sealAt, ok)
	}
}

// The seal is taken when the last member arrives, not when the first does.
func TestSealIsTakenAtTheLastArrival(t *testing.T) {
	var clock atomic.Int64
	clock.Store(500)

	b, err := NewBarrier(2, func() int64 { return clock.Load() })
	if err != nil {
		t.Fatalf("new barrier: %v", err)
	}

	first := make(chan int64, 1)
	go func() { first <- b.Arrive() }()

	time.Sleep(20 * time.Millisecond)
	clock.Store(900) // time passes while the cohort is incomplete
	second := b.Arrive()

	if second != 900 {
		t.Errorf("second member observed seal %d, want 900", second)
	}
	if got := <-first; got != 900 {
		t.Errorf("first member observed seal %d, want the release instant 900", got)
	}
}

// A cohort that over-fills is an accounting error, and the barrier must not
// absorb it: every downstream count would silently be wrong.
func TestBarrierRefusesMoreMembersThanTheCohortHolds(t *testing.T) {
	b, err := NewBarrier(1, func() int64 { return 1 })
	if err != nil {
		t.Fatalf("new barrier: %v", err)
	}
	b.Arrive()

	defer func() {
		if recover() == nil {
			t.Error("arriving beyond the cohort size must not be silently accepted")
		}
	}()
	b.Arrive()
}

func TestNewBarrierRejectsInvalidConfiguration(t *testing.T) {
	if _, err := NewBarrier(0, func() int64 { return 0 }); err == nil {
		t.Error("a barrier of size 0 should be refused")
	}
	if _, err := NewBarrier(4, nil); err == nil {
		t.Error("a barrier without a clock should be refused")
	}
}

// Identity is minted once at the load generator and carried end to end.
func TestNewCohortMintsContiguousOrdinals(t *testing.T) {
	c := NewCohort(7, 4, false)
	if c.ID != 7 {
		t.Errorf("cohort id = %d, want 7", c.ID)
	}
	if len(c.Members) != 4 {
		t.Fatalf("cohort has %d members, want 4", len(c.Members))
	}
	for i, m := range c.Members {
		want := identity.LogicalRequest{Cohort: 7, Ordinal: identity.Ordinal(i)}
		if m != want {
			t.Errorf("member %d = %v, want %v", i, m, want)
		}
	}

	// The uid the model attests must resolve back to the member that minted it.
	for _, m := range c.Members {
		if got := m.UID().LogicalRequest(); got != m {
			t.Errorf("uid %d resolved to %v, want %v", m.UID(), got, m)
		}
	}
}
