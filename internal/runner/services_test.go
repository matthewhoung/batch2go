package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// shellPath returns a POSIX shell, or skips. The lifecycle these tests describe
// is about processes, so it is exercised against real ones.
func shellPath(t *testing.T) string {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh available; the service lifecycle needs real processes to be tested")
	}
	return sh
}

// newServices builds a services under test and guarantees nothing it launched
// outlives the test.
//
// The guarantee is load-bearing rather than tidy: one of the fixtures below
// ignores SIGINT by construction, so a test that panics, times out, or is
// interrupted would otherwise leave an immortal process reparented to init.
// That has already happened on this machine once.
func newServices(t *testing.T, hooks serviceHooks) *services {
	t.Helper()
	s := &services{hooks: hooks}
	t.Cleanup(func() {
		for _, p := range s.procs {
			if p.cmd.Process != nil {
				_ = p.cmd.Process.Kill()
			}
		}
	})
	return s
}

// trapping builds a shell fixture that installs a SIGINT disposition and then
// announces it is ready.
//
// The announcement is load-bearing: a signal delivered before the trap is
// installed kills the shell outright, so a test that signalled immediately
// would be measuring shell startup rather than the shutdown path.
func trapping(t *testing.T, disposition string) (script string, ready func()) {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "ready")
	script = "trap '" + disposition + "' INT; : > " + marker + "; while true; do sleep 1; done"

	return script, func() {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(marker); err == nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("the fixture never announced its signal trap at %s", marker)
	}
}

// stopWithin runs Stop and fails if it does not return in time.
//
// A bound rather than a bare call, because the exactly-once exit-status
// protocol between the watcher and Stop fails by blocking: without it, breaking
// that protocol would hang the package for the whole test timeout instead of
// failing the test that guards it. The runner must never hang, and neither must
// the tests that say so.
func stopWithin(t *testing.T, s *services, budget time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.Stop() }()
	select {
	case err := <-done:
		return err
	case <-time.After(budget):
		t.Fatalf("Stop did not return within %s; the exit status was never delivered", budget)
		return nil
	}
}

// reaped blocks until the service at index i has been waited on.
//
// It observes the exit channel rather than cmd.ProcessState, which the watcher
// writes: the channel is the synchronisation point the design already has, and
// polling the process state from here would be a data race against Wait. The
// value is put straight back, because Stop reads it too and it is written once.
func reaped(t *testing.T, s *services, i int) {
	t.Helper()
	p := s.procs[i]
	select {
	case err := <-p.exit:
		p.exit <- err
	case <-time.After(10 * time.Second):
		t.Fatalf("service %q never exited", p.name)
	}
}

// A service that exits during a run is the run's cause, not one of its
// symptoms. Every request in flight fails against a process that is gone, and
// reporting one of those failures names a cohort and an ordinal while saying
// nothing about the process.
func TestAServiceThatDiesDuringTheRunIsNamed(t *testing.T) {
	sh := shellPath(t)

	cancelled := make(chan struct{})
	s := newServices(t, serviceHooks{onDeath: func() { close(cancelled) }})
	if err := s.start("adapter", sh, "-c", "exit 3"); err != nil {
		t.Fatalf("start: %v", err)
	}

	select {
	case <-cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("a service exited and the run was never woken")
	}

	died := s.died()
	if died == nil {
		t.Fatal("a service exited during the run and nothing recorded it")
	}
	if !strings.Contains(died.Error(), "adapter") {
		t.Errorf("the diagnosis %q does not name which service died", died)
	}
	if !strings.Contains(died.Error(), "exit status 3") {
		t.Errorf("the diagnosis %q does not carry how it died", died)
	}

	// And the symptom is reported under the cause rather than in place of it.
	symptom := context.Canceled
	explained := s.explain(symptom)
	if !strings.Contains(explained.Error(), "adapter") {
		t.Errorf("the explained error %q buries the cause", explained)
	}
	if !strings.Contains(explained.Error(), symptom.Error()) {
		t.Errorf("the explained error %q drops the symptom that surfaced it", explained)
	}

	if err := stopWithin(t, s, 30*time.Second); err == nil {
		t.Error("Stop should still report the death it inherited")
	}
}

// explain is a lens, not a substitute. Against a healthy data plane it has
// nothing to add and must hand back exactly what it was given — a version that
// returned nil there would turn a failed cohort into a completed run.
func TestExplainPassesThroughWhenNothingDied(t *testing.T) {
	s := newServices(t, serviceHooks{})

	symptom := fmt.Errorf("runner: request c3/o1 failed: deadline exceeded")
	got := s.explain(symptom)
	if got == nil {
		t.Fatal("explain dropped the error of a run whose data plane was healthy")
	}
	if !errors.Is(got, symptom) {
		t.Errorf("explain returned %v, want the error it was given", got)
	}
	if s.explain(nil) != nil {
		t.Error("explain invented an error where there was none")
	}
}

// A service that exits cleanly but unasked is still a loss: the data plane is
// incomplete from that moment, whatever its exit status says.
func TestAServiceThatExitsCleanlyUnaskedIsStillADeath(t *testing.T) {
	sh := shellPath(t)

	woken := make(chan struct{})
	s := newServices(t, serviceHooks{onDeath: func() { close(woken) }})
	if err := s.start("proxy", sh, "-c", "exit 0"); err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case <-woken:
	case <-time.After(10 * time.Second):
		t.Fatal("a clean but unasked exit did not wake the run")
	}
	if died := s.died(); died == nil || !strings.Contains(died.Error(), "proxy") {
		t.Errorf("a clean unasked exit should be reported and named, got %v", died)
	}
	_ = stopWithin(t, s, 30*time.Second)
}

// The mirror of the test above, and the one that matters more: an exit the
// runner asked for must wake nothing.
//
// The callback is the run's cancel function. Pulling it during a normal
// shutdown would cancel the context the statistics snapshot and the trace
// collection still need, so a run that produced every execution would fail
// while being archived.
func TestAShutdownTheRunnerAskedForWakesNothing(t *testing.T) {
	sh := shellPath(t)

	woke := make(chan struct{}, 4)
	s := newServices(t, serviceHooks{onDeath: func() { woke <- struct{}{} }})

	script, ready := trapping(t, "exit 0")
	if err := s.start("proxy", sh, "-c", script); err != nil {
		t.Fatalf("start: %v", err)
	}
	ready()

	if err := stopWithin(t, s, 30*time.Second); err != nil {
		t.Fatalf("a clean shutdown should not be an error: %v", err)
	}
	// The watcher runs concurrently with Stop's return; give it room to be wrong.
	time.Sleep(200 * time.Millisecond)

	select {
	case <-woke:
		t.Error("a shutdown the runner asked for cancelled the run; a run that succeeded would fail while being archived")
	default:
	}
	if died := s.died(); died != nil {
		t.Errorf("an exit during shutdown is the shutdown working, not a death: %v", died)
	}
}

// The same distinction by the other route: the services share the runner's
// process group, so a terminal interrupt reaches them at the same instant it
// reaches the runner. Recording their obedience as a death would put a
// fabricated cause in the bundle in place of the operator's interrupt.
func TestAServiceObeyingTheOperatorsInterruptIsNotADeath(t *testing.T) {
	sh := shellPath(t)

	interrupted, cancel := context.WithCancel(context.Background())
	woke := make(chan struct{}, 4)
	s := newServices(t, serviceHooks{
		onDeath:     func() { woke <- struct{}{} },
		interrupted: func() bool { return interrupted.Err() != nil },
	})

	// The operator interrupts; the services take the same signal and exit.
	cancel()
	if err := s.start("proxy", sh, "-c", "exit 0"); err != nil {
		t.Fatalf("start: %v", err)
	}
	reaped(t, s, 0)
	time.Sleep(200 * time.Millisecond)

	if died := s.died(); died != nil {
		t.Errorf("a service obeying the operator's interrupt was recorded as a death: %v", died)
	}
	select {
	case <-woke:
		t.Error("the interrupt was already cancelling the run; the data plane pulled it again")
	default:
	}
	_ = stopWithin(t, s, 30*time.Second)
}

// A service that refuses its configuration exits in milliseconds. Waiting for a
// listener it will never open would spend the whole startup timeout to report
// that nothing was listening — true, and silent about the process that said why.
func TestAServiceThatNeverListensFailsFastAndByName(t *testing.T) {
	sh := shellPath(t)

	s := newServices(t, serviceHooks{})
	start := time.Now()
	// Port 1 on the loopback: nothing will ever accept there.
	err := s.startAndWait(context.Background(), "proxy", sh, "127.0.0.1:1", "-c", "exit 2")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a service that exited before listening must fail its startup")
	}
	if !strings.Contains(err.Error(), "proxy") {
		t.Errorf("the error %q does not name the service", err)
	}
	if !strings.Contains(err.Error(), "exit status 2") {
		t.Errorf("the error %q does not carry how it exited", err)
	}
	// The claim is that the exit is noticed, not merely that the wait is shorter,
	// so the bound is the scale of a process exiting rather than a fraction of
	// the timeout it replaces.
	if elapsed > 2*time.Second {
		t.Errorf("startup took %s; it waited for a listener instead of noticing the exit", elapsed)
	}

	// The exit status has to survive being read here, because Stop reads it too.
	if err := stopWithin(t, s, 30*time.Second); err == nil {
		t.Error("Stop should report the service that exited on its own")
	}
}

// Waiting for a shutdown is what keeps a stream from being archived half
// flushed. Waiting forever is how a run ends with no bundle and no line saying
// why, so the wait is bounded and the truncation is reported.
func TestStopKillsAServiceThatWillNotExit(t *testing.T) {
	if testing.Short() {
		t.Skip("this test spends the shutdown grace period")
	}
	sh := shellPath(t)

	s := newServices(t, serviceHooks{})
	// Ignores SIGINT and outlives the grace period.
	script, ready := trapping(t, "")
	if err := s.start("adapter", sh, "-c", script); err != nil {
		t.Fatalf("start: %v", err)
	}
	ready()

	start := time.Now()
	err := stopWithin(t, s, shutdownGrace+20*time.Second)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a service that had to be killed must be reported")
	}
	if !strings.Contains(err.Error(), "adapter") || !strings.Contains(err.Error(), "killed") {
		t.Errorf("the error %q should name the service and say it was killed", err)
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Errorf("the error %q should say what the kill may have cost", err)
	}
	if elapsed < shutdownGrace {
		t.Errorf("Stop returned after %s; it did not give the service its grace period", elapsed)
	}
}

// A service asked to stop, that stops, is not a failure — and Stop is called
// from a deferred path as well as explicitly, so it has to survive being called
// twice.
func TestStopIsQuietAndIdempotentForACleanShutdown(t *testing.T) {
	sh := shellPath(t)

	s := newServices(t, serviceHooks{})
	// The real services trap SIGINT, flush their stream and exit zero; anything
	// else is a shutdown that may have cost evidence, so the fixture exits zero.
	script, ready := trapping(t, "exit 0")
	if err := s.start("proxy", sh, "-c", script); err != nil {
		t.Fatalf("start: %v", err)
	}
	ready()

	if err := stopWithin(t, s, 30*time.Second); err != nil {
		t.Errorf("a service that stopped when asked should not be an error: %v", err)
	}
	if err := stopWithin(t, s, 5*time.Second); err != nil {
		t.Errorf("a second Stop should be a no-op, got %v", err)
	}
}

// A service that exits on its own after the shutdown has begun is signalled by
// a Stop arriving second. The signal fails because the watcher already reaped
// it, and reporting that failure would replace the real exit status — sitting
// in the channel one line below — with the news that the process had finished.
func TestStopDoesNotReportArrivingSecond(t *testing.T) {
	sh := shellPath(t)

	s := newServices(t, serviceHooks{})
	// Exits on its own, unasked, and is reaped before Stop begins.
	if err := s.start("adapter", sh, "-c", "exit 0"); err != nil {
		t.Fatalf("start adapter: %v", err)
	}
	reaped(t, s, 0)

	// Stop's own diagnosis would mask the signal error, so this asserts on the
	// wording rather than on Stop being quiet: the death is real and reported,
	// but "already finished" is this code arriving second, not a fault.
	err := stopWithin(t, s, 30*time.Second)
	if err != nil && strings.Contains(err.Error(), "already finished") {
		t.Errorf("Stop reported arriving second as a fault: %v", err)
	}
}
