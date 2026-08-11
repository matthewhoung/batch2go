package runner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/matthewhoung/batch2go/internal/events/clockdomain"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/manifest"
	"github.com/matthewhoung/batch2go/internal/modelrepo"
)

// DefaultBinDir is where the data-plane binaries are expected. `make build`
// puts them there.
const DefaultBinDir = "bin"

// services are the data-plane processes a cell's path needs beyond the runner.
//
// The proxy and the adapter run as real, separate processes rather than as
// goroutines. That is not ceremony: the transport between them is a hop the
// design measures, their timestamps have to come from genuinely separate
// processes for the cross-process clock rules to mean anything, and each binary
// has to stay independently deployable so that Env 2 can put the proxy on a
// different node (ARCHITECTURE §10).
type services struct {
	procs []*proc
	hooks serviceHooks

	mu sync.Mutex
	// stopping distinguishes the two ways a service exits. After Stop has begun,
	// an exit is the shutdown working; before it, an exit is the run losing a
	// process it needs.
	stopping bool
	// died is the first such loss, worded once. It is the run's real cause: a
	// dead service makes every request in flight fail, and reporting one of those
	// failures names the symptom and buries what produced it.
	firstDeath error
}

// serviceHooks are what the run lends its data plane so a service's exit can be
// interpreted rather than merely noticed.
type serviceHooks struct {
	// onDeath is pulled when a service exits before anyone asked it to. It
	// cancels the run's context so the requests in flight fail immediately
	// instead of each waiting out its own deadline against a process that is
	// gone.
	onDeath func()

	// interrupted reports whether the operator has asked the whole run to stop.
	// The services share the runner's process group, so a terminal interrupt
	// reaches them at the same instant it reaches the runner: without this, their
	// obedience would be recorded as a death and the interrupt — the actual
	// cause — would not appear in the bundle at all.
	interrupted func() bool
}

// asked reports whether some exit now would be a service doing as it was told.
func (h serviceHooks) asked() bool { return h.interrupted != nil && h.interrupted() }

// proc is one launched service and the single place its exit status is read.
//
// exit is buffered and written exactly once, by the watcher goroutine, because
// exec.Cmd.Wait may be called only once — so the watcher owns it and Stop reads
// the result rather than calling Wait itself.
type proc struct {
	name string
	cmd  *exec.Cmd
	exit chan error
}

// shutdownGrace is how long a service has to flush its event stream and exit
// after SIGINT before it is killed.
//
// It is a code constant rather than a manifest parameter because it changes
// nothing that is measured: a service that needs longer than this has already
// failed, and killing it is how that becomes a verdict instead of a hang. It is
// generous relative to closing a stream and writing a counters sidecar.
const shutdownGrace = 10 * time.Second

// startupTimeout is how long a service has to open its listening socket. It is
// only reached by a service that started and then hung; one that refuses its
// configuration exits, and startAndWait reports that instead.
const startupTimeout = 30 * time.Second

// startServices launches the processes the cell needs and waits for them to
// accept connections.
func startServices(
	ctx context.Context,
	m *manifest.Manifest,
	clock *clockdomain.Domain,
	layout Layout,
	entry modelrepo.Entry,
	graph modelrepo.Graph,
	opts Options,
	hooks serviceHooks,
) (*services, error) {
	if !m.Cell.UsesProxy() {
		return &services{}, nil
	}

	binDir := opts.BinDir
	if binDir == "" {
		binDir = DefaultBinDir
	}
	adapterBin := filepath.Join(binDir, "adapter")
	proxyBin := filepath.Join(binDir, "proxy")
	for _, bin := range []string{adapterBin, proxyBin} {
		if _, err := os.Stat(bin); err != nil {
			return nil, fmt.Errorf("runner: %s is not built (%v); run `make build`", bin, err)
		}
	}
	if m.Transport.AdapterEndpoint == "" {
		return nil, fmt.Errorf("runner: cell %s needs transport.adapter_endpoint", m.Cell)
	}

	s := &services{hooks: hooks}
	// The adapter starts first so the proxy has something to dial.
	if err := s.startAndWait(ctx, "adapter", adapterBin, m.Transport.AdapterEndpoint,
		"--listen", m.Transport.AdapterEndpoint,
		"--triton", m.Transport.TritonEndpoint,
		"--model", entry.Name,
		"--events", layout.StreamPath(identity.EmitterAdapter),
		"--experiment", string(m.Experiment),
		"--session", string(m.Session),
		"--run", string(m.Run),
		"--cell", string(m.Cell),
		"--clock-domain", string(clock.ID),
		"--target-b", strconv.Itoa(m.Cohort.Size),
		"--feature-width", strconv.Itoa(graph.FeatureWidth),
		"--payload-floats", strconv.Itoa(graph.PayloadFloats),
		"--max-message-bytes", strconv.Itoa(m.Transport.MaxMessageBytes),
		"--initial-window-size", strconv.Itoa(int(m.Transport.InitialWindowSize)),
		"--initial-conn-window-size", strconv.Itoa(int(m.Transport.InitialConnWindowSize)),
	); err != nil {
		s.Stop()
		return nil, s.explain(err)
	}

	if err := s.startAndWait(ctx, "proxy", proxyBin, m.Transport.ProxyEndpoint,
		"--listen", m.Transport.ProxyEndpoint,
		"--backend", m.Transport.AdapterEndpoint,
		"--events", layout.StreamPath(identity.EmitterProxy),
		"--experiment", string(m.Experiment),
		"--session", string(m.Session),
		"--run", string(m.Run),
		"--cell", string(m.Cell),
		"--clock-domain", string(clock.ID),
		"--target-b", strconv.Itoa(m.Cohort.Size),
		"--formation-deadline-millis", strconv.Itoa(m.Cohort.FormationDeadlineMillis),
		"--max-message-bytes", strconv.Itoa(m.Transport.MaxMessageBytes),
		"--initial-window-size", strconv.Itoa(int(m.Transport.InitialWindowSize)),
		"--initial-conn-window-size", strconv.Itoa(int(m.Transport.InitialConnWindowSize)),
	); err != nil {
		// A service that died while the next one was starting cancelled this
		// context, so err is "context canceled" and names nobody. explain puts the
		// death back in front of the symptom it produced.
		s.Stop()
		return nil, s.explain(err)
	}

	opts.Logf("data plane up: proxy %s -> adapter %s", m.Transport.ProxyEndpoint, m.Transport.AdapterEndpoint)
	return s, nil
}

// startAndWait launches a service and blocks until it is accepting connections
// or has proved it never will.
//
// The two outcomes are watched together on purpose. A service that refuses its
// configuration exits in milliseconds, and waiting for a listener it will never
// open would spend the whole startup timeout to report that nothing was
// listening — true, and silent about the process that said why on its way out.
func (s *services) startAndWait(ctx context.Context, name, bin, endpoint string, args ...string) error {
	if err := s.start(name, bin, args...); err != nil {
		return err
	}
	p := s.procs[len(s.procs)-1]

	listening := make(chan error, 1)
	go func() { listening <- waitForListener(ctx, endpoint, startupTimeout) }()

	select {
	case err := <-listening:
		return err
	case err := <-p.exit:
		// Put it back: Stop reads this channel too, and the exit status is written
		// exactly once.
		p.exit <- err
		return fmt.Errorf("runner: the %s exited before it began listening on %s (%s)", name, endpoint, describeExit(err))
	}
}

// start launches one service and puts a watcher on it.
//
// The watcher is the only caller of Wait, so the exit status is read exactly
// once and is available to whoever needs it: Stop, to know the service went
// down cleanly, or the run, to learn it went down at all.
func (s *services) start(name, bin string, args ...string) error {
	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("runner: start %s: %w", name, err)
	}

	p := &proc{name: name, cmd: cmd, exit: make(chan error, 1)}
	s.procs = append(s.procs, p)
	go s.watch(p)
	return nil
}

// watch reaps one service and decides which kind of exit it was.
func (s *services) watch(p *proc) {
	err := p.cmd.Wait()
	p.exit <- err

	// Two exits look identical and mean opposite things. One is the shutdown
	// working; the other is the run losing a process it needs. Stop's flag names
	// the first, and the operator's interrupt names it too — the services share
	// the runner's process group, so a terminal SIGINT reaches them at the same
	// instant, and recording their obedience as a death would put a fabricated
	// cause in the bundle in place of the real one.
	s.mu.Lock()
	unexpected := !s.stopping && !s.hooks.asked() && s.firstDeath == nil
	if unexpected {
		s.firstDeath = fmt.Errorf(
			"runner: the %s exited before the run was done with it (%s); the data plane was incomplete from that moment and so is the run's evidence",
			p.name, describeExit(err))
	}
	s.mu.Unlock()

	// Pulled outside the lock: cancelling the run wakes the request goroutines,
	// which is work that must not happen while Stop may be holding the mutex.
	if unexpected && s.hooks.onDeath != nil {
		s.hooks.onDeath()
	}
}

// describeExit words a service's exit status without editorialising about why.
// A zero exit is reported as a zero exit; whether that was a loss is decided by
// the caller, which knows whether anyone had asked.
func describeExit(err error) string {
	if err == nil {
		return "exit status 0"
	}
	return err.Error()
}

// died reports the first service to exit before the runner asked it to.
func (s *services) died() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstDeath
}

// explain prefers the data plane's own diagnosis over the symptom it produced.
//
// A service that exits mid-run makes every request in flight fail, so the error
// that reaches the caller is a request error naming a cohort and an ordinal. It
// is true and it is not the cause; the cause is that a process the run needs is
// gone, and this is where the two are put back together.
func (s *services) explain(err error) error {
	died := s.died()
	if died == nil || err == nil {
		return err
	}
	return fmt.Errorf("%w; the failure that surfaced it: %v", died, err)
}

// Stop shuts the processes down in reverse order and waits for them, bounded.
//
// Waiting matters: each service flushes and closes its event stream on shutdown,
// and archiving before that finishes would capture a partial stream and report
// missing evidence for requests that were in fact recorded. Bounding the wait
// matters for the mirror reason: a service that never finishes shutting down
// would hang the runner with the bundle unwritten and no line saying why, so
// after the grace period it is killed and the truncation is reported rather
// than waited on.
//
// A service that already exited is handled by the same path: its exit status is
// waiting in the channel. Signalling it fails with os.ErrProcessDone, because
// the watcher has already reaped it, and that is not a fault — it is this code
// arriving second. Reporting it would replace the real exit status, which is
// sitting in the channel one line below, with the news that the process had
// already finished.
func (s *services) Stop() error {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()

	// A death the run already diagnosed is the better error, and it is returned
	// ahead of whatever the shutdown makes of the corpse.
	firstErr := s.died()

	for i := len(s.procs) - 1; i >= 0; i-- {
		p := s.procs[i]
		if p.cmd.Process == nil {
			continue
		}
		if err := p.cmd.Process.Signal(os.Interrupt); err != nil &&
			!errors.Is(err, os.ErrProcessDone) && firstErr == nil {
			firstErr = fmt.Errorf("runner: signal %s: %w", p.name, err)
		}
		select {
		case err := <-p.exit:
			// A graceful stop exits zero; anything else means the service went down
			// in a way that may have cost evidence.
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("runner: %s exited badly: %w", p.name, err)
			}
		case <-time.After(shutdownGrace):
			_ = p.cmd.Process.Kill()
			<-p.exit
			if firstErr == nil {
				firstErr = fmt.Errorf(
					"runner: the %s did not exit within %s of being asked and was killed; its event stream may be truncated",
					p.name, shutdownGrace)
			}
		}
	}
	s.procs = nil
	return firstErr
}

// waitForListener blocks until the endpoint accepts a connection.
func waitForListener(ctx context.Context, endpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", endpoint, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("runner: nothing listening on %s within %s (last: %v)", endpoint, timeout, lastErr)
}
