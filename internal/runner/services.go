package runner

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
	procs []*exec.Cmd
	names []string
}

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

	s := &services{}
	// The adapter starts first so the proxy has something to dial.
	if err := s.start(ctx, opts, "adapter", adapterBin,
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
		return nil, err
	}
	if err := waitForListener(ctx, m.Transport.AdapterEndpoint, 30*time.Second); err != nil {
		s.Stop()
		return nil, err
	}

	if err := s.start(ctx, opts, "proxy", proxyBin,
		"--listen", m.Transport.ProxyEndpoint,
		"--backend", m.Transport.AdapterEndpoint,
		"--events", layout.StreamPath(identity.EmitterProxy),
		"--experiment", string(m.Experiment),
		"--session", string(m.Session),
		"--run", string(m.Run),
		"--cell", string(m.Cell),
		"--clock-domain", string(clock.ID),
		"--target-b", strconv.Itoa(m.Cohort.Size),
		"--max-message-bytes", strconv.Itoa(m.Transport.MaxMessageBytes),
		"--initial-window-size", strconv.Itoa(int(m.Transport.InitialWindowSize)),
		"--initial-conn-window-size", strconv.Itoa(int(m.Transport.InitialConnWindowSize)),
	); err != nil {
		s.Stop()
		return nil, err
	}
	if err := waitForListener(ctx, m.Transport.ProxyEndpoint, 30*time.Second); err != nil {
		s.Stop()
		return nil, err
	}

	opts.Logf("data plane up: proxy %s -> adapter %s", m.Transport.ProxyEndpoint, m.Transport.AdapterEndpoint)
	return s, nil
}

func (s *services) start(ctx context.Context, opts Options, name, bin string, args ...string) error {
	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("runner: start %s: %w", name, err)
	}
	s.procs = append(s.procs, cmd)
	s.names = append(s.names, name)
	return nil
}

// Stop shuts the processes down in reverse order and waits for them.
//
// Waiting matters: each service flushes and closes its event stream on shutdown,
// and archiving before that finishes would capture a partial stream and report
// missing evidence for requests that were in fact recorded.
func (s *services) Stop() error {
	var firstErr error
	for i := len(s.procs) - 1; i >= 0; i-- {
		cmd, name := s.procs[i], s.names[i]
		if cmd.Process == nil {
			continue
		}
		if err := cmd.Process.Signal(os.Interrupt); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("runner: signal %s: %w", name, err)
		}
		if err := cmd.Wait(); err != nil && firstErr == nil {
			// A graceful stop exits zero; anything else means the service died in a
			// way that may have cost evidence.
			firstErr = fmt.Errorf("runner: %s exited badly: %w", name, err)
		}
	}
	s.procs = nil
	s.names = nil
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
