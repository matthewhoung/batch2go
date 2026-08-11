// Command proxy runs the shared path's entry point.
//
// It is an independently deployable binary so that Env 2 can place it with the
// load generator on a separate node. This file parses flags and wires
// components; what the proxy does at each factor level lives in internal/proxy.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/events/clockdomain"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/proxy"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "proxy: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", "127.0.0.1:9100", "address to serve the client protocol on")
	backend := flag.String("backend", "127.0.0.1:9101", "adapter endpoint")
	eventsPath := flag.String("events", "", "event record stream to write")

	experiment := flag.String("experiment", "", "experiment id")
	session := flag.String("session", "", "session id")
	runID := flag.String("run", "", "run id")
	cell := flag.String("cell", "", "cell being served")
	clockDomainID := flag.String("clock-domain", "", "clock domain the run declared")
	targetB := flag.Int("target-b", 0, "cohort size the run declared")

	// No default: at A=on this bounds how long a cohort is held before it is
	// failed whole, which is an experimental quantity the manifest declares. A
	// zero here is refused by proxy.Config for an A=on cell rather than standing
	// in for a number nobody wrote down (ADR-0010).
	formationDeadlineMillis := flag.Int("formation-deadline-millis", 0, "how long the proxy may hold a partly assembled cohort (A=on only)")

	maxMessageBytes := flag.Int("max-message-bytes", 256<<20, "gRPC message ceiling")
	initialWindow := flag.Int("initial-window-size", 4<<20, "gRPC stream flow-control window")
	initialConnWindow := flag.Int("initial-conn-window-size", 16<<20, "gRPC connection flow-control window")
	flag.Parse()

	if *eventsPath == "" || *runID == "" || *cell == "" {
		return fmt.Errorf("proxy needs --events, --run and --cell")
	}
	parsedCell, err := identity.ParseCell(*cell)
	if err != nil {
		return err
	}
	// Which cells this build runs is settled in one place, and the proxy consults
	// it as the adapter does. Until F10 landed, an unimplemented A=on cell was
	// refused by the service itself for being A=on at all; now that A=on is a
	// level the proxy serves, the cell has to be checked against the authority
	// rather than against a property it shares with cells that do run.
	if err := parsedCell.CheckImplemented(); err != nil {
		return err
	}

	clock, err := clockdomain.Establish()
	if err != nil {
		return err
	}
	if *clockDomainID != "" && string(clock.ID) != *clockDomainID {
		return fmt.Errorf("proxy is in clock domain %s, the run declared %s; their timestamps cannot be subtracted",
			clock.ID, *clockDomainID)
	}

	conn, err := grpc.NewClient(*backend,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(*maxMessageBytes),
			grpc.MaxCallSendMsgSize(*maxMessageBytes),
		),
		grpc.WithInitialWindowSize(int32(*initialWindow)),
		grpc.WithInitialConnWindowSize(int32(*initialConnWindow)),
	)
	if err != nil {
		return fmt.Errorf("proxy: dial adapter %s: %w", *backend, err)
	}
	defer conn.Close()

	builder, err := envelope.NewBuilder(envelope.Config{
		Experiment:  identity.ExperimentID(*experiment),
		Session:     identity.SessionID(*session),
		Run:         identity.RunID(*runID),
		Cell:        parsedCell,
		ClockDomain: clock.ID,
		TargetB:     *targetB,
	})
	if err != nil {
		return err
	}

	writer, err := events.NewFileWriter(*eventsPath, events.RunHeader{
		Experiment:  identity.ExperimentID(*experiment),
		Session:     identity.SessionID(*session),
		Run:         identity.RunID(*runID),
		Cell:        parsedCell,
		ClockDomain: clock.ID,
		WriterID:    2,
	})
	if err != nil {
		return err
	}
	defer writer.Close()

	formationDeadline := time.Duration(*formationDeadlineMillis) * time.Millisecond
	service, err := proxy.New(proxy.Config{
		Cell:              parsedCell,
		Run:               identity.RunID(*runID),
		TargetB:           *targetB,
		FormationDeadline: formationDeadline,
	}, builder, envelopev1.NewBackendClient(conn), writer, clock.Now)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("proxy: listen on %s: %w", *listen, err)
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(*maxMessageBytes),
		grpc.MaxSendMsgSize(*maxMessageBytes),
		grpc.InitialWindowSize(int32(*initialWindow)),
		grpc.InitialConnWindowSize(int32(*initialConnWindow)),
	)
	envelopev1.RegisterProxyServer(server, service)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		// Release the cohorts still assembling before waiting on the handlers.
		// GracefulStop waits for every in-flight call to return, and a held member
		// is a call that will not return until its cohort does — so without this
		// the shutdown would last as long as the formation deadline, or as long as
		// the client's request timeout for a cohort whose remaining members are
		// never coming because the load generator is stopping too.
		service.Close()
		server.GracefulStop()
	}()

	// The line names the factor level rather than the cell alone, because the two
	// behaviours differ in exactly the way the experiment is measuring and an
	// operator reading a log should not have to know the contract table.
	mode := "pass-through"
	if parsedCell.AggregatesEnvelopes() {
		mode = fmt.Sprintf("cohort formation at B=%d, deadline %s", *targetB, formationDeadline)
	}
	fmt.Fprintf(os.Stderr, "proxy: %s for %s on %s -> %s (clock domain %s)\n",
		mode, parsedCell, *listen, *backend, clock.ID)
	if err := server.Serve(lis); err != nil {
		return fmt.Errorf("proxy: serve: %w", err)
	}
	if err := writer.Close(); err != nil {
		return err
	}
	// The counters live in this process, so they are persisted beside the stream:
	// a record this service dropped has to remain reportable after it exits.
	return events.WriteStats(*eventsPath, writer.Stats())
}
