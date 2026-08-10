// Command adapter runs the backend adapter: it terminates the envelope protocol
// and dispatches to an executor.
//
// It is an independently deployable binary so that Env 2 can place LoadGen and
// Proxy on a separate node. This file parses flags and wires components; the
// measurement logic lives in internal/adapter and its executor.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/adapter"
	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/events/clockdomain"
	"github.com/matthewhoung/batch2go/internal/executor/individual"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/triton"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "adapter: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", "127.0.0.1:9101", "address to serve the backend protocol on")
	tritonEndpoint := flag.String("triton", "127.0.0.1:8001", "Triton gRPC endpoint")
	model := flag.String("model", "", "Triton model entry to submit against")
	eventsPath := flag.String("events", "", "event record stream to write")

	experiment := flag.String("experiment", "", "experiment id")
	session := flag.String("session", "", "session id")
	runID := flag.String("run", "", "run id")
	cell := flag.String("cell", "", "cell being served")
	clockDomainID := flag.String("clock-domain", "", "clock domain the run declared")
	targetB := flag.Int("target-b", 0, "cohort size the run declared")

	featureWidth := flag.Int("feature-width", 0, "model feature width")
	payloadFloats := flag.Int("payload-floats", 0, "model padding width in float32 elements")
	maxMessageBytes := flag.Int("max-message-bytes", triton.MaxMessageBytes, "gRPC message ceiling")
	initialWindow := flag.Int("initial-window-size", 4<<20, "gRPC stream flow-control window")
	initialConnWindow := flag.Int("initial-conn-window-size", 16<<20, "gRPC connection flow-control window")
	flag.Parse()

	if *model == "" || *eventsPath == "" || *runID == "" || *cell == "" {
		return fmt.Errorf("adapter needs --model, --events, --run and --cell")
	}
	parsedCell, err := identity.ParseCell(*cell)
	if err != nil {
		return err
	}

	// Every process establishes its own clock domain and checks it against the
	// one the run declared. Agreeing is what makes cross-process subtraction
	// legitimate; disagreeing means the processes are not on one clock and the
	// run must stop rather than produce timestamps nobody may subtract.
	clock, err := clockdomain.Establish()
	if err != nil {
		return err
	}
	if *clockDomainID != "" && string(clock.ID) != *clockDomainID {
		return fmt.Errorf("adapter is in clock domain %s, the run declared %s; their timestamps cannot be subtracted",
			clock.ID, *clockDomainID)
	}

	gw, err := triton.Dial(triton.Config{
		Endpoint:              *tritonEndpoint,
		MaxMessageBytes:       *maxMessageBytes,
		InitialWindowSize:     int32(*initialWindow),
		InitialConnWindowSize: int32(*initialConnWindow),
	})
	if err != nil {
		return err
	}
	defer gw.Close()

	submitter, err := triton.NewSubmitter(gw, *featureWidth, *payloadFloats)
	if err != nil {
		return err
	}
	exec, err := individual.New(submitter, clock.Now)
	if err != nil {
		return err
	}

	writer, err := events.NewFileWriter(*eventsPath, events.RunHeader{
		Experiment:  identity.ExperimentID(*experiment),
		Session:     identity.SessionID(*session),
		Run:         identity.RunID(*runID),
		Cell:        parsedCell,
		ClockDomain: clock.ID,
		WriterID:    3,
	})
	if err != nil {
		return err
	}
	defer writer.Close()

	service, err := adapter.New(adapter.Config{
		Model: *model,
		Expectation: envelope.Expectation{
			Run:         identity.RunID(*runID),
			Cell:        parsedCell,
			ClockDomain: clock.ID,
			TargetB:     *targetB,
		},
	}, exec, writer, clock.Now)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("adapter: listen on %s: %w", *listen, err)
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(*maxMessageBytes),
		grpc.MaxSendMsgSize(*maxMessageBytes),
		grpc.InitialWindowSize(int32(*initialWindow)),
		grpc.InitialConnWindowSize(int32(*initialConnWindow)),
	)
	envelopev1.RegisterBackendServer(server, service)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	fmt.Fprintf(os.Stderr, "adapter: serving %s on %s (clock domain %s)\n", *model, *listen, clock.ID)
	if err := server.Serve(lis); err != nil {
		return fmt.Errorf("adapter: serve: %w", err)
	}
	// Closing the writer here rather than only in the deferred call makes the
	// stream complete before the process exits, so the runner never archives a
	// half-flushed bank.
	if err := writer.Close(); err != nil {
		return err
	}
	// The counters live in this process, so they are persisted beside the stream:
	// a record this service dropped has to remain reportable after it exits.
	return events.WriteStats(*eventsPath, writer.Stats())
}
