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
	"github.com/matthewhoung/batch2go/internal/executor"
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
	if err := parsedCell.CheckImplemented(); err != nil {
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
	exec, kind, err := newExecutor(parsedCell, submitter, clock.Now)
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

	// What this process actually wired, written before it serves anything.
	//
	// Every value comes from the live object rather than from the flag that built
	// it: the channel from the gateway, the tensor shape from the submitter, the
	// executor kind from the code that selected it. A record assembled from the
	// flags would restate the manifest the runner already archived, and two cells
	// would then agree on their configuration whatever their processes did — the
	// tautology this record exists to break.
	serving := adapter.ServingConfig{
		MaxMessageBytes:       *maxMessageBytes,
		InitialWindowSize:     int32(*initialWindow),
		InitialConnWindowSize: int32(*initialConnWindow),
	}
	if err := adapter.WriteProcessRecord(*eventsPath, adapter.ProcessRecord{
		SchemaVersion: adapter.ProcessRecordSchemaVersion,
		Experiment:    identity.ExperimentID(*experiment),
		Session:       identity.SessionID(*session),
		Run:           identity.RunID(*runID),
		Cell:          parsedCell,
		ClockDomain:   clock.ID,
		Executor:      kind,
		ModelEntry:    service.Model(),
		Downstream:    gw.Config(),
		Serving:       serving,
		FeatureWidth:  submitter.FeatureWidth(),
		PayloadFloats: submitter.PayloadFloats(),
	}); err != nil {
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

// newExecutor builds the executor the cell's declared factor levels selected.
//
// The selection is made in internal/executor, from the cell's own properties;
// this switch only names which implementation each kind resolves to. A kind
// with no case here cannot be reached — SelectKind refuses an unimplemented
// kind first — but the default is an error rather than the individual executor,
// because a silent fall back is exactly the failure this wiring exists to
// prevent: it would run a V=on cell as V=off and record it under the V=on label.
// It returns the kind it wired as well as the executor, because the bundle
// records what this process built rather than what the cell implies. Deriving
// the kind again at record time would report the second, and the record would
// then agree with the manifest by construction — which is exactly the tautology
// the record exists to break.
func newExecutor(cell identity.Cell, submitter *triton.Submitter, now executor.Clock) (executor.Executor, executor.Kind, error) {
	kind, err := executor.SelectKind(cell)
	if err != nil {
		return nil, "", err
	}
	switch kind {
	case executor.KindIndividual:
		exec, err := individual.New(submitter, now)
		return exec, kind, err
	default:
		return nil, "", fmt.Errorf("adapter: cell %s selected the %s executor, which this binary does not wire", cell, kind)
	}
}
