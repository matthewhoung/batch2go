package runner

import (
	"context"
	"fmt"

	"github.com/matthewhoung/batch2go/internal/client/direct"
	"github.com/matthewhoung/batch2go/internal/client/shared"
	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/manifest"
	"github.com/matthewhoung/batch2go/internal/triton"
)

// pathResult is what one logical request produced, as the load generator sees it.
type pathResult struct {
	Membership []identity.UID
	BatchSize  int
}

// path is how a cell's logical requests leave the load generator.
//
// The two implementations are deliberately not interchangeable behind the
// scenes: D0 reaches Triton and cannot build an envelope, the shared path
// reaches the proxy and cannot reach Triton. This interface is where the runner
// selects between them, and cellPath is the only place that selection happens —
// so a cell can never end up on the wrong one by accident.
type path interface {
	Send(ctx context.Context, member identity.LogicalRequest) (pathResult, error)
	LogicalBytes() int
	Close() error
}

// cellPath builds the path a cell's factor levels require.
func cellPath(m *manifest.Manifest, gw *triton.Gateway, model string, graph graphShape) (path, error) {
	if m.Cell.UsesProxy() {
		client, err := shared.Dial(shared.Config{
			ProxyEndpoint:         m.Transport.ProxyEndpoint,
			MaxMessageBytes:       m.Transport.MaxMessageBytes,
			InitialWindowSize:     m.Transport.InitialWindowSize,
			InitialConnWindowSize: m.Transport.InitialConnWindowSize,
		}, m.Run, buildPayload(graph.PayloadFloats))
		if err != nil {
			return nil, err
		}
		return &sharedPath{client: client}, nil
	}

	submitter, err := triton.NewSubmitter(gw, graph.FeatureWidth, graph.PayloadFloats)
	if err != nil {
		return nil, err
	}
	client, err := direct.New(m.Cell, submitter, model)
	if err != nil {
		return nil, err
	}
	return &directPath{client: client}, nil
}

// graphShape is the model's tensor geometry, which both paths need to build
// requests of the declared payload size.
type graphShape struct {
	FeatureWidth  int
	PayloadFloats int
}

// buildPayload renders the declared padding once. Its contents are
// scientifically irrelevant — only its size is realized on every hop — so it is
// built once per run and sent by reference.
func buildPayload(payloadFloats int) []byte {
	return make([]byte, payloadFloats*4)
}

type directPath struct{ client *direct.Client }

func (p *directPath) Send(ctx context.Context, member identity.LogicalRequest) (pathResult, error) {
	out, err := p.client.Send(ctx, member)
	if err != nil {
		return pathResult{}, err
	}
	return pathResult{Membership: out.Membership, BatchSize: out.BatchSize}, nil
}

func (p *directPath) LogicalBytes() int { return p.client.LogicalBytes() }
func (p *directPath) Close() error      { return nil }

type sharedPath struct{ client *shared.Client }

// Send carries no barrier release instant. The seal is a stage the load
// generator owns and records at A=off, and the proxy mints its own at A=on
// (ADR-0001); neither end reads one sent from here.
func (p *sharedPath) Send(ctx context.Context, member identity.LogicalRequest) (pathResult, error) {
	out, err := p.client.Send(ctx, member)
	if err != nil {
		return pathResult{}, err
	}
	return pathResult{Membership: out.Membership, BatchSize: out.BatchSize}, nil
}

func (p *sharedPath) LogicalBytes() int { return p.client.LogicalBytes() }
func (p *sharedPath) Close() error      { return p.client.Close() }

// checkCellImplemented refuses a cell this build cannot run, rather than falling
// back to something that would look like a result.
func checkCellImplemented(cell identity.Cell) error {
	switch cell {
	case identity.CellD0, identity.CellF00:
		return nil
	default:
		return fmt.Errorf("runner: cell %s is not executable by this build; spec 0001 implements D0 and F00", cell)
	}
}
