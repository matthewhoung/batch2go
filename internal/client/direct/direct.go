// Package direct is the D0 diagnostic's isolated client.
//
// D0 measures the proxy-path tax by *not* traversing the shared path, which
// makes it the one place in the system where bypassing the proxy is correct. The
// risk is obvious: if a factorial cell ever ran through here, its measurement
// would silently omit the very hop the design says every factorial cell shares,
// and nothing about the resulting numbers would look wrong.
//
// So the isolation is structural rather than advisory. This package knows how to
// reach Triton and nothing else: it does not import the envelope protocol, the
// proxy, or the adapter, and it therefore *cannot* construct or send an
// envelope. The shared-path client is the mirror image — it can reach the proxy
// and cannot reach Triton. direct_test.go asserts both boundaries against the
// real import graph, and New refuses any cell but D0 on top of that.
package direct

import (
	"context"
	"fmt"

	"github.com/matthewhoung/batch2go/internal/identity"
	"github.com/matthewhoung/batch2go/internal/triton"
)

// Client sends one logical request at a time straight to the backend, using the
// same single-request submission policy the shared path uses. That sharing is
// what makes the proxy-path tax a contrast between paths rather than between two
// client implementations (ARCHITECTURE §6.4).
type Client struct {
	submitter *triton.Submitter
	model     string
}

// New builds the D0 client. It refuses any cell but D0: a factorial cell asking
// for a direct path is a configuration error that must surface before a run, not
// a preference to be honoured.
func New(cell identity.Cell, submitter *triton.Submitter, model string) (*Client, error) {
	if cell != identity.CellD0 {
		return nil, fmt.Errorf(
			"client/direct: cell %s traverses the shared path; only D0 may use the direct client", cell)
	}
	if submitter == nil {
		return nil, fmt.Errorf("client/direct: needs a submission engine")
	}
	if model == "" {
		return nil, fmt.Errorf("client/direct: needs a model entry to target")
	}
	return &Client{submitter: submitter, model: model}, nil
}

// LogicalBytes is the wire size of one logical request.
func (c *Client) LogicalBytes() int { return c.submitter.LogicalBytes() }

// Send submits one logical request and returns the evidence it produced.
func (c *Client) Send(ctx context.Context, member identity.LogicalRequest) (triton.Result, error) {
	return c.submitter.Submit(ctx, c.model, []identity.LogicalRequest{member})
}
