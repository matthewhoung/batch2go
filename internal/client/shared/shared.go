// Package shared is the load generator's client for the shared path.
//
// It is the mirror image of the D0 direct client. This package can reach the
// proxy and cannot reach Triton — it does not depend on the gateway, so there is
// no expression here that submits to the backend directly. A factorial cell
// therefore cannot skip the hop it is defined to traverse, and the direct client
// cannot construct an envelope; the boundary is checked against the real import
// graph in internal/client/direct.
package shared

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// Config is the client's transport configuration, pinned by the manifest.
type Config struct {
	ProxyEndpoint         string
	MaxMessageBytes       int
	InitialWindowSize     int32
	InitialConnWindowSize int32
}

// Result is one logical request's outcome as the client sees it.
type Result struct {
	Membership   []identity.UID
	BatchSize    int
	DataOutBytes int
}

// Client submits logical requests to the proxy.
type Client struct {
	conn    *grpc.ClientConn
	proxy   envelopev1.ProxyClient
	run     identity.RunID
	payload []byte
}

// Dial connects to the proxy.
//
// payload is the declared padding, built once and sent by reference on every
// request: its contents are scientifically irrelevant, only its size is, so
// rebuilding it per request would add allocation to the hot path — and at A=off
// a cohort pays that B times where A=on pays it once (ADR-0004).
func Dial(cfg Config, run identity.RunID, payload []byte) (*Client, error) {
	if cfg.ProxyEndpoint == "" {
		return nil, fmt.Errorf("client/shared: needs a proxy endpoint")
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("client/shared: needs a payload; the declared padding traverses every hop")
	}
	conn, err := grpc.NewClient(cfg.ProxyEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cfg.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(cfg.MaxMessageBytes),
		),
		grpc.WithInitialWindowSize(cfg.InitialWindowSize),
		grpc.WithInitialConnWindowSize(cfg.InitialConnWindowSize),
	)
	if err != nil {
		return nil, fmt.Errorf("client/shared: dial proxy %s: %w", cfg.ProxyEndpoint, err)
	}
	return &Client{conn: conn, proxy: envelopev1.NewProxyClient(conn), run: run, payload: payload}, nil
}

// Close releases the connection.
func (c *Client) Close() error { return c.conn.Close() }

// LogicalBytes is the wire size of one logical request's payload.
func (c *Client) LogicalBytes() int { return len(c.payload) }

// Send submits one logical request through the proxy.
//
// seal is the barrier release instant, carried because the load generator owns
// t_cohort_seal at A=off (ADR-0001). The client sends one request at a time
// whatever the cell's factor level: whether requests share an envelope is the
// proxy's decision, not the client's.
func (c *Client) Send(ctx context.Context, member identity.LogicalRequest, seal int64) (Result, error) {
	resp, err := c.proxy.Submit(ctx, &envelopev1.ClientRequest{
		RunId:       string(c.run),
		CohortId:    uint32(member.Cohort),
		Ordinal:     uint32(member.Ordinal),
		Uid:         int64(member.UID()),
		Payload:     c.payload,
		TCohortSeal: &seal,
	})
	if err != nil {
		return Result{}, fmt.Errorf("client/shared: submit %v: %w", member, err)
	}
	if resp.GetStatus() != envelopev1.Status_STATUS_OK {
		return Result{}, fmt.Errorf("client/shared: %v returned %s: %s", member, resp.GetStatus(), resp.GetError())
	}

	out := Result{BatchSize: int(resp.GetBatchSize()), DataOutBytes: int(resp.GetDataOutBytes())}
	for _, uid := range resp.GetMembershipUids() {
		out.Membership = append(out.Membership, identity.UID(uid))
	}
	return out, nil
}
