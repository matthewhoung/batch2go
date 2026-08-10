package triton

import (
	"context"
	"fmt"
	"sync"

	tritonv2 "github.com/matthewhoung/batch2go/api/triton/v2"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// Tensor names of the synthetic model. They are the contract between the model
// generator and this gateway; a model missing any of them is rejected at
// readiness rather than at the first request.
const (
	InputData    = "data"
	InputPadding = "padding"
	InputUID     = "uid"
	OutputData   = "data_out"
	OutputUIDSet = "uid_set"
)

// Submitter is the single-request submission engine.
//
// All four singles-submitting cells — D0, F00, F01, F10 — share it. That sharing
// is the point: the proxy-path tax contrast compares paths, so it must not also
// compare two client implementations. The only thing that varies between cells is
// which model entry the request targets (ARCHITECTURE §6.4).
type Submitter struct {
	gw            *Gateway
	featureWidth  int
	payloadFloats int

	// data and padding are built once and reused by reference on every request.
	// Their contents are scientifically irrelevant — only their size is — so
	// rebuilding them per request would add allocation to the hot path for
	// nothing, and A=off cells would pay it B times more often than A=on.
	data    []byte
	padding []byte

	uidBufs sync.Pool
}

// NewSubmitter builds a submission engine for one model shape.
func NewSubmitter(gw *Gateway, featureWidth, payloadFloats int) (*Submitter, error) {
	if featureWidth <= 0 {
		return nil, fmt.Errorf("triton: feature width must be positive, got %d", featureWidth)
	}
	if payloadFloats <= 0 {
		return nil, fmt.Errorf("triton: payload must be at least one element, got %d", payloadFloats)
	}

	s := &Submitter{gw: gw, featureWidth: featureWidth, payloadFloats: payloadFloats}
	s.uidBufs.New = func() any {
		b := make([]byte, 0, 8*16)
		return &b
	}

	feature := make([]float32, featureWidth)
	for i := range feature {
		feature[i] = 0.01
	}
	s.data = float32SliceToBytes(make([]byte, 0, 4*featureWidth), feature)

	pad := make([]float32, payloadFloats)
	for i := range pad {
		pad[i] = 1.0
	}
	s.padding = float32SliceToBytes(make([]byte, 0, 4*payloadFloats), pad)
	return s, nil
}

// LogicalBytes is the wire size of one logical request's tensors: the payload
// plus the feature and uid tensors. It is recorded per event, not estimated.
func (s *Submitter) LogicalBytes() int { return len(s.data) + len(s.padding) + 8 }

// Result is the evidence one submission produced. It carries no conclusions:
// whether the observed membership is correct for the cell is the validator's
// judgment, made offline from the archived bundle.
type Result struct {
	Members []identity.LogicalRequest

	// Membership is the complete uid set the execution attested to. For a
	// correct unbatched execution this is exactly the submitted member; anything
	// larger means the scheduler coalesced, which is what makes the attestation
	// worth collecting (ADR-0007).
	Membership []identity.UID

	// BatchSize is the execution's realized shape, taken from the attested set's
	// size — a fact the model reported, not an inference from timestamps.
	BatchSize int

	DataOutBytes int
}

// Submit sends one request carrying the given members and returns its evidence.
//
// At the unbatched entry members has exactly one element. The signature takes a
// slice anyway because the executor seam is dispatch-shaped: later cells release
// n=B members in one call, and this engine should not need reshaping for them.
func (s *Submitter) Submit(ctx context.Context, model string, members []identity.LogicalRequest) (Result, error) {
	if len(members) == 0 {
		return Result{}, fmt.Errorf("triton: submission carries no members")
	}
	n := int64(len(members))

	uidBuf := s.uidBufs.Get().(*[]byte)
	defer s.uidBufs.Put(uidBuf)
	uids := make([]int64, 0, len(members))
	for _, m := range members {
		uids = append(uids, int64(m.UID()))
	}
	*uidBuf = int64SliceToBytes((*uidBuf)[:0], uids)

	// A batched request repeats the shared feature and padding tensors per member.
	// At n=1 — every cell in this slice — the shared buffers are used as they are.
	data, padding := s.data, s.padding
	if n > 1 {
		data = repeat(s.data, int(n))
		padding = repeat(s.padding, int(n))
	}

	req := &tritonv2.ModelInferRequest{
		ModelName: model,
		Id:        events.TritonRequestID(members[0]),
		Inputs: []*tritonv2.ModelInferRequest_InferInputTensor{
			{Name: InputData, Datatype: "FP32", Shape: []int64{n, int64(s.featureWidth)}},
			{Name: InputPadding, Datatype: "FP32", Shape: []int64{n, int64(s.payloadFloats)}},
			{Name: InputUID, Datatype: "INT64", Shape: []int64{n, 1}},
		},
		Outputs: []*tritonv2.ModelInferRequest_InferRequestedOutputTensor{
			{Name: OutputData},
			{Name: OutputUIDSet},
		},
		RawInputContents: [][]byte{data, padding, *uidBuf},
	}

	resp, err := s.gw.client.ModelInfer(ctx, req)
	if err != nil {
		return Result{}, fmt.Errorf("triton: infer %s (%s): %w", model, req.Id, err)
	}
	return parseResponse(resp, members)
}

// parseResponse validates the response's shape and byte counts before splitting
// it. A response that does not describe what was asked for is a failure, not
// something to interpret leniently.
func parseResponse(resp *tritonv2.ModelInferResponse, members []identity.LogicalRequest) (Result, error) {
	outputs := resp.GetOutputs()
	raw := resp.GetRawOutputContents()
	if len(outputs) != len(raw) {
		return Result{}, fmt.Errorf("triton: response has %d output descriptors and %d payloads", len(outputs), len(raw))
	}

	res := Result{Members: members}
	var sawUIDSet bool
	for i, out := range outputs {
		switch out.GetName() {
		case OutputData:
			res.DataOutBytes = len(raw[i])
		case OutputUIDSet:
			sawUIDSet = true
			shape := out.GetShape()
			if len(shape) != 2 {
				return Result{}, fmt.Errorf("triton: %s has shape %v, want two dimensions", OutputUIDSet, shape)
			}
			if shape[0] != int64(len(members)) {
				return Result{}, fmt.Errorf("triton: %s reports %d rows for %d submitted members",
					OutputUIDSet, shape[0], len(members))
			}
			values, err := bytesToInt64Slice(raw[i])
			if err != nil {
				return Result{}, err
			}
			if int64(len(values)) != shape[0]*shape[1] {
				return Result{}, fmt.Errorf("triton: %s carries %d values for shape %v", OutputUIDSet, len(values), shape)
			}

			// Every row must attest the same set — it is one execution's membership,
			// returned to each of its members. Rows that disagree mean the model is
			// not attesting what it claims to.
			width := int(shape[1])
			for row := 1; row < int(shape[0]); row++ {
				for col := 0; col < width; col++ {
					if values[row*width+col] != values[col] {
						return Result{}, fmt.Errorf(
							"triton: %s rows disagree about the execution's membership at row %d column %d",
							OutputUIDSet, row, col)
					}
				}
			}
			res.Membership = make([]identity.UID, 0, width)
			for _, v := range values[:width] {
				res.Membership = append(res.Membership, identity.UID(v))
			}
			res.BatchSize = width
		}
	}
	if !sawUIDSet {
		return Result{}, fmt.Errorf("triton: response carries no %s output; membership cannot be attested", OutputUIDSet)
	}
	return res, nil
}

func repeat(b []byte, n int) []byte {
	out := make([]byte, 0, len(b)*n)
	for i := 0; i < n; i++ {
		out = append(out, b...)
	}
	return out
}
