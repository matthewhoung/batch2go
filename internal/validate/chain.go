// Package validate is the Result Validator: pure functions over recorded
// evidence that decide whether a run did what its cell says it does.
//
// Two constraints shape this package, and both are deliberate.
//
// It imports only the event schema and the identity vocabulary — never the
// runner, the manifest, the gateway, or the model repository. What a run was
// supposed to produce arrives as plain data in an Expectation, translated by the
// caller. That keeps a verdict reproducible from an archived bundle alone, with
// no network and no live state, and it keeps the validator from sharing code
// with the path that emitted the evidence: agreement between two implementations
// is worth something, agreement by construction is worth nothing.
//
// And it never repairs. A missing timestamp is reported, not interpolated; a
// residual is reported signed, never relabeled; nothing is excluded silently.
package validate

import (
	"fmt"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// The cycle model's stage vocabulary (M1 §4):
//
//	C = W_form + A_pack + Σ_hop X_req + Q_backend + S_comp + Σ_hop X_resp + F_fanout
//
// Transfer terms are indexed by hop, so that Stage A fits and the conservation
// check are both well defined.
const (
	StageWForm     = "W_form"
	StageAPack     = "A_pack"
	StageXReq      = "X_req"
	StageXReqHop1  = "X_req_hop1" // LoadGen → Proxy
	StageXReqHop2  = "X_req_hop2" // Proxy → Adapter
	StageXReqHop3  = "X_req_hop3" // Adapter → Triton
	StageQBackend  = "Q_backend"
	StageSComp     = "S_comp"
	StageXResp     = "X_resp"
	StageXRespHop1 = "X_resp_hop1" // Triton → Adapter
	StageXRespHop2 = "X_resp_hop2" // Adapter → Proxy
	StageXRespHop3 = "X_resp_hop3" // Proxy → LoadGen
	StageFFanout   = "F_fanout"

	// Intervals the path really spends time in, but which the cycle model does
	// not name. They are measured and reported — as the residual, never folded
	// into a neighbouring stage.
	//
	// StageBarrierWait is deliberately NOT called W_form. W_form is the cycle
	// model's formation term and it is a property of the proxy: it is the time a
	// cohort spends being assembled where aggregation happens. At A=off nothing
	// assembles anything — the load generator releases B requests through the
	// barrier and each goes straight out — so the interval between the scheduled
	// release and the barrier's release is the generator's own wait, and it is the
	// record of the release jitter M1 §6 requires measured against timer
	// granularity. Naming both quantities W_form would put two physically
	// unrelated numbers in one archive column, keyed by a cell label nothing
	// checks.
	StageBarrierWait   = "barrier_wait"
	StageReleaseToSend = "release_to_send"
	StageAdapterUnpack = "adapter_unpack"
	StageResponsePack  = "response_pack"
)

// Span is one interval between two consecutive timestamps.
type Span struct {
	Name  string
	Start events.Stage
	End   events.Stage

	// Accounted marks the spans the cycle model names. The conservation residual
	// is the end-to-end path minus the sum of these — which is to say, it is the
	// time the model does not explain.
	//
	// This distinction is the whole content of the check. If every interval were
	// counted as a stage, the chain would telescope and the residual would be
	// zero by construction for any input whatsoever: an arithmetic identity
	// dressed up as a measurement. Leaving the cycle model's actual boundaries
	// where they are keeps the residual a real quantity, and one that grows if a
	// process starts spending time somewhere nobody modelled.
	Accounted bool
}

// Chain is a cell's timestamps in the order the path actually visits them,
// paired into named spans.
//
// The schema's numbering is not the traversal order. t_cohort_seal is number 4,
// but at A=off the load generator emits it at barrier release — before the
// client sends — while at A=on the proxy emits it after receiving. The chain is
// therefore built per cell from where the seal actually belongs, which is the
// same conditional ownership the schema records (ADR-0001).
func Chain(cell identity.Cell) ([]Span, error) {
	switch cell {
	case identity.CellD0:
		// The direct path has one transport hop in each direction, so its transfer
		// terms are not hop-indexed.
		return []Span{
			{StageBarrierWait, events.StageSched, events.StageCohortSeal, false},
			{StageReleaseToSend, events.StageCohortSeal, events.StageClientSend, false},
			{StageXReq, events.StageClientSend, events.StageQueueStart, true},
			{StageQBackend, events.StageQueueStart, events.StageComputeStart, true},
			{StageSComp, events.StageComputeStart, events.StageComputeEnd, true},
			{StageXResp, events.StageComputeEnd, events.StageClientRecv, true},
		}, nil

	case identity.CellF00, identity.CellF01, identity.CellF00Seq:
		// A=off: the load generator seals at barrier release, so the seal sits
		// between t_sched and the client send, and the proxy emits none.
		return append([]Span{
			{StageBarrierWait, events.StageSched, events.StageCohortSeal, false},
			{StageReleaseToSend, events.StageCohortSeal, events.StageClientSend, false},
			{StageXReqHop1, events.StageClientSend, events.StageProxyRecv, true},
			{StageAPack, events.StageProxyRecv, events.StageProxySend, true},
		}, sharedPathTail()...), nil

	case identity.CellF10, identity.CellF11D, identity.CellF11P:
		// A=on: the proxy seals the envelope after receiving the cohort, so the
		// seal sits inside the proxy's own stages and W_form is measured there.
		return append([]Span{
			{StageReleaseToSend, events.StageSched, events.StageClientSend, false},
			{StageXReqHop1, events.StageClientSend, events.StageProxyRecv, true},
			{StageWForm, events.StageProxyRecv, events.StageCohortSeal, true},
			{StageAPack, events.StageCohortSeal, events.StageProxySend, true},
		}, sharedPathTail()...), nil

	default:
		return nil, fmt.Errorf("validate: no stage chain for cell %q", cell)
	}
}

// sharedPathTail is the traversal from the proxy's send onward, shared by every
// cell that goes through the adapter.
func sharedPathTail() []Span {
	return []Span{
		{StageXReqHop2, events.StageProxySend, events.StageAdapterRecv, true},
		{StageAdapterUnpack, events.StageAdapterRecv, events.StageAdapterDispatch, false},
		{StageXReqHop3, events.StageAdapterDispatch, events.StageQueueStart, true},
		{StageQBackend, events.StageQueueStart, events.StageComputeStart, true},
		{StageSComp, events.StageComputeStart, events.StageComputeEnd, true},
		{StageXRespHop1, events.StageComputeEnd, events.StageAdapterResult, true},
		{StageResponsePack, events.StageAdapterResult, events.StageAdapterSend, false},
		{StageXRespHop2, events.StageAdapterSend, events.StageProxyRespRecv, true},
		{StageFFanout, events.StageProxyRespRecv, events.StageProxyFanout, true},
		{StageXRespHop3, events.StageProxyFanout, events.StageClientRecv, true},
	}
}

// ConservedSpan is the interval the conservation identity is stated over:
// client send to client completion, i.e. t15 − t2 (M2-PLAN §4.3).
//
// Stages before the client send are outside it, and that is not an omission. At
// A=off, the barrier wait and the load generator's dispatch latency happen before
// the request enters the system at all — they are scheduling facts about the
// generator, reported separately and never summed into the cycle. At A=on, W_form
// appears at the proxy between t_proxy_recv and t_cohort_seal and is inside the
// span — which is exactly when formation becomes a real cycle stage rather than a
// property of the harness. There is no cell in which both exist.
func ConservedSpan() (start, end events.Stage) {
	return events.StageClientSend, events.StageClientRecv
}

// PreCycle reports whether a span sits before the conserved interval begins.
func PreCycle(spans []Span, i int) bool {
	start, _ := ConservedSpan()
	for j := 0; j <= i && j < len(spans); j++ {
		if spans[j].Start == start {
			return false
		}
	}
	return true
}

// ChainStages lists the chain's timestamps in traversal order.
func ChainStages(spans []Span) []events.Stage {
	if len(spans) == 0 {
		return nil
	}
	out := make([]events.Stage, 0, len(spans)+1)
	out = append(out, spans[0].Start)
	for _, s := range spans {
		out = append(out, s.End)
	}
	return out
}

// AdditiveConservation reports whether a cell's per-request stage durations may
// be summed at cohort level. Single-submission cells admit the additive
// identity; multi-RPC cells do not, because the members' stages legitimately
// overlap and summing them would double-count (M1 §4, M2-PLAN §4.3).
func AdditiveConservation(cell identity.Cell) bool {
	switch cell {
	case identity.CellF10, identity.CellF11P, identity.CellF11D:
		return true
	default:
		return false
	}
}
