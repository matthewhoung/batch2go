// Package events owns the scientific event record: the 15-timestamp schema, the
// stage-presence vocabulary, the clock-domain rules, and the append-only writers.
// Nothing else in the tree defines event shapes.
//
// Two properties of this package are load-bearing and easy to lose:
//
//   - The hot path allocates nothing. Run-scoped identity lives on the writer,
//     not on the record, so a Record is a flat value with no pointers.
//   - Absence is typed. A timestamp is either present, or absent because the
//     cell's topology has no such stage — never zero (ADR-0005).
package events

import (
	"fmt"
	"math/bits"
	"strings"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// SchemaVersion is the version of the record vocabulary below. Every record
// carries it so an archived bundle is readable without out-of-band context.
const SchemaVersion = 1

// Stage identifies one of the 15 timestamps (M2-PLAN §4.1). Values are the
// schema's canonical 1-based numbering and are part of the archived format.
type Stage uint8

const (
	StageSched           Stage = 1  // t_sched            LoadGen  scheduled release
	StageClientSend      Stage = 2  // t_client_send      LoadGen  client send
	StageProxyRecv       Stage = 3  // t_proxy_recv       Proxy    X_req hop 1 end
	StageCohortSeal      Stage = 4  // t_cohort_seal      LoadGen at A=off, Proxy at A=on
	StageProxySend       Stage = 5  // t_proxy_send       Proxy    A_pack end
	StageAdapterRecv     Stage = 6  // t_adapter_recv     Adapter  X_req hop 2 end
	StageAdapterDispatch Stage = 7  // t_adapter_dispatch Adapter  unpack end
	StageQueueStart      Stage = 8  // t_queue_start      Triton   Q_backend start
	StageComputeStart    Stage = 9  // t_compute_start    Triton   S_comp start
	StageComputeEnd      Stage = 10 // t_compute_end      Triton   S_comp end
	StageAdapterResult   Stage = 11 // t_adapter_result   Adapter  backend return
	StageAdapterSend     Stage = 12 // t_adapter_send     Adapter  response pack end
	StageProxyRespRecv   Stage = 13 // t_proxy_resp_recv  Proxy    X_resp end
	StageProxyFanout     Stage = 14 // t_proxy_fanout     Proxy    F_fanout end
	StageClientRecv      Stage = 15 // t_client_recv      LoadGen  client completion

	// StageCount is the number of timestamps in the schema. The name "15-timestamp
	// schema" is used throughout the design documents; changing this constant is a
	// schema revision, not an implementation detail.
	StageCount = 15
)

// stageNames are the column names the archive and the analysis toolchain share.
// They are the same strings the design documents use, so a term in a paper draft
// and a term in a Parquet header resolve to the same definition.
var stageNames = [StageCount + 1]string{
	StageSched:           "t_sched",
	StageClientSend:      "t_client_send",
	StageProxyRecv:       "t_proxy_recv",
	StageCohortSeal:      "t_cohort_seal",
	StageProxySend:       "t_proxy_send",
	StageAdapterRecv:     "t_adapter_recv",
	StageAdapterDispatch: "t_adapter_dispatch",
	StageQueueStart:      "t_queue_start",
	StageComputeStart:    "t_compute_start",
	StageComputeEnd:      "t_compute_end",
	StageAdapterResult:   "t_adapter_result",
	StageAdapterSend:     "t_adapter_send",
	StageProxyRespRecv:   "t_proxy_resp_recv",
	StageProxyFanout:     "t_proxy_fanout",
	StageClientRecv:      "t_client_recv",
}

func (s Stage) String() string {
	if s >= 1 && s <= StageCount {
		return stageNames[s]
	}
	return fmt.Sprintf("Stage(%d)", uint8(s))
}

// Valid reports whether s names a timestamp in the schema.
func (s Stage) Valid() bool { return s >= 1 && s <= StageCount }

// AllStages returns the 15 stages in schema order.
func AllStages() []Stage {
	out := make([]Stage, 0, StageCount)
	for s := StageSched; s <= StageClientRecv; s++ {
		out = append(out, s)
	}
	return out
}

// ParseStage resolves a stage from its schema name.
func ParseStage(name string) (Stage, error) {
	for s := StageSched; s <= StageClientRecv; s++ {
		if stageNames[s] == name {
			return s, nil
		}
	}
	return 0, fmt.Errorf("events: unknown stage %q", name)
}

// StageMask is a set of stages. It is how absence is typed: a stage outside the
// cell's topology mask is absent by design, while a stage inside it but outside
// the record's presence mask is a missing timestamp — a validation failure.
type StageMask uint32

// MaskOf builds a mask from stages.
func MaskOf(stages ...Stage) StageMask {
	var m StageMask
	for _, s := range stages {
		m = m.With(s)
	}
	return m
}

// With returns m plus s.
func (m StageMask) With(s Stage) StageMask { return m | StageMask(1)<<(s-1) }

// Without returns m minus s.
func (m StageMask) Without(s Stage) StageMask { return m &^ (StageMask(1) << (s - 1)) }

// Has reports whether s is in m.
func (m StageMask) Has(s Stage) bool { return m&(StageMask(1)<<(s-1)) != 0 }

// Len is the number of stages in m.
func (m StageMask) Len() int { return bits.OnesCount32(uint32(m)) }

// Stages lists m's members in schema order.
func (m StageMask) Stages() []Stage {
	out := make([]Stage, 0, m.Len())
	for s := StageSched; s <= StageClientRecv; s++ {
		if m.Has(s) {
			out = append(out, s)
		}
	}
	return out
}

func (m StageMask) String() string {
	names := make([]string, 0, m.Len())
	for _, s := range m.Stages() {
		names = append(names, s.String())
	}
	return "{" + strings.Join(names, ",") + "}"
}

// stage groupings by owning process, used to build topology masks.
var (
	loadGenStages = MaskOf(StageSched, StageClientSend, StageClientRecv)
	proxyStages   = MaskOf(StageProxyRecv, StageProxySend, StageProxyRespRecv, StageProxyFanout)
	adapterStages = MaskOf(StageAdapterRecv, StageAdapterDispatch, StageAdapterResult, StageAdapterSend)
	tritonStages  = MaskOf(StageQueueStart, StageComputeStart, StageComputeEnd)
)

// TopologyMask is the set of stages a cell's path actually traverses — the mask
// a fully joined record for that cell must match exactly.
//
// D0 has no proxy and no adapter, so those stages are absent by topology, not
// missing. Every cell carries t_cohort_seal: at A=off the load generator emits
// it at barrier release, at A=on the proxy emits it at envelope seal (ADR-0001).
func TopologyMask(cell identity.Cell) (StageMask, error) {
	switch cell {
	case identity.CellD0:
		return loadGenStages | tritonStages | MaskOf(StageCohortSeal), nil
	case identity.CellF00, identity.CellF01, identity.CellF10,
		identity.CellF11D, identity.CellF11P, identity.CellF00Seq:
		return loadGenStages | proxyStages | adapterStages | tritonStages | MaskOf(StageCohortSeal), nil
	default:
		return 0, fmt.Errorf("events: no topology mask for cell %q", cell)
	}
}

// SealOwner names the process that emits t_cohort_seal for a cell. Ownership is
// conditional on Factor A and is recorded, not assumed (ADR-0001).
func SealOwner(cell identity.Cell) identity.Emitter {
	if cell.AggregatesEnvelopes() {
		return identity.EmitterProxy
	}
	return identity.EmitterLoadGen
}

// OwnedStages is the set of stages an emitter may write for a cell. A record
// carrying a stage its emitter does not own is a schema violation: it would mean
// a timestamp was taken by a process that cannot legitimately observe it.
func OwnedStages(cell identity.Cell, emitter identity.Emitter) StageMask {
	var m StageMask
	switch emitter {
	case identity.EmitterLoadGen:
		m = loadGenStages
	case identity.EmitterProxy:
		m = proxyStages
	case identity.EmitterAdapter:
		m = adapterStages
	case identity.EmitterTriton:
		m = tritonStages
	default:
		return 0
	}
	if SealOwner(cell) == emitter {
		m = m.With(StageCohortSeal)
	}
	topology, err := TopologyMask(cell)
	if err != nil {
		return 0
	}
	return m & topology
}

// Status is the terminal outcome of a logical request.
type Status uint8

const (
	StatusUnspecified Status = 0
	StatusOK          Status = 1
	StatusError       Status = 2
	StatusTimeout     Status = 3
)

var statusNames = map[Status]string{
	StatusUnspecified: "unspecified",
	StatusOK:          "ok",
	StatusError:       "error",
	StatusTimeout:     "timeout",
}

func (s Status) String() string {
	if n, ok := statusNames[s]; ok {
		return n
	}
	return fmt.Sprintf("Status(%d)", uint8(s))
}

// MaxMembership caps how many uids one record stores. Cohort sizes in the design
// are 4 and 16; the headroom exists so that cross-cohort coalescing — the thing
// membership evidence is meant to detect — is observable rather than silently
// truncated. A record whose MembershipCount exceeds what it stored is reported
// by the validator as truncated evidence.
const MaxMembership = 64

// Record is one process's contribution to one logical request's path.
//
// It is a flat value with no pointers: run-scoped identity (experiment, session,
// run, cell, clock domain) lives on the Writer and is stamped in at encode time,
// so recording a record costs no allocation.
type Record struct {
	Emitter     identity.Emitter
	Seq         uint64
	Cohort      identity.CohortID
	Ordinal     identity.Ordinal
	EnvelopeID  identity.EnvelopeID
	ExecutionID identity.ExecutionID

	// Presence must agree with which entries of TS are set; SetStage maintains
	// both together so the two cannot drift.
	Presence StageMask
	Status   Status

	// TS is indexed by Stage, so TS[0] is unused and TS[StageSched] is timestamp 1.
	TS [StageCount + 1]int64

	LogicalBytes  uint32
	EnvelopeBytes uint32
	BatchSize     uint32

	// Membership holds the self-attested uid set of this request's execution
	// (ADR-0007), and MembershipCount the size the execution claimed. They differ
	// only when the evidence exceeded MaxMembership.
	Membership       [MaxMembership]identity.UID
	MembershipStored uint8
	MembershipCount  uint32
}

// SetStage records a timestamp and marks it present in one operation.
func (r *Record) SetStage(s Stage, ts int64) {
	r.TS[s] = ts
	r.Presence = r.Presence.With(s)
}

// Stage returns the timestamp and whether it is present. A stage that is absent
// reads as (0, false) — callers can never mistake absence for a zero instant.
func (r *Record) Stage(s Stage) (int64, bool) {
	if !s.Valid() || !r.Presence.Has(s) {
		return 0, false
	}
	return r.TS[s], true
}

// SetMembership records the uid set an execution attested to. count is the size
// the execution claimed, which may exceed len(uids) only if evidence was
// truncated upstream.
func (r *Record) SetMembership(uids []identity.UID) {
	r.MembershipCount = uint32(len(uids))
	n := len(uids)
	if n > MaxMembership {
		n = MaxMembership
	}
	copy(r.Membership[:n], uids[:n])
	r.MembershipStored = uint8(n)
}

// MembershipUIDs returns the stored uid set. The slice aliases the record, so it
// is read-only for the caller's purposes and costs no allocation.
func (r *Record) MembershipUIDs() []identity.UID {
	return r.Membership[:r.MembershipStored]
}

// Request is the logical request this record describes.
func (r *Record) Request() identity.LogicalRequest {
	return identity.LogicalRequest{Cohort: r.Cohort, Ordinal: r.Ordinal}
}

// Reset returns the record to its zero value so a pooled record cannot leak the
// previous request's stages into the next one.
func (r *Record) Reset() { *r = Record{} }
