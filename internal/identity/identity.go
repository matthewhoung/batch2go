// Package identity holds the typed identifiers that every other package speaks.
//
// These are accounting labels, not runtime objects. In particular a cohort at
// A=OFF is exactly the pair (CohortID, Ordinal) minted by the load generator and
// carried end to end; nothing joins those requests at runtime (ADR-0001). The
// package imports nothing above the standard library.
package identity

import (
	"fmt"
	"strings"
)

// Schema-bearing string identifiers. They are opaque to everything but the
// process that mints them; the validator only ever compares them for equality.
type (
	ExperimentID  string
	SessionID     string
	RunID         string
	ClockDomainID string
)

// Cell names one experimental condition by its factor levels.
type Cell string

const (
	CellD0     Cell = "D0"      // diagnostic: direct path, no proxy
	CellF00    Cell = "F00"     // A=off, V=off — the factorial baseline
	CellF01    Cell = "F01"     // A=off, V=on
	CellF10    Cell = "F10"     // A=on,  V=off
	CellF11D   Cell = "F11-D"   // A=on,  V=on, scheduler-formed
	CellF11P   Cell = "F11-P"   // formation-location implementation contrast
	CellF00Seq Cell = "F00-seq" // diagnostic: serial release
)

// implementedCells are the cells this build can run end to end.
//
// It is the single authority. The manifest validator, the runner and the
// adapter all consult it rather than each carrying a list, because three lists
// can disagree — and a disagreement shows up as a run that parsed, materialized
// a model repository, started a data plane, and only then discovered that the
// cell it was asked for does not exist here.
var implementedCells = map[Cell]bool{CellD0: true, CellF00: true, CellF10: true}

// AllCells lists every cell the design defines, in contract-table order.
func AllCells() []Cell {
	return []Cell{CellD0, CellF00, CellF01, CellF10, CellF11D, CellF11P, CellF00Seq}
}

// ParseCell resolves a manifest's cell string, rejecting anything not in the
// design. Cell names are never invented at the call site.
func ParseCell(s string) (Cell, error) {
	for _, c := range AllCells() {
		if string(c) == s {
			return c, nil
		}
	}
	return "", fmt.Errorf("identity: unknown cell %q (known: %s)", s, joinCells(AllCells()))
}

// Implemented reports whether this slice of the platform can run the cell.
// Cells beyond it parse but do not run: a manifest naming one fails visibly at
// validation rather than silently falling back (ARCHITECTURE §3.7).
func (c Cell) Implemented() bool { return implementedCells[c] }

// ImplementedCells lists the cells this build can run, in contract-table order.
func ImplementedCells() []Cell {
	out := make([]Cell, 0, len(implementedCells))
	for _, c := range AllCells() {
		if implementedCells[c] {
			out = append(out, c)
		}
	}
	return out
}

// CheckImplemented refuses a cell this build cannot run, naming it and what is
// available instead.
//
// The refusal is worded once, here, so that every gate between a manifest and a
// running process gives the same answer for the same reason. A cell that is not
// in the design at all is reported as that, which is a different mistake from
// naming a cell the design defines but this slice has not built.
func (c Cell) CheckImplemented() error {
	if c.Implemented() {
		return nil
	}
	if _, err := ParseCell(string(c)); err != nil {
		return err
	}
	return fmt.Errorf("identity: cell %s is in the design but not implemented by this build, which runs %s",
		c, joinCells(ImplementedCells()))
}

// PreformsBatch reports whether the cell's [B,…] tensor is built before the
// backend sees it, rather than by the backend's own scheduler.
//
// It is the formation-location contrast F11-P exists to draw, and it is a third
// property of a cell rather than a third factor: F11-P sits outside the
// factorial for exactly that reason. Executor selection reads it here rather
// than testing a cell name at the call site.
func (c Cell) PreformsBatch() bool { return c == CellF11P }

// UsesProxy reports whether the cell traverses the shared path. D0 is the only
// direct-path condition; every factorial cell goes through proxy and adapter.
func (c Cell) UsesProxy() bool { return c != CellD0 }

// AggregatesEnvelopes reports Factor A's level: whether a cohort's B logical
// requests share one transport envelope.
func (c Cell) AggregatesEnvelopes() bool {
	switch c {
	case CellF10, CellF11D, CellF11P:
		return true
	default:
		return false
	}
}

// VectorizesCompute reports Factor V's declared level. It is a declared policy;
// realized scheduler compliance is measured, never assumed.
func (c Cell) VectorizesCompute() bool {
	switch c {
	case CellF01, CellF11D, CellF11P:
		return true
	default:
		return false
	}
}

func joinCells(cs []Cell) string {
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = string(c)
	}
	return strings.Join(parts, ", ")
}

// CohortID labels the set of B logical requests released together by the load
// generator's release barrier.
type CohortID uint32

// Ordinal is a logical request's position within its cohort, in [0, B).
type Ordinal uint32

// EnvelopeID labels one transport message between proxy and adapter. At A=off
// there is one envelope per logical request; at A=on one per cohort.
type EnvelopeID uint64

// ExecutionID labels one model execution as reported by the backend.
type ExecutionID uint64

// WriterID distinguishes the append-only event streams of one run, so that
// sequence numbers are unique per writer rather than globally coordinated.
type WriterID uint32

// LogicalRequest is the unit of work as the client sees it: the identity minted
// by the load generator and carried, unchanged, to the response.
type LogicalRequest struct {
	Cohort  CohortID `json:"cohort_id"`
	Ordinal Ordinal  `json:"ordinal"`
}

func (r LogicalRequest) String() string {
	return fmt.Sprintf("c%d/o%d", r.Cohort, r.Ordinal)
}

// UID is the value carried in the model's uid tensor and returned, tiled, to
// every member of an execution — the physical membership evidence of ADR-0007.
//
// The encoding is invertible so that a uid observed in a response resolves back
// to the logical request that produced it without consulting any side table.
type UID int64

const uidOrdinalBits = 20

// MaxOrdinal is the largest ordinal a UID can encode, and therefore a hard
// ceiling on cohort size.
const MaxOrdinal = (1 << uidOrdinalBits) - 1

// UID encodes the logical request as the scalar the model consumes.
func (r LogicalRequest) UID() UID {
	return UID(uint64(r.Cohort)<<uidOrdinalBits | uint64(r.Ordinal))
}

// LogicalRequest inverts UID back to the identity that minted it.
func (u UID) LogicalRequest() LogicalRequest {
	return LogicalRequest{
		Cohort:  CohortID(uint64(u) >> uidOrdinalBits),
		Ordinal: Ordinal(uint64(u) & MaxOrdinal),
	}
}

// Emitter names the process that owns a timestamp. Ownership is part of the
// evidence: t_cohort_seal is emitted by the load generator at A=off and by the
// proxy at A=on, and the record says which (ADR-0001).
type Emitter uint8

const (
	EmitterUnknown Emitter = iota
	EmitterLoadGen
	EmitterProxy
	EmitterAdapter
	EmitterTriton
)

var emitterNames = map[Emitter]string{
	EmitterUnknown: "unknown",
	EmitterLoadGen: "loadgen",
	EmitterProxy:   "proxy",
	EmitterAdapter: "adapter",
	EmitterTriton:  "triton",
}

func (e Emitter) String() string {
	if n, ok := emitterNames[e]; ok {
		return n
	}
	return fmt.Sprintf("Emitter(%d)", uint8(e))
}

// ParseEmitter resolves an emitter name read back from an archived bundle.
func ParseEmitter(s string) (Emitter, error) {
	for e, n := range emitterNames {
		if n == s && e != EmitterUnknown {
			return e, nil
		}
	}
	return EmitterUnknown, fmt.Errorf("identity: unknown emitter %q", s)
}
