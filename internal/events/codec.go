package events

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// The hot-path codec writes batch2go.events.v1.EventRecord wire format by hand.
//
// The generated protobuf code cannot be used on the record path: `optional
// int64` becomes `*int64`, so every present timestamp would cost an allocation,
// and A=off cells emit ~B times more records than A=on — a treatment-correlated
// allocation cost is exactly the contamination ADR-0004 exists to keep out of
// the measurement. Encoding by hand into a preallocated buffer costs nothing.
//
// The price is that this file must stay wire-compatible with the .proto by
// inspection. codec_test.go pays it: every record round-trips through the
// generated implementation in both directions.

// Protobuf field numbers, mirroring api/events/v1/event.proto.
const (
	fieldSchemaVersion   = 1
	fieldExperimentID    = 2
	fieldSessionID       = 3
	fieldRunID           = 4
	fieldCell            = 5
	fieldClockDomainID   = 6
	fieldEmitter         = 7
	fieldWriterID        = 8
	fieldSeq             = 9
	fieldCohortID        = 10
	fieldOrdinal         = 11
	fieldEnvelopeID      = 12
	fieldExecutionID     = 13
	fieldTritonRequestID = 14
	fieldPresenceMask    = 15
	fieldStatus          = 16

	// The 15 timestamps occupy 20..34 in schema order, so a stage's field number
	// is fieldTimestampBase + stage - 1.
	fieldTimestampBase = 20

	fieldLogicalBytes    = 40
	fieldEnvelopeBytes   = 41
	fieldBatchSize       = 42
	fieldMembershipUIDs  = 43
	fieldMembershipCount = 44
)

const (
	wireVarint = 0
	wireLen    = 2
)

// maxBodySize bounds the encoding of everything but the run-scoped header:
// identity and counters, 15 timestamps at 12 bytes each, and a full membership
// set. It exists so buffers are sized once rather than grown on the hot path.
const maxBodySize = 1024

// RunHeader is the run-scoped identity every record in a stream shares. It is
// encoded once by the writer instead of per record.
type RunHeader struct {
	Experiment  identity.ExperimentID
	Session     identity.SessionID
	Run         identity.RunID
	Cell        identity.Cell
	ClockDomain identity.ClockDomainID
	WriterID    identity.WriterID
}

// Validate rejects a header that would produce records the validator cannot
// attribute to a run.
func (h RunHeader) Validate() error {
	switch {
	case h.Experiment == "":
		return fmt.Errorf("events: run header needs an experiment id")
	case h.Session == "":
		return fmt.Errorf("events: run header needs a session id")
	case h.Run == "":
		return fmt.Errorf("events: run header needs a run id")
	case h.Cell == "":
		return fmt.Errorf("events: run header needs a cell")
	case h.ClockDomain == "":
		return fmt.Errorf("events: run header needs a clock domain id; timestamps without one cannot be subtracted")
	}
	return nil
}

// encodeHeader renders the fields shared by every record in the stream.
func encodeHeader(h RunHeader) []byte {
	buf := make([]byte, 0, 128+len(h.Experiment)+len(h.Session)+len(h.Run)+len(h.ClockDomain))
	buf = appendVarintField(buf, fieldSchemaVersion, SchemaVersion)
	buf = appendStringField(buf, fieldExperimentID, string(h.Experiment))
	buf = appendStringField(buf, fieldSessionID, string(h.Session))
	buf = appendStringField(buf, fieldRunID, string(h.Run))
	buf = appendStringField(buf, fieldCell, string(h.Cell))
	buf = appendStringField(buf, fieldClockDomainID, string(h.ClockDomain))
	return buf
}

// appendRecord encodes r's per-request fields onto dst and returns the extended
// slice. It allocates nothing when dst has maxBodySize spare capacity.
func appendRecord(dst []byte, r *Record, writerID identity.WriterID) []byte {
	dst = appendVarintField(dst, fieldEmitter, uint64(r.Emitter))
	dst = appendVarintField(dst, fieldWriterID, uint64(writerID))
	dst = appendVarintField(dst, fieldSeq, r.Seq)
	dst = appendVarintField(dst, fieldCohortID, uint64(r.Cohort))
	dst = appendVarintField(dst, fieldOrdinal, uint64(r.Ordinal))
	dst = appendVarintField(dst, fieldEnvelopeID, uint64(r.EnvelopeID))
	dst = appendVarintField(dst, fieldExecutionID, uint64(r.ExecutionID))

	// The Triton request id is a pure function of the logical request's identity,
	// so it is derived here rather than stored on the record. Formatting into a
	// stack array keeps the derivation allocation-free.
	var idBuf [tritonRequestIDMax]byte
	dst = appendBytesField(dst, fieldTritonRequestID, AppendTritonRequestID(idBuf[:0], r.Request()))

	dst = appendVarintField(dst, fieldPresenceMask, uint64(r.Presence))
	dst = appendVarintField(dst, fieldStatus, uint64(r.Status))

	for s := StageSched; s <= StageClientRecv; s++ {
		if r.Presence.Has(s) {
			// Present timestamps are written unconditionally, including zero: for an
			// `optional` field the tag itself is the presence signal.
			dst = appendTag(dst, fieldTimestampBase+int(s)-1, wireVarint)
			dst = binary.AppendUvarint(dst, uint64(r.TS[s]))
		}
	}

	dst = appendVarintField(dst, fieldLogicalBytes, uint64(r.LogicalBytes))
	dst = appendVarintField(dst, fieldEnvelopeBytes, uint64(r.EnvelopeBytes))
	dst = appendVarintField(dst, fieldBatchSize, uint64(r.BatchSize))

	if n := int(r.MembershipStored); n > 0 {
		var payload int
		for _, uid := range r.Membership[:n] {
			payload += varintLen(uint64(uid))
		}
		dst = appendTag(dst, fieldMembershipUIDs, wireLen)
		dst = binary.AppendUvarint(dst, uint64(payload))
		for _, uid := range r.Membership[:n] {
			dst = binary.AppendUvarint(dst, uint64(uid))
		}
	}
	dst = appendVarintField(dst, fieldMembershipCount, uint64(r.MembershipCount))
	return dst
}

// tritonRequestIDMax bounds the rendered "c<cohort>/o<ordinal>" form.
const tritonRequestIDMax = 24

// AppendTritonRequestID renders the request id carried on the Triton wire and
// echoed back in Triton's trace output, which is what joins backend timestamps
// to a logical request. Timestamp proximity is never used as identity.
func AppendTritonRequestID(dst []byte, r identity.LogicalRequest) []byte {
	dst = append(dst, 'c')
	dst = strconv.AppendUint(dst, uint64(r.Cohort), 10)
	dst = append(dst, '/', 'o')
	dst = strconv.AppendUint(dst, uint64(r.Ordinal), 10)
	return dst
}

// TritonRequestID is the string form of the id, for call sites that are not on
// the hot path.
func TritonRequestID(r identity.LogicalRequest) string {
	var buf [tritonRequestIDMax]byte
	return string(AppendTritonRequestID(buf[:0], r))
}

// ParseTritonRequestID inverts TritonRequestID, so a trace entry resolves to the
// logical request that produced it.
func ParseTritonRequestID(s string) (identity.LogicalRequest, error) {
	var lr identity.LogicalRequest
	if len(s) < 4 || s[0] != 'c' {
		return lr, fmt.Errorf("events: malformed triton request id %q", s)
	}
	sep := -1
	for i := 1; i < len(s)-1; i++ {
		if s[i] == '/' && s[i+1] == 'o' {
			sep = i
			break
		}
	}
	if sep < 0 {
		return lr, fmt.Errorf("events: malformed triton request id %q", s)
	}
	cohort, err := strconv.ParseUint(s[1:sep], 10, 32)
	if err != nil {
		return lr, fmt.Errorf("events: malformed cohort in request id %q: %w", s, err)
	}
	ordinal, err := strconv.ParseUint(s[sep+2:], 10, 32)
	if err != nil {
		return lr, fmt.Errorf("events: malformed ordinal in request id %q: %w", s, err)
	}
	return identity.LogicalRequest{Cohort: identity.CohortID(cohort), Ordinal: identity.Ordinal(ordinal)}, nil
}

func appendTag(dst []byte, field, wire int) []byte {
	return binary.AppendUvarint(dst, uint64(field)<<3|uint64(wire))
}

func appendVarintField(dst []byte, field int, v uint64) []byte {
	if v == 0 {
		return dst // proto3 implicit presence: zero scalars are not on the wire
	}
	dst = appendTag(dst, field, wireVarint)
	return binary.AppendUvarint(dst, v)
}

func appendStringField(dst []byte, field int, s string) []byte {
	if s == "" {
		return dst
	}
	dst = appendTag(dst, field, wireLen)
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func appendBytesField(dst []byte, field int, b []byte) []byte {
	if len(b) == 0 {
		return dst
	}
	dst = appendTag(dst, field, wireLen)
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func varintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// Decoded is one record read back from an archived stream, carrying both the
// run-scoped header and the per-request record. Reading happens offline, where
// allocation costs nothing, so this is a plain struct.
type Decoded struct {
	SchemaVersion uint32
	Header        RunHeader
	Record        Record
}

// Decode parses one EventRecord message body. It is the inverse of the writer's
// encoding and is used by the validator and the Parquet converter, never on a
// hot path.
func Decode(b []byte) (Decoded, error) {
	var d Decoded
	var membershipCountSeen bool
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return d, fmt.Errorf("events: truncated field tag")
		}
		b = b[n:]
		field, wire := int(tag>>3), int(tag&7)

		switch wire {
		case wireVarint:
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return d, fmt.Errorf("events: truncated varint in field %d", field)
			}
			b = b[n:]
			if err := d.setVarint(field, v); err != nil {
				return d, err
			}
			if field == fieldMembershipCount {
				membershipCountSeen = true
			}
		case wireLen:
			size, n := binary.Uvarint(b)
			if n <= 0 || uint64(len(b[n:])) < size {
				return d, fmt.Errorf("events: truncated length-delimited field %d", field)
			}
			payload := b[n : n+int(size)]
			b = b[n+int(size):]
			if err := d.setBytes(field, payload); err != nil {
				return d, err
			}
		default:
			return d, fmt.Errorf("events: unsupported wire type %d in field %d", wire, field)
		}
	}
	// membership_count is a proto3 implicit-presence scalar, so a single-member
	// execution whose count is 1 is on the wire, but a truly absent count is
	// indistinguishable from zero. Recovering it from the stored uids keeps
	// "how many members did the execution claim" answerable from the record.
	if !membershipCountSeen && d.Record.MembershipStored > 0 {
		d.Record.MembershipCount = uint32(d.Record.MembershipStored)
	}
	return d, nil
}

func (d *Decoded) setVarint(field int, v uint64) error {
	if field >= fieldTimestampBase && field < fieldTimestampBase+StageCount {
		stage := Stage(field - fieldTimestampBase + 1)
		d.Record.TS[stage] = int64(v)
		// Presence comes from the recorded mask, not from which tags appeared, so
		// that a record claiming a stage it did not write is a detectable defect
		// rather than something the decoder quietly repairs.
		return nil
	}
	switch field {
	case fieldSchemaVersion:
		d.SchemaVersion = uint32(v)
	case fieldEmitter:
		d.Record.Emitter = identity.Emitter(v)
	case fieldWriterID:
		d.Header.WriterID = identity.WriterID(v)
	case fieldSeq:
		d.Record.Seq = v
	case fieldCohortID:
		d.Record.Cohort = identity.CohortID(v)
	case fieldOrdinal:
		d.Record.Ordinal = identity.Ordinal(v)
	case fieldEnvelopeID:
		d.Record.EnvelopeID = identity.EnvelopeID(v)
	case fieldExecutionID:
		d.Record.ExecutionID = identity.ExecutionID(v)
	case fieldPresenceMask:
		d.Record.Presence = StageMask(v)
	case fieldStatus:
		d.Record.Status = Status(v)
	case fieldLogicalBytes:
		d.Record.LogicalBytes = uint32(v)
	case fieldEnvelopeBytes:
		d.Record.EnvelopeBytes = uint32(v)
	case fieldBatchSize:
		d.Record.BatchSize = uint32(v)
	case fieldMembershipCount:
		d.Record.MembershipCount = uint32(v)
	}
	return nil
}

func (d *Decoded) setBytes(field int, payload []byte) error {
	switch field {
	case fieldExperimentID:
		d.Header.Experiment = identity.ExperimentID(payload)
	case fieldSessionID:
		d.Header.Session = identity.SessionID(payload)
	case fieldRunID:
		d.Header.Run = identity.RunID(payload)
	case fieldCell:
		d.Header.Cell = identity.Cell(payload)
	case fieldClockDomainID:
		d.Header.ClockDomain = identity.ClockDomainID(payload)
	case fieldTritonRequestID:
		// Derived from identity on encode; nothing to restore.
	case fieldMembershipUIDs:
		var uids []identity.UID
		for len(payload) > 0 {
			v, n := binary.Uvarint(payload)
			if n <= 0 {
				return fmt.Errorf("events: truncated membership uid")
			}
			payload = payload[n:]
			uids = append(uids, identity.UID(v))
		}
		d.Record.SetMembership(uids)
	}
	return nil
}
