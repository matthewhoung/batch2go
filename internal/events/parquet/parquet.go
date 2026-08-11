// Package parquet converts the binary record stream written on the hot path
// into the Parquet + zstd archive that the analysis toolchain reads (ADR-0005).
//
// This runs at run finalization, never on a hot path, so it may allocate freely.
// Column names are the schema's own vocabulary, so a term in a paper draft, a
// term in an event record, and a column header all resolve to one definition.
package parquet

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// Row is one event record as an archive row.
//
// The 15 timestamps are pointers so that Parquet stores a real null for a stage
// the process did not observe. Absence must survive into the archive: an analysis
// that read a missing timestamp as the instant zero would silently compute a
// stage duration out of nothing (ADR-0005).
type Row struct {
	SchemaVersion uint32 `parquet:"schema_version"`
	ExperimentID  string `parquet:"experiment_id,dict"`
	SessionID     string `parquet:"session_id,dict"`
	RunID         string `parquet:"run_id,dict"`
	Cell          string `parquet:"cell,dict"`
	ClockDomainID string `parquet:"clock_domain_id,dict"`
	Emitter       string `parquet:"emitter,dict"`
	WriterID      uint32 `parquet:"writer_id"`
	Seq           uint64 `parquet:"seq"`

	CohortID        uint32 `parquet:"cohort_id"`
	Ordinal         uint32 `parquet:"ordinal"`
	EnvelopeID      uint64 `parquet:"envelope_id"`
	ExecutionID     uint64 `parquet:"execution_id"`
	TritonRequestID string `parquet:"triton_request_id"`
	PresenceMask    uint32 `parquet:"presence_mask"`
	Status          string `parquet:"status,dict"`

	TSched           *int64 `parquet:"t_sched,optional"`
	TClientSend      *int64 `parquet:"t_client_send,optional"`
	TProxyRecv       *int64 `parquet:"t_proxy_recv,optional"`
	TCohortSeal      *int64 `parquet:"t_cohort_seal,optional"`
	TProxySend       *int64 `parquet:"t_proxy_send,optional"`
	TAdapterRecv     *int64 `parquet:"t_adapter_recv,optional"`
	TAdapterDispatch *int64 `parquet:"t_adapter_dispatch,optional"`
	TQueueStart      *int64 `parquet:"t_queue_start,optional"`
	TComputeStart    *int64 `parquet:"t_compute_start,optional"`
	TComputeEnd      *int64 `parquet:"t_compute_end,optional"`
	TAdapterResult   *int64 `parquet:"t_adapter_result,optional"`
	TAdapterSend     *int64 `parquet:"t_adapter_send,optional"`
	TProxyRespRecv   *int64 `parquet:"t_proxy_resp_recv,optional"`
	TProxyFanout     *int64 `parquet:"t_proxy_fanout,optional"`
	TClientRecv      *int64 `parquet:"t_client_recv,optional"`

	LogicalBytes    uint32  `parquet:"logical_bytes"`
	EnvelopeBytes   uint32  `parquet:"envelope_bytes"`
	BatchSize       uint32  `parquet:"batch_size"`
	MembershipUIDs  []int64 `parquet:"membership_uids,list"`
	MembershipCount uint32  `parquet:"membership_count"`

	// The adapter's fan-out evidence, nullable for the same reason the timestamps
	// are: a process that observed no dispatch archives nulls, while a dispatch of
	// one member archives a skew of zero. An analysis that read those as the same
	// number would conclude the fan-out was tight in runs where it was never
	// measured at all.
	Dispatched        *uint32 `parquet:"dispatched,optional"`
	DispatchSkewNanos *int64  `parquet:"dispatch_skew_nanos,optional"`
	AdapterCPUNanos   *int64  `parquet:"adapter_cpu_nanos,optional"`
	AdapterCPUScope   *string `parquet:"adapter_cpu_scope,optional"`
}

// stageField maps a stage to its slot in a Row, so the converter states the
// schema's stage-to-column correspondence exactly once.
func stageField(r *Row, s events.Stage) **int64 {
	switch s {
	case events.StageSched:
		return &r.TSched
	case events.StageClientSend:
		return &r.TClientSend
	case events.StageProxyRecv:
		return &r.TProxyRecv
	case events.StageCohortSeal:
		return &r.TCohortSeal
	case events.StageProxySend:
		return &r.TProxySend
	case events.StageAdapterRecv:
		return &r.TAdapterRecv
	case events.StageAdapterDispatch:
		return &r.TAdapterDispatch
	case events.StageQueueStart:
		return &r.TQueueStart
	case events.StageComputeStart:
		return &r.TComputeStart
	case events.StageComputeEnd:
		return &r.TComputeEnd
	case events.StageAdapterResult:
		return &r.TAdapterResult
	case events.StageAdapterSend:
		return &r.TAdapterSend
	case events.StageProxyRespRecv:
		return &r.TProxyRespRecv
	case events.StageProxyFanout:
		return &r.TProxyFanout
	case events.StageClientRecv:
		return &r.TClientRecv
	}
	return nil
}

// ToRow renders a decoded record as an archive row, preserving typed absence.
func ToRow(d events.Decoded) Row {
	row := Row{
		SchemaVersion:   d.SchemaVersion,
		ExperimentID:    string(d.Header.Experiment),
		SessionID:       string(d.Header.Session),
		RunID:           string(d.Header.Run),
		Cell:            string(d.Header.Cell),
		ClockDomainID:   string(d.Header.ClockDomain),
		Emitter:         d.Record.Emitter.String(),
		WriterID:        uint32(d.Header.WriterID),
		Seq:             d.Record.Seq,
		CohortID:        uint32(d.Record.Cohort),
		Ordinal:         uint32(d.Record.Ordinal),
		EnvelopeID:      uint64(d.Record.EnvelopeID),
		ExecutionID:     uint64(d.Record.ExecutionID),
		TritonRequestID: events.TritonRequestID(d.Record.Request()),
		PresenceMask:    uint32(d.Record.Presence),
		Status:          d.Record.Status.String(),
		LogicalBytes:    d.Record.LogicalBytes,
		EnvelopeBytes:   d.Record.EnvelopeBytes,
		BatchSize:       d.Record.BatchSize,
		MembershipCount: d.Record.MembershipCount,
		MembershipUIDs:  []int64{},
	}
	for _, s := range events.AllStages() {
		if ts, ok := d.Record.Stage(s); ok {
			v := ts
			*stageField(&row, s) = &v
		}
	}
	for _, uid := range d.Record.MembershipUIDs() {
		row.MembershipUIDs = append(row.MembershipUIDs, int64(uid))
	}
	if d.Record.HasDispatch {
		e := d.Record.Dispatch
		scope := e.CPUScope.String()
		row.Dispatched = &e.Dispatched
		row.DispatchSkewNanos = &e.SkewNanos
		row.AdapterCPUNanos = &e.CPUNanos
		row.AdapterCPUScope = &scope
	}
	return row
}

// FromRow reconstructs a decoded record from an archive row, so a verdict can be
// reproduced from the archive alone.
func FromRow(row Row) (events.Decoded, error) {
	emitter, err := identity.ParseEmitter(row.Emitter)
	if err != nil {
		return events.Decoded{}, err
	}
	d := events.Decoded{
		SchemaVersion: row.SchemaVersion,
		Header: events.RunHeader{
			Experiment:  identity.ExperimentID(row.ExperimentID),
			Session:     identity.SessionID(row.SessionID),
			Run:         identity.RunID(row.RunID),
			Cell:        identity.Cell(row.Cell),
			ClockDomain: identity.ClockDomainID(row.ClockDomainID),
			WriterID:    identity.WriterID(row.WriterID),
		},
		Record: events.Record{
			Emitter:       emitter,
			Seq:           row.Seq,
			Cohort:        identity.CohortID(row.CohortID),
			Ordinal:       identity.Ordinal(row.Ordinal),
			EnvelopeID:    identity.EnvelopeID(row.EnvelopeID),
			ExecutionID:   identity.ExecutionID(row.ExecutionID),
			Presence:      events.StageMask(row.PresenceMask),
			LogicalBytes:  row.LogicalBytes,
			EnvelopeBytes: row.EnvelopeBytes,
			BatchSize:     row.BatchSize,
		},
	}
	switch row.Status {
	case "ok":
		d.Record.Status = events.StatusOK
	case "error":
		d.Record.Status = events.StatusError
	case "timeout":
		d.Record.Status = events.StatusTimeout
	default:
		d.Record.Status = events.StatusUnspecified
	}
	for _, s := range events.AllStages() {
		if p := *stageField(&row, s); p != nil {
			d.Record.TS[s] = *p
		}
	}
	uids := make([]identity.UID, 0, len(row.MembershipUIDs))
	for _, uid := range row.MembershipUIDs {
		uids = append(uids, identity.UID(uid))
	}
	d.Record.SetMembership(uids)
	d.Record.MembershipCount = row.MembershipCount

	// The evidence describes one fan-out, so the row either carries all of it or
	// none. A half-present row is refused rather than completed with zeros: a CPU
	// number with no scope beside it has no definition, and a skew with no
	// dispatch size is a claim about a fan-out nobody counted. The analysis
	// reader refuses the same shapes, independently.
	present := 0
	for _, carried := range []bool{
		row.Dispatched != nil,
		row.DispatchSkewNanos != nil,
		row.AdapterCPUNanos != nil,
		row.AdapterCPUScope != nil,
	} {
		if carried {
			present++
		}
	}
	switch present {
	case 0:
	case 4:
		scope, err := events.ParseCPUScope(*row.AdapterCPUScope)
		if err != nil {
			return events.Decoded{}, err
		}
		d.Record.SetDispatch(events.DispatchEvidence{
			Dispatched: *row.Dispatched,
			SkewNanos:  *row.DispatchSkewNanos,
			CPUNanos:   *row.AdapterCPUNanos,
			CPUScope:   scope,
		})
	default:
		return events.Decoded{}, fmt.Errorf(
			"parquet: %v carries %d of the 4 dispatch-evidence columns; it describes one fan-out and travels whole",
			d.Record.Request(), present)
	}
	return d, nil
}

// Write archives records as Parquet with zstd compression.
func Write(path string, records []events.Decoded) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("parquet: create archive %s: %w", path, err)
	}
	defer f.Close()

	w := parquet.NewGenericWriter[Row](f, parquet.Compression(&zstd.Codec{}))
	rows := make([]Row, 0, len(records))
	for _, d := range records {
		rows = append(rows, ToRow(d))
	}
	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			return fmt.Errorf("parquet: write rows: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("parquet: close archive: %w", err)
	}
	return nil
}

// Read loads an archive back into decoded records.
func Read(path string) ([]events.Decoded, error) {
	rows, err := parquet.ReadFile[Row](path)
	if err != nil {
		return nil, fmt.Errorf("parquet: read archive %s: %w", path, err)
	}
	out := make([]events.Decoded, 0, len(rows))
	for _, row := range rows {
		d, err := FromRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// ConvertStream converts a binary record stream into a Parquet archive and
// reports how many records it carried.
func ConvertStream(streamPath, archivePath string) (int, error) {
	records, err := events.ReadFile(streamPath)
	if err != nil {
		return 0, err
	}
	if err := Write(archivePath, records); err != nil {
		return 0, err
	}
	return len(records), nil
}
