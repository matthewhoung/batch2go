package parquet

import (
	"path/filepath"
	"testing"

	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

func sampleRecords() []events.Decoded {
	header := events.RunHeader{
		Experiment:  "exp-walking-skeleton",
		Session:     "sess-0001",
		Run:         "run-0001",
		Cell:        identity.CellD0,
		ClockDomain: "cd-abcdef0123456789abcd",
		WriterID:    3,
	}
	topology, err := events.TopologyMask(identity.CellD0)
	if err != nil {
		panic(err)
	}

	var out []events.Decoded
	for i := 0; i < 8; i++ {
		rec := events.Record{
			Emitter:      identity.EmitterLoadGen,
			Seq:          uint64(i + 1),
			Cohort:       identity.CohortID(i / 4),
			Ordinal:      identity.Ordinal(i % 4),
			Status:       events.StatusOK,
			LogicalBytes: 262144,
			BatchSize:    1,
		}
		// Only the stages D0's topology actually has; everything else stays absent.
		for _, s := range topology.Stages() {
			rec.SetStage(s, int64(1_000_000_000+int(s)*1_000+i))
		}
		rec.SetMembership([]identity.UID{rec.Request().UID()})
		out = append(out, events.Decoded{SchemaVersion: events.SchemaVersion, Header: header, Record: rec})
	}
	return out
}

// The archive is what analysis reads and what a verdict must be reproducible
// from, so nothing may be lost in conversion — including which stages were
// absent.
func TestArchiveRoundTripIsLossless(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.parquet")
	want := sampleRecords()

	if err := Write(path, want); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("read %d rows, want %d", len(got), len(want))
	}

	for i := range got {
		if got[i].Header != want[i].Header {
			t.Errorf("row %d header = %+v, want %+v", i, got[i].Header, want[i].Header)
		}
		gr, wr := got[i].Record, want[i].Record
		if gr.Presence != wr.Presence {
			t.Errorf("row %d presence = %v, want %v", i, gr.Presence, wr.Presence)
		}
		if gr.Request() != wr.Request() || gr.Seq != wr.Seq || gr.Status != wr.Status {
			t.Errorf("row %d identity = %v/%d/%v, want %v/%d/%v",
				i, gr.Request(), gr.Seq, gr.Status, wr.Request(), wr.Seq, wr.Status)
		}
		for _, s := range events.AllStages() {
			gts, gok := gr.Stage(s)
			wts, wok := wr.Stage(s)
			if gok != wok {
				t.Errorf("row %d %s presence = %v, want %v", i, s, gok, wok)
			} else if gok && gts != wts {
				t.Errorf("row %d %s = %d, want %d", i, s, gts, wts)
			}
		}
		if len(gr.MembershipUIDs()) != len(wr.MembershipUIDs()) {
			t.Errorf("row %d membership size = %d, want %d",
				i, len(gr.MembershipUIDs()), len(wr.MembershipUIDs()))
		}
	}
}

// dispatchRecords pair a load-generator record, which observed no dispatch at
// all, with an adapter record whose one-member dispatch measured a skew of
// exactly zero. The archive has to keep those two apart.
func dispatchRecords() (absent, measuredZero events.Decoded) {
	header := events.RunHeader{
		Experiment:  "exp-walking-skeleton",
		Session:     "sess-0001",
		Run:         "run-0001",
		Cell:        identity.CellF00,
		ClockDomain: "cd-abcdef0123456789abcd",
		WriterID:    3,
	}

	fromLoadGen := events.Record{Emitter: identity.EmitterLoadGen, Cohort: 1, Status: events.StatusOK}
	fromLoadGen.SetStage(events.StageSched, 500)

	fromAdapter := events.Record{Emitter: identity.EmitterAdapter, Cohort: 1, Status: events.StatusOK}
	fromAdapter.SetStage(events.StageAdapterRecv, 700)
	fromAdapter.SetDispatch(events.DispatchEvidence{
		Dispatched: 1,
		SkewNanos:  0,
		CPUNanos:   0,
		CPUScope:   events.CPUScopeProcess,
	})

	return events.Decoded{SchemaVersion: events.SchemaVersion, Header: header, Record: fromLoadGen},
		events.Decoded{SchemaVersion: events.SchemaVersion, Header: header, Record: fromAdapter}
}

// A measured zero and a never-measured value must not archive as the same
// number. An analysis that read them alike would report a tight fan-out for runs
// where the fan-out was never observed.
func TestMeasuredZeroDispatchArchivesApartFromAbsent(t *testing.T) {
	absent, measured := dispatchRecords()

	absentRow, measuredRow := ToRow(absent), ToRow(measured)
	if absentRow.DispatchSkewNanos != nil {
		t.Errorf("a record that observed no dispatch archived skew %d", *absentRow.DispatchSkewNanos)
	}
	if absentRow.AdapterCPUScope != nil {
		t.Errorf("a record that observed no dispatch archived scope %q", *absentRow.AdapterCPUScope)
	}
	if measuredRow.DispatchSkewNanos == nil {
		t.Fatal("a one-member dispatch measured a skew of zero; the archive must carry it")
	}
	if *measuredRow.DispatchSkewNanos != 0 {
		t.Errorf("dispatch_skew_nanos = %d, want a measured 0", *measuredRow.DispatchSkewNanos)
	}
	if measuredRow.AdapterCPUScope == nil || *measuredRow.AdapterCPUScope != events.CPUScopeProcess.String() {
		t.Errorf("adapter_cpu_scope = %v, want %q; the cpu number is not interpretable without it",
			measuredRow.AdapterCPUScope, events.CPUScopeProcess)
	}

	path := filepath.Join(t.TempDir(), "events.parquet")
	if err := Write(path, []events.Decoded{absent, measured}); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("read %d rows, want 2", len(got))
	}
	if got[0].Record.HasDispatch {
		t.Errorf("the load generator's record came back carrying evidence %+v", got[0].Record.Dispatch)
	}
	if !got[1].Record.HasDispatch {
		t.Fatal("the adapter's measured zero came back as never-measured")
	}
	if got[1].Record.Dispatch != measured.Record.Dispatch {
		t.Errorf("evidence = %+v, want %+v", got[1].Record.Dispatch, measured.Record.Dispatch)
	}
}

// Half the evidence is not evidence: a CPU number with no scope beside it has no
// definition, and a skew with no dispatch size is a claim about a fan-out nobody
// counted. The archive reader refuses those rows rather than completing them
// with zeros, which is what the analysis reader does independently.
func TestPartialDispatchEvidenceIsRefused(t *testing.T) {
	_, measured := dispatchRecords()

	for name, blank := range map[string]func(*Row){
		"no dispatched": func(r *Row) { r.Dispatched = nil },
		"no skew":       func(r *Row) { r.DispatchSkewNanos = nil },
		"no cpu":        func(r *Row) { r.AdapterCPUNanos = nil },
		"no scope":      func(r *Row) { r.AdapterCPUScope = nil },
	} {
		row := ToRow(measured)
		blank(&row)

		if _, err := FromRow(row); err == nil {
			t.Errorf("%s: a partial row should be refused", name)
		}
	}

	// And a scope nobody can name is reported rather than read as "unspecified",
	// which is a different claim about the number beside it.
	row := ToRow(measured)
	unknown := "per-goroutine"
	row.AdapterCPUScope = &unknown
	if _, err := FromRow(row); err == nil {
		t.Error("an unknown cpu scope should be refused")
	}
}

// A stage outside the cell's topology must arrive in the archive as a null, not
// as the instant zero.
func TestAbsentStagesArchiveAsNull(t *testing.T) {
	rows := []Row{}
	for _, d := range sampleRecords() {
		rows = append(rows, ToRow(d))
	}
	for i, row := range rows {
		if row.TProxyRecv != nil {
			t.Errorf("row %d: D0 has no proxy stage, but t_proxy_recv archived as %d", i, *row.TProxyRecv)
		}
		if row.TAdapterDispatch != nil {
			t.Errorf("row %d: D0 has no adapter stage, but t_adapter_dispatch archived as %d", i, *row.TAdapterDispatch)
		}
		if row.TSched == nil {
			t.Errorf("row %d: t_sched is in D0's topology and must be archived", i)
		}
	}
}

func TestConvertStreamArchivesEveryRecord(t *testing.T) {
	dir := t.TempDir()
	streamPath := filepath.Join(dir, "events.b2g")
	archivePath := filepath.Join(dir, "events.parquet")

	records := sampleRecords()
	w, err := events.NewFileWriter(streamPath, records[0].Header)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	for i := range records {
		rec := records[i].Record
		if _, ok := w.Record(&rec); !ok {
			t.Fatalf("record %d dropped", i)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	n, err := ConvertStream(streamPath, archivePath)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if n != len(records) {
		t.Errorf("converted %d records, want %d", n, len(records))
	}

	got, err := Read(archivePath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if len(got) != len(records) {
		t.Fatalf("archive has %d rows, want %d", len(got), len(records))
	}
}
