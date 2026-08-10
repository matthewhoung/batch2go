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
