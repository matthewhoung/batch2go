package events

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matthewhoung/batch2go/internal/identity"
)

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

// blockingSink stalls every write until it is released, so a test can prove what
// the record path does when the flusher cannot keep up.
type blockingSink struct {
	release chan struct{}
	mu      sync.Mutex
	n       int
}

func newBlockingSink() *blockingSink {
	return &blockingSink{release: make(chan struct{})}
}

func (s *blockingSink) Write(p []byte) (int, error) {
	<-s.release
	s.mu.Lock()
	s.n += len(p)
	s.mu.Unlock()
	return len(p), nil
}

func (s *blockingSink) Close() error { return nil }

// The record path must allocate nothing. A=off cells emit B times more records
// than A=on, so a per-record allocation would be a treatment-correlated cost
// sitting directly inside the effect being measured (ADR-0004).
func TestRecordAllocatesNothing(t *testing.T) {
	w, err := NewWriter(nopCloser{io.Discard}, testHeader())
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer w.Close()

	rec := fullRecord()
	if allocs := testing.AllocsPerRun(200, func() {
		w.Record(&rec)
	}); allocs != 0 {
		t.Errorf("Record allocated %.1f times per call, want 0", allocs)
	}
}

// Recording must not wait for I/O. With the sink stalled, calls still return
// promptly; the cost of a stalled flusher is dropped records, which are counted
// and reported rather than absorbed by a blocked hot path.
func TestRecordNeverBlocksOnFlush(t *testing.T) {
	sink := newBlockingSink()
	w, err := NewWriter(sink, testHeader(), WithBankBytes(maxBodySize*8), WithBanks(2))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	rec := fullRecord()
	deadline := time.Now().Add(2 * time.Second)
	for w.Stats().Dropped == 0 {
		start := time.Now()
		w.Record(&rec)
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			close(sink.release)
			w.Close()
			t.Fatalf("Record blocked for %v with the sink stalled", elapsed)
		}
		if time.Now().After(deadline) {
			close(sink.release)
			w.Close()
			t.Fatal("never exhausted the banks; the test cannot observe the drop path")
		}
	}

	if got := w.Stats().Written + w.Stats().Dropped; got == 0 {
		t.Error("no records accounted for")
	}
	close(sink.release)
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Records with partial timestamp sets must survive the binary stream unchanged:
// what a process did not observe must still read back as absent.
func TestStreamRoundTripPreservesPartialTimestampSets(t *testing.T) {
	var buf bytes.Buffer
	h := testHeader()
	w, err := NewWriter(nopCloser{&buf}, h)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	const n = 500
	want := make([]Record, 0, n)
	for i := 0; i < n; i++ {
		rec := Record{
			Emitter: identity.EmitterLoadGen,
			Cohort:  identity.CohortID(i / 4),
			Ordinal: identity.Ordinal(i % 4),
			Status:  StatusOK,
		}
		// Only the load generator's own stages, so most of the schema is absent.
		rec.SetStage(StageSched, int64(1_000_000+i))
		rec.SetStage(StageClientSend, int64(1_000_100+i))
		if i%4 == 0 {
			rec.SetStage(StageCohortSeal, int64(1_000_050+i))
		}
		rec.SetStage(StageClientRecv, int64(1_009_000+i))
		if _, ok := w.Record(&rec); !ok {
			t.Fatalf("record %d dropped", i)
		}
		want = append(want, rec)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := w.Stats(); got.Written != n || got.Dropped != 0 {
		t.Fatalf("stats = %+v, want %d written / 0 dropped", got, n)
	}

	got, err := ReadStream(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(got) != n {
		t.Fatalf("read %d records, want %d", len(got), n)
	}
	for i := range got {
		if got[i].Header != h {
			t.Fatalf("record %d header = %+v, want %+v", i, got[i].Header, h)
		}
		if got[i].Record.Seq != uint64(i+1) {
			t.Errorf("record %d seq = %d, want %d", i, got[i].Record.Seq, i+1)
		}
		want[i].Seq = got[i].Record.Seq
		assertRecordsEqual(t, got[i].Record, want[i])
	}
}

func TestFileWriterRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.b2g")
	h := testHeader()
	w, err := NewFileWriter(path, h)
	if err != nil {
		t.Fatalf("new file writer: %v", err)
	}
	rec := fullRecord()
	if _, ok := w.Record(&rec); !ok {
		t.Fatal("record dropped")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got, err := ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d records, want 1", len(got))
	}
	rec.Seq = got[0].Record.Seq
	assertRecordsEqual(t, got[0].Record, rec)

	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatalf("stream file is empty (err=%v)", err)
	}
}

func TestWriterRejectsHeaderWithoutClockDomain(t *testing.T) {
	h := testHeader()
	h.ClockDomain = ""
	if _, err := NewWriter(nopCloser{io.Discard}, h); err == nil {
		t.Error("a writer without a clock domain should be refused: its timestamps could not be subtracted")
	}
}

func TestConcurrentRecordingKeepsEverySequenceNumber(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(nopCloser{&buf}, testHeader())
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	const writers, each = 8, 200
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rec := Record{Emitter: identity.EmitterProxy, Cohort: identity.CohortID(g), Status: StatusOK}
			for i := 0; i < each; i++ {
				rec.Ordinal = identity.Ordinal(i)
				rec.SetStage(StageProxyRecv, int64(i))
				w.Record(&rec)
			}
		}(g)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got, err := ReadStream(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(got) != writers*each {
		t.Fatalf("read %d records, want %d", len(got), writers*each)
	}
	seen := make(map[uint64]bool, len(got))
	for _, d := range got {
		if seen[d.Record.Seq] {
			t.Fatalf("duplicate sequence number %d", d.Record.Seq)
		}
		seen[d.Record.Seq] = true
	}
}

func BenchmarkRecord(b *testing.B) {
	w, err := NewWriter(nopCloser{io.Discard}, testHeader())
	if err != nil {
		b.Fatalf("new writer: %v", err)
	}
	defer w.Close()

	rec := fullRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Record(&rec)
	}
}
