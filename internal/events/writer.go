package events

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// DefaultBankBytes sizes one buffer bank. At roughly 200 bytes per record this
// holds ~20k records, which is far more than a flush interval's worth even at
// A=off, where a cohort emits B times more records than at A=on.
const DefaultBankBytes = 4 << 20

// A bank is one preallocated buffer. The writer fills one while the flusher
// drains the other, which is what keeps the record path off the I/O path.
type bank struct {
	buf []byte
	n   int
}

func (b *bank) reset() { b.n = 0 }

// Writer is a run-scoped, append-only event stream for one process.
//
// Two properties matter and are tested rather than asserted:
//
//   - Record allocates nothing. The run-scoped header is encoded once at
//     construction and the record body is appended into a preallocated bank.
//   - Record never waits for I/O. When both banks are busy the record is dropped
//     and counted; a run whose bundle reports any drop fails validation, so lost
//     evidence is visible rather than silently absorbed by a stalled hot path.
type Writer struct {
	header   []byte
	writerID identity.WriterID

	mu     sync.Mutex
	active *bank
	closed bool

	free    chan *bank
	pending chan *bank

	seq     atomic.Uint64
	dropped atomic.Uint64
	written atomic.Uint64

	flusherDone chan struct{}
	flushErr    atomic.Pointer[error]

	closeOnce sync.Once
	sink      io.WriteCloser
}

// WriterOption configures a Writer at construction.
type WriterOption func(*writerConfig)

type writerConfig struct {
	bankBytes int
	banks     int
}

// WithBankBytes sets the size of each buffer bank.
func WithBankBytes(n int) WriterOption {
	return func(c *writerConfig) { c.bankBytes = n }
}

// WithBanks sets how many buffer banks circulate between the record path and
// the flusher. Two is the minimum that decouples them.
func WithBanks(n int) WriterOption {
	return func(c *writerConfig) { c.banks = n }
}

// NewFileWriter opens path and streams length-delimited records into it.
func NewFileWriter(path string, h RunHeader, opts ...WriterOption) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("events: create record stream %s: %w", path, err)
	}
	w, err := NewWriter(f, h, opts...)
	if err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// NewWriter streams length-delimited records into sink. The writer takes
// ownership of sink and closes it on Close.
func NewWriter(sink io.WriteCloser, h RunHeader, opts ...WriterOption) (*Writer, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}
	cfg := writerConfig{bankBytes: DefaultBankBytes, banks: 2}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.banks < 2 {
		return nil, fmt.Errorf("events: need at least 2 banks to decouple recording from flushing, got %d", cfg.banks)
	}
	if cfg.bankBytes < maxBodySize*4 {
		return nil, fmt.Errorf("events: bank of %d bytes is too small for records of up to %d bytes", cfg.bankBytes, maxBodySize)
	}

	w := &Writer{
		header:      encodeHeader(h),
		writerID:    h.WriterID,
		free:        make(chan *bank, cfg.banks),
		pending:     make(chan *bank, cfg.banks),
		flusherDone: make(chan struct{}),
		sink:        sink,
	}
	for i := 0; i < cfg.banks; i++ {
		b := &bank{buf: make([]byte, cfg.bankBytes)}
		if i == 0 {
			w.active = b
		} else {
			w.free <- b
		}
	}
	go w.flush()
	return w, nil
}

// maxFrameSize is the largest a single encoded record can be, including its
// length prefix.
func (w *Writer) maxFrameSize() int { return binary.MaxVarintLen64 + len(w.header) + maxBodySize }

// Record appends r to the stream and returns the sequence number it was given.
//
// It allocates nothing and does not block on I/O. A returned ok=false means the
// record was dropped because no bank was available; the count is reported in the
// bundle and fails validation.
func (w *Writer) Record(r *Record) (seq uint64, ok bool) {
	seq = w.seq.Add(1)
	r.Seq = seq

	w.mu.Lock()
	if w.closed || len(w.active.buf)-w.active.n < w.maxFrameSize() {
		if w.closed || !w.rotateLocked() {
			w.mu.Unlock()
			w.dropped.Add(1)
			return seq, false
		}
	}
	b := w.active
	// Encode the body after a reserved length prefix, then write the true length
	// back into the reservation. Reserving the maximum and shifting is what keeps
	// this to one pass with no scratch buffer.
	start := b.n + binary.MaxVarintLen64
	body := appendRecord(append(b.buf[start:start:len(b.buf)], w.header...), r, w.writerID)

	prefix := binary.AppendUvarint(b.buf[b.n:b.n:start], uint64(len(body)))
	// The prefix is shorter than the reservation, so slide the body back against it.
	copy(b.buf[b.n+len(prefix):], body)
	b.n += len(prefix) + len(body)
	w.mu.Unlock()

	w.written.Add(1)
	return seq, true
}

// rotateLocked hands the active bank to the flusher and takes a free one. It
// reports false when every other bank is still in flight, which is the only
// condition under which a record is dropped.
func (w *Writer) rotateLocked() bool {
	select {
	case next := <-w.free:
		w.pending <- w.active
		w.active = next
		return true
	default:
		return false
	}
}

// flush drains full banks to the sink. It is the only goroutine that touches
// the sink, so records reach the file in bank order.
func (w *Writer) flush() {
	defer close(w.flusherDone)
	out := bufio.NewWriterSize(w.sink, 1<<20)
	for b := range w.pending {
		if _, err := out.Write(b.buf[:b.n]); err != nil {
			w.setFlushErr(fmt.Errorf("events: flush record bank: %w", err))
		}
		b.reset()
		w.free <- b
	}
	if err := out.Flush(); err != nil {
		w.setFlushErr(fmt.Errorf("events: flush record stream: %w", err))
	}
}

func (w *Writer) setFlushErr(err error) {
	w.flushErr.CompareAndSwap(nil, &err)
}

// Stats reports what the stream recorded. Dropped is evidence about the
// instrument itself and belongs in the run bundle.
type Stats struct {
	Written uint64 `json:"written"`
	Dropped uint64 `json:"dropped"`
}

// Stats returns the stream's counters.
func (w *Writer) Stats() Stats {
	return Stats{Written: w.written.Load(), Dropped: w.dropped.Load()}
}

// Close flushes every buffered record and closes the sink.
func (w *Writer) Close() error {
	var err error
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		if w.active.n > 0 {
			w.pending <- w.active
		}
		w.mu.Unlock()

		close(w.pending)
		<-w.flusherDone

		if e := w.flushErr.Load(); e != nil {
			err = *e
		}
		if cerr := w.sink.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("events: close record stream: %w", cerr)
		}
	})
	return err
}

// ReadStream decodes every record in a length-delimited stream. It is an
// offline reader for the validator and the archive converter.
func ReadStream(r io.Reader) ([]Decoded, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	var out []Decoded
	for {
		size, err := binary.ReadUvarint(br)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, fmt.Errorf("events: read record length: %w", err)
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(br, body); err != nil {
			return out, fmt.Errorf("events: read record body: %w", err)
		}
		d, err := Decode(body)
		if err != nil {
			return out, err
		}
		out = append(out, d)
	}
}

// ReadFile decodes a record stream from disk.
func ReadFile(path string) ([]Decoded, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("events: open record stream %s: %w", path, err)
	}
	defer f.Close()
	return ReadStream(f)
}

// StatsPath is the sidecar file holding a stream's counters.
//
// A stream written by another process leaves its counters in that process's
// memory, and reading the file back can only recover how many records arrived —
// never how many were dropped. Persisting the counters is what keeps a drop
// reportable across a process boundary; without it a bundle would state
// "dropped: 0" on no evidence, and the validator's drop check could never fire
// for the shared path.
func StatsPath(streamPath string) string { return streamPath + ".stats.json" }

// WriteStats records a stream's counters beside it. Services call this after
// Close, so the counts describe the completed stream.
func WriteStats(streamPath string, s Stats) error {
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("events: encode stream stats: %w", err)
	}
	if err := os.WriteFile(StatsPath(streamPath), append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("events: write stream stats: %w", err)
	}
	return nil
}

// ReadStats recovers a stream's counters. A missing file is an error rather than
// a zero: "no drops" and "nobody said" must not look alike.
func ReadStats(streamPath string) (Stats, error) {
	var s Stats
	b, err := os.ReadFile(StatsPath(streamPath))
	if err != nil {
		return s, fmt.Errorf("events: read stream stats for %s: %w", streamPath, err)
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("events: parse stream stats for %s: %w", streamPath, err)
	}
	return s, nil
}
