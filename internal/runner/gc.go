package runner

import (
	"math"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"time"
)

// Go's collector is a treatment-correlated covariate, not background noise. The
// two Factor-A levels allocate differently by construction — one B·P-sized
// envelope against B separate P-sized ones — so GC pressure and pause timing
// move systematically with treatment, at the millisecond scale being measured.
// The design's answer is to keep Go and measure the collector: pin its settings
// in the manifest, record its behavior per run, and refuse to interpret an
// effect smaller than the resulting bound (ADR-0004).

const (
	metricGCCycles  = "/gc/cycles/total:gc-cycles"
	metricGCPauses  = "/gc/pauses:seconds"
	metricHeapBytes = "/memory/classes/heap/objects:bytes"
)

// GCStats is one run's collector behavior, recorded alongside the settings that
// produced it so the two are never separated in the archive.
type GCStats struct {
	GOGC       int   `json:"gogc"`
	GOMEMLIMIT int64 `json:"gomemlimit_bytes"`

	Collections     uint64 `json:"collections"`
	TotalPauseNanos int64  `json:"total_pause_nanos"`
	PauseP99Nanos   int64  `json:"pause_p99_nanos"`
	HeapPeakBytes   uint64 `json:"heap_peak_bytes"`

	// SampleCount is how many heap samples the peak was taken over, so a peak
	// read from too few samples is visible as such rather than trusted.
	SampleCount int `json:"heap_sample_count"`
}

// gcSample is one reading of the collector's metrics.
type gcSample struct {
	cycles    uint64
	pauses    *metrics.Float64Histogram
	heapBytes uint64
}

func readGC() gcSample {
	samples := []metrics.Sample{
		{Name: metricGCCycles},
		{Name: metricGCPauses},
		{Name: metricHeapBytes},
	}
	metrics.Read(samples)

	s := gcSample{}
	if samples[0].Value.Kind() == metrics.KindUint64 {
		s.cycles = samples[0].Value.Uint64()
	}
	if samples[1].Value.Kind() == metrics.KindFloat64Histogram {
		s.pauses = samples[1].Value.Float64Histogram()
	}
	if samples[2].Value.Kind() == metrics.KindUint64 {
		s.heapBytes = samples[2].Value.Uint64()
	}
	return s
}

// GCRecorder measures the collector across a run.
type GCRecorder struct {
	gogc       int
	gomemlimit int64
	interval   time.Duration

	start gcSample

	mu       sync.Mutex
	heapPeak uint64
	samples  int

	stop chan struct{}
	done chan struct{}
}

// PinGC applies the manifest's collector settings and returns a recorder for
// them. Settings are applied, not merely recorded: a bundle that reports GOGC
// without having set it would be describing a run that did not happen.
func PinGC(gogc int, gomemlimit int64, sampleInterval time.Duration) *GCRecorder {
	debug.SetGCPercent(gogc)
	debug.SetMemoryLimit(gomemlimit)

	if sampleInterval <= 0 {
		sampleInterval = 20 * time.Millisecond
	}
	r := &GCRecorder{
		gogc:       gogc,
		gomemlimit: gomemlimit,
		interval:   sampleInterval,
	}
	return r
}

// Start begins recording. The heap sampler runs on its own goroutine so that no
// data-plane path pays for it.
func (r *GCRecorder) Start() {
	r.start = readGC()
	r.heapPeak = r.start.heapBytes
	r.samples = 1
	r.stop = make(chan struct{})
	r.done = make(chan struct{})

	go func() {
		defer close(r.done)
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-ticker.C:
				s := readGC()
				r.mu.Lock()
				if s.heapBytes > r.heapPeak {
					r.heapPeak = s.heapBytes
				}
				r.samples++
				r.mu.Unlock()
			}
		}
	}()
}

// Stop ends recording and returns the run's statistics.
func (r *GCRecorder) Stop() GCStats {
	if r.stop != nil {
		close(r.stop)
		<-r.done
		r.stop = nil
	}
	end := readGC()

	r.mu.Lock()
	heapPeak, samples := r.heapPeak, r.samples
	r.mu.Unlock()
	if end.heapBytes > heapPeak {
		heapPeak = end.heapBytes
	}

	total, p99 := pauseDelta(r.start.pauses, end.pauses)
	return GCStats{
		GOGC:            r.gogc,
		GOMEMLIMIT:      r.gomemlimit,
		Collections:     end.cycles - r.start.cycles,
		TotalPauseNanos: total,
		PauseP99Nanos:   p99,
		HeapPeakBytes:   heapPeak,
		SampleCount:     samples + 1,
	}
}

// pauseDelta reports the total and 99th-percentile pause across the run, from
// the difference of two cumulative pause histograms.
//
// The p99 is a bucket upper bound, which is the most the histogram can honestly
// support: Go reports pauses bucketed, so a percentile from it is an upper bound
// on the true value, not an estimate of it.
func pauseDelta(start, end *metrics.Float64Histogram) (totalNanos, p99Nanos int64) {
	if end == nil {
		return 0, 0
	}
	counts := make([]uint64, len(end.Counts))
	copy(counts, end.Counts)
	if start != nil && len(start.Counts) == len(counts) {
		for i := range counts {
			counts[i] -= start.Counts[i]
		}
	}

	var observations uint64
	var totalSeconds float64
	for i, c := range counts {
		if c == 0 {
			continue
		}
		observations += c
		totalSeconds += float64(c) * bucketMidpoint(end.Buckets, i)
	}
	if observations == 0 {
		return 0, 0
	}

	target := uint64(math.Ceil(0.99 * float64(observations)))
	var cumulative uint64
	for i, c := range counts {
		cumulative += c
		if cumulative >= target {
			p99Nanos = int64(bucketUpper(end.Buckets, i) * float64(time.Second))
			break
		}
	}
	return int64(totalSeconds * float64(time.Second)), p99Nanos
}

// bucketMidpoint is the representative duration of bucket i. Buckets has one
// more entry than Counts: they are the bucket edges.
func bucketMidpoint(buckets []float64, i int) float64 {
	lo, hi := bucketBounds(buckets, i)
	if math.IsInf(hi, 1) {
		return lo
	}
	if math.IsInf(lo, -1) {
		return hi
	}
	return (lo + hi) / 2
}

func bucketUpper(buckets []float64, i int) float64 {
	_, hi := bucketBounds(buckets, i)
	if math.IsInf(hi, 1) {
		lo, _ := bucketBounds(buckets, i)
		return lo
	}
	return hi
}

func bucketBounds(buckets []float64, i int) (lo, hi float64) {
	if i < 0 || i+1 >= len(buckets) {
		return 0, 0
	}
	return buckets[i], buckets[i+1]
}
