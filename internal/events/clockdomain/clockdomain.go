// Package clockdomain establishes the verified monotonic clock domain that
// every timestamp in a run belongs to.
//
// The rule this package exists to enforce: stage arithmetic subtracts two
// monotonic-clock readings, and it may do so only when both readings came from
// the same clock on the same boot of the same kernel. Go's time.Time is not
// usable for this — it strips its monotonic component the moment it crosses a
// process boundary, so a serialized time.Time carries only wall-clock, which is
// subject to NTP steps and slew. Timestamps here are therefore raw
// CLOCK_MONOTONIC nanoseconds, and every record names the domain it was read in
// so that the validator can refuse a cross-domain subtraction rather than
// silently producing a number.
package clockdomain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	_ "unsafe" // for go:linkname

	"golang.org/x/sys/unix"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// nanotime is the runtime's own CLOCK_MONOTONIC reader. On Linux it resolves
// through the vDSO, so it costs tens of nanoseconds rather than the microsecond
// a clock_gettime syscall costs — and at 15 timestamps per logical request, on a
// path where A=off emits B times more records than A=on, that difference is
// exactly the kind of treatment-correlated overhead ADR-0004 keeps out of the
// measurement.
//
// Using it is only legitimate if it really is the same absolute clock the
// syscall reads, so Establish proves that by bracketing (see certifyFastClock)
// rather than assuming it.
//
//go:linkname nanotime runtime.nanotime
func nanotime() int64

// Source names which clock reader a domain certified.
type Source string

const (
	// SourceVDSO is the runtime's vDSO-backed CLOCK_MONOTONIC reader.
	SourceVDSO Source = "CLOCK_MONOTONIC/vdso"
	// SourceSyscall is the clock_gettime(CLOCK_MONOTONIC) syscall, used when the
	// fast reader fails certification.
	SourceSyscall Source = "CLOCK_MONOTONIC/syscall"
)

// Domain is the recorded identity of a clock domain: which clock, on which boot
// of which kernel, with what resolution and read cost. It is written once per
// session into the run bundle, and every event record references its ID.
type Domain struct {
	ID identity.ClockDomainID `json:"id"`

	// BootID is the kernel's boot identity. Two processes may subtract each
	// other's timestamps only if this matches: CLOCK_MONOTONIC restarts at boot,
	// so readings from different boots are unrelated numbers that would subtract
	// into plausible-looking nonsense.
	BootID string `json:"boot_id"`

	// BootTimeUnix is the kernel's boot instant in wall-clock seconds. It is
	// provenance for the session record; it never enters stage arithmetic.
	BootTimeUnix int64 `json:"boot_time_unix"`

	Hostname string `json:"hostname"`
	Source   Source `json:"source"`

	// ResolutionNanos is the clock's advertised granularity, and
	// ReadOverheadNanos the measured median cost of taking one timestamp. The
	// conservation tolerance is finalized against them (M2-PLAN §4.2).
	ResolutionNanos   int64 `json:"resolution_nanos"`
	ReadOverheadNanos int64 `json:"read_overhead_nanos"`
}

// Now reads the domain's clock. It is the only way a timestamp enters the
// system.
func (d *Domain) Now() int64 {
	if d.Source == SourceVDSO {
		return nanotime()
	}
	return syscallNow()
}

// SameDomain reports whether timestamps from d and other may be subtracted.
func (d *Domain) SameDomain(other *Domain) bool {
	return d != nil && other != nil && d.ID == other.ID
}

func syscallNow() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		// A monotonic clock that cannot be read is not a condition the measurement
		// can continue through: every downstream number would be fabricated.
		panic(fmt.Sprintf("clockdomain: CLOCK_MONOTONIC unreadable: %v", err))
	}
	return ts.Nano()
}

// Establish identifies the current clock domain and certifies a reader for it.
// It is called once per process per session; the resulting Domain is recorded in
// the run bundle and its ID is stamped on every event record.
func Establish() (*Domain, error) {
	bootID, err := readBootID()
	if err != nil {
		return nil, err
	}
	bootTime, err := readBootTime()
	if err != nil {
		return nil, err
	}
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("clockdomain: read hostname: %w", err)
	}

	var res unix.Timespec
	if err := unix.ClockGetres(unix.CLOCK_MONOTONIC, &res); err != nil {
		return nil, fmt.Errorf("clockdomain: read CLOCK_MONOTONIC resolution: %w", err)
	}

	source := SourceSyscall
	if certifyFastClock() {
		source = SourceVDSO
	}

	d := &Domain{
		BootID:          bootID,
		BootTimeUnix:    bootTime,
		Hostname:        host,
		Source:          source,
		ResolutionNanos: res.Nano(),
	}
	d.ID = deriveID(d)
	d.ReadOverheadNanos = measureReadOverhead(d)
	return d, nil
}

// certifyReads is how many bracketing trials certifyFastClock runs.
const certifyReads = 64

// certifyFastClock proves that the runtime's reader returns the same absolute
// CLOCK_MONOTONIC value the syscall returns, by checking that a fast read taken
// between two syscall reads always falls between them. A reader on a different
// clock, a different epoch, or different units cannot satisfy that.
func certifyFastClock() bool {
	for i := 0; i < certifyReads; i++ {
		before := syscallNow()
		fast := nanotime()
		after := syscallNow()
		if fast < before || fast > after {
			return false
		}
	}
	return true
}

// measureReadOverhead reports the median cost of one timestamp read, which is
// part of what the conservation tolerance is judged against.
func measureReadOverhead(d *Domain) int64 {
	const trials = 256
	const perTrial = 64

	samples := make([]int64, 0, trials)
	for i := 0; i < trials; i++ {
		start := d.Now()
		for j := 0; j < perTrial; j++ {
			_ = d.Now()
		}
		elapsed := d.Now() - start
		if elapsed > 0 {
			samples = append(samples, elapsed/perTrial)
		}
	}
	if len(samples) == 0 {
		return 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[len(samples)/2]
}

// deriveID hashes the facts that make two readings comparable. Any change to
// them — a reboot, a different host, a different clock — yields a different
// domain, and the validator refuses to subtract across domains.
func deriveID(d *Domain) identity.ClockDomainID {
	h := sha256.New()
	fmt.Fprintf(h, "batch2go/clockdomain/v1\nboot=%s\nbtime=%d\nhost=%s\nclock=%s\n",
		d.BootID, d.BootTimeUnix, d.Hostname, monotonicClockName)
	return identity.ClockDomainID("cd-" + hex.EncodeToString(h.Sum(nil))[:20])
}

// monotonicClockName is part of the domain hash so that a future change of clock
// cannot reuse an existing domain identity.
const monotonicClockName = "CLOCK_MONOTONIC"

// bootIDPath is the kernel's per-boot identity. It is read rather than
// synthesized so that containers on one host — Triton included — land in the
// same domain as the Go processes: CLOCK_MONOTONIC is shared across a host's
// containers unless a time namespace intervenes, and this file is how that is
// checked rather than assumed.
const bootIDPath = "/proc/sys/kernel/random/boot_id"

func readBootID() (string, error) {
	b, err := os.ReadFile(bootIDPath)
	if err != nil {
		return "", fmt.Errorf("clockdomain: read boot identity: %w", err)
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", fmt.Errorf("clockdomain: empty boot identity in %s", bootIDPath)
	}
	return id, nil
}

func readBootTime() (int64, error) {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, fmt.Errorf("clockdomain: read /proc/stat: %w", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if rest, ok := strings.CutPrefix(line, "btime "); ok {
			v, err := strconv.ParseInt(strings.TrimSpace(rest), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("clockdomain: parse btime: %w", err)
			}
			return v, nil
		}
	}
	return 0, fmt.Errorf("clockdomain: no btime line in /proc/stat")
}
