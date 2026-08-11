package proxy

import (
	"io"
	"strings"
	"testing"
	"time"

	envelopev1 "github.com/matthewhoung/batch2go/api/envelope/v1"
	"github.com/matthewhoung/batch2go/internal/envelope"
	"github.com/matthewhoung/batch2go/internal/events"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// The formation deadline is an experimental quantity: it bounds how long a
// cohort is held, and a cohort that cannot form costs one request at A=off and B
// at A=on. A code default would be a number nobody declared, so the proxy
// refuses to run without one where it holds cohorts — and refuses to carry one
// where it does not, since a deadline there would bound a wait that never
// happens (ADR-0010).
func TestFormationDeadlineExistsExactlyWhereTheProxyAggregates(t *testing.T) {
	for _, cell := range identity.AllCells() {
		if cell == identity.CellD0 {
			continue // D0 does not traverse the proxy at all
		}

		absent := Config{Cell: cell, Run: "run-1", TargetB: 4}
		err := absent.Validate()
		switch {
		case cell.AggregatesEnvelopes() && err == nil:
			t.Errorf("cell %s aggregates and must be refused without a formation deadline", cell)
		case cell.AggregatesEnvelopes() && !strings.Contains(err.Error(), "formation deadline"):
			t.Errorf("cell %s: refusal %q should name the deadline", cell, err)
		case !cell.AggregatesEnvelopes() && err != nil:
			t.Errorf("cell %s forms no cohort and needs no deadline: %v", cell, err)
		}

		present := Config{Cell: cell, Run: "run-1", TargetB: 4, FormationDeadline: 5 * time.Second}
		err = present.Validate()
		switch {
		case cell.AggregatesEnvelopes() && err != nil:
			t.Errorf("cell %s: %v", cell, err)
		case !cell.AggregatesEnvelopes() && err == nil:
			t.Errorf("cell %s forms no cohort and must not carry a formation deadline", cell)
		}
	}
}

func TestConfigRefusesWhatItCouldNotServe(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no run":       {Cell: identity.CellF00, TargetB: 4},
		"no cell":      {Run: "run-1", TargetB: 4},
		"no target B":  {Cell: identity.CellF00, Run: "run-1"},
		"negative B":   {Cell: identity.CellF00, Run: "run-1", TargetB: -1},
		"F10 with B=0": {Cell: identity.CellF10, Run: "run-1", FormationDeadline: time.Second},
	} {
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: should have been refused", name)
		}
	}
}

// The proxy is constructed with everything it needs or not at all. A nil
// collaborator would surface as a panic on the first request, in a process whose
// job is to record what happened.
//
// Each collaborator is withheld on its own. Nilling all four at once would prove
// only that the first check exists: the other three could be deleted and the
// test would still pass, leaving a proxy that accepts a nil writer and panics
// the moment it has something to record.
func TestNewRefusesIncompleteConstruction(t *testing.T) {
	cfg := Config{Cell: identity.CellF00, Run: "run-1", TargetB: 4}
	builder, err := envelope.NewBuilder(envelope.Config{
		Run: "run-1", Cell: identity.CellF00, ClockDomain: "cd-test000000000000", TargetB: 4,
	})
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	backend := envelopev1.NewBackendClient(nil)
	writer, err := events.NewWriter(nopCloser{io.Discard}, events.RunHeader{
		Experiment: "exp", Session: "sess", Run: "run-1",
		Cell: identity.CellF00, ClockDomain: "cd-test000000000000", WriterID: 2,
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	defer writer.Close()
	now := Clock(func() int64 { return 0 })

	for name, withhold := range map[string]func() (*envelope.Builder, envelopev1.BackendClient, *events.Writer, Clock){
		"no builder": func() (*envelope.Builder, envelopev1.BackendClient, *events.Writer, Clock) {
			return nil, backend, writer, now
		},
		"no backend": func() (*envelope.Builder, envelopev1.BackendClient, *events.Writer, Clock) {
			return builder, nil, writer, now
		},
		"no writer": func() (*envelope.Builder, envelopev1.BackendClient, *events.Writer, Clock) {
			return builder, backend, nil, now
		},
		"no clock": func() (*envelope.Builder, envelopev1.BackendClient, *events.Writer, Clock) {
			return builder, backend, writer, nil
		},
	} {
		b, bk, w, c := withhold()
		if _, err := New(cfg, b, bk, w, c); err == nil {
			t.Errorf("%s: the proxy must refuse to be built", name)
		}
	}

	// And with all four present it builds, or the cases above would pass for the
	// wrong reason.
	if _, err := New(cfg, builder, backend, writer, now); err != nil {
		t.Errorf("a fully constructed proxy should build: %v", err)
	}
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }
