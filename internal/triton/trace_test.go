package triton

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/matthewhoung/batch2go/internal/identity"
)

// Triton emits a trace as a stream of fragments sharing a trace id: one carries
// the request id, others carry a timestamp each, and some carry several. The
// parser has to reassemble them into per-request events, and it must join by the
// request id the client set — never by timing.
const traceFixture = `[
{"id":2,"model_name":"synthetic_unbatched","model_version":1,"request_id":"c1/o0"},
{"id":2,"timestamps":[{"name":"REQUEST_START","ns":1000}]},
{"id":2,"timestamps":[{"name":"QUEUE_START","ns":1100}]},
{"id":2,"timestamps":[{"name":"COMPUTE_START","ns":1200}]},
{"id":2,"timestamps":[{"name":"COMPUTE_INPUT_END","ns":1250}]},
{"id":2,"timestamps":[{"name":"COMPUTE_OUTPUT_START","ns":1750}]},
{"id":2,"timestamps":[{"name":"COMPUTE_END","ns":1800}]},
{"id":2,"timestamps":[{"name":"REQUEST_END","ns":1900}]},
{"id":3,"model_name":"synthetic_unbatched","request_id":"c1/o1"},
{"id":3,"timestamps":[{"name":"QUEUE_START","ns":2100},{"name":"COMPUTE_START","ns":2200},{"name":"COMPUTE_END","ns":2800}]},
{"id":4,"model_name":"synthetic_unbatched"},
{"id":4,"timestamps":[{"name":"QUEUE_START","ns":3100}]}
]`

func writeFixture(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "trace.json.0")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

func TestParseTraceFileJoinsFragmentsByRequestID(t *testing.T) {
	got, err := ParseTraceFile(writeFixture(t, traceFixture))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// The third trace has no request id, so it is not attributable to a logical
	// request and must be dropped rather than matched by timing.
	if len(got) != 2 {
		t.Fatalf("parsed %d events, want 2 attributable ones", len(got))
	}

	first := got[0]
	if want := (identity.LogicalRequest{Cohort: 1, Ordinal: 0}); first.Request != want {
		t.Errorf("first event request = %v, want %v", first.Request, want)
	}
	if first.QueueStart != 1100 || first.ComputeStart != 1200 || first.ComputeEnd != 1800 {
		t.Errorf("first event schema timestamps = %d/%d/%d, want 1100/1200/1800",
			first.QueueStart, first.ComputeStart, first.ComputeEnd)
	}
	// Sub-spans are retained inside S_comp rather than discarded.
	if first.ComputeInputEnd != 1250 || first.ComputeOutputStart != 1750 {
		t.Errorf("sub-spans = %d/%d, want 1250/1750", first.ComputeInputEnd, first.ComputeOutputStart)
	}
	if !first.Complete() {
		t.Error("first event should be complete")
	}

	second := got[1]
	if want := (identity.LogicalRequest{Cohort: 1, Ordinal: 1}); second.Request != want {
		t.Errorf("second event request = %v, want %v", second.Request, want)
	}
	if !second.Complete() {
		t.Error("a trace carrying all three timestamps in one fragment should be complete")
	}
}

func TestIncompleteTraceIsNotComplete(t *testing.T) {
	got, err := ParseTraceFile(writeFixture(t, `[
{"id":9,"request_id":"c2/o3"},
{"id":9,"timestamps":[{"name":"QUEUE_START","ns":5000}]}
]`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("parsed %d events, want 1", len(got))
	}
	if got[0].Complete() {
		t.Error("a trace missing compute timestamps must not report itself complete")
	}
}

func TestParseEmptyTraceFile(t *testing.T) {
	got, err := ParseTraceFile(writeFixture(t, ""))
	if err != nil {
		t.Fatalf("an empty trace file is not an error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("parsed %d events from an empty file", len(got))
	}
}

func TestParseMalformedTraceFileFails(t *testing.T) {
	if _, err := ParseTraceFile(writeFixture(t, "{not json")); err == nil {
		t.Error("a malformed trace file must fail rather than yield no evidence silently")
	}
}
