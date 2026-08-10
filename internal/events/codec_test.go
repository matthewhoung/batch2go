package events

import (
	"testing"

	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/matthewhoung/batch2go/api/events/v1"
	"github.com/matthewhoung/batch2go/internal/identity"
)

// The hot-path codec is hand-written for allocation reasons, so its only
// guarantee that it still speaks the schema in api/events/v1 is this file: every
// record must survive a round trip through the generated implementation, in both
// directions, with no field lost and no absence turned into a zero.

func testHeader() RunHeader {
	return RunHeader{
		Experiment:  "exp-walking-skeleton",
		Session:     "sess-0001",
		Run:         "run-0001",
		Cell:        identity.CellF00,
		ClockDomain: "cd-abcdef0123456789abcd",
		WriterID:    7,
	}
}

// fullRecord exercises every field, including a partial timestamp set: stages 3
// and 5 are deliberately absent so the round trip has to preserve absence rather
// than fill in zeros.
func fullRecord() Record {
	r := Record{
		Emitter:       identity.EmitterAdapter,
		Seq:           4242,
		Cohort:        17,
		Ordinal:       3,
		EnvelopeID:    99001,
		ExecutionID:   555,
		Status:        StatusOK,
		LogicalBytes:  262144,
		EnvelopeBytes: 262400,
		BatchSize:     1,
	}
	for _, s := range AllStages() {
		if s == StageProxyRecv || s == StageProxySend {
			continue
		}
		r.SetStage(s, 1_000_000_000+int64(s)*1_000)
	}
	r.SetMembership([]identity.UID{
		identity.LogicalRequest{Cohort: 17, Ordinal: 0}.UID(),
		identity.LogicalRequest{Cohort: 17, Ordinal: 1}.UID(),
		identity.LogicalRequest{Cohort: 17, Ordinal: 2}.UID(),
		identity.LogicalRequest{Cohort: 17, Ordinal: 3}.UID(),
	})
	return r
}

func encodeForTest(t *testing.T, h RunHeader, r *Record) []byte {
	t.Helper()
	buf := make([]byte, 0, 512+maxBodySize)
	buf = append(buf, encodeHeader(h)...)
	return appendRecord(buf, r, h.WriterID)
}

func TestHandRolledEncodingParsesAsGeneratedProtobuf(t *testing.T) {
	h := testHeader()
	rec := fullRecord()

	var got eventsv1.EventRecord
	if err := proto.Unmarshal(encodeForTest(t, h, &rec), &got); err != nil {
		t.Fatalf("generated decoder rejected hand-rolled encoding: %v", err)
	}

	if got.GetSchemaVersion() != SchemaVersion {
		t.Errorf("schema_version = %d, want %d", got.GetSchemaVersion(), SchemaVersion)
	}
	if got.GetRunId() != string(h.Run) || got.GetCell() != string(h.Cell) {
		t.Errorf("run identity = (%q,%q), want (%q,%q)", got.GetRunId(), got.GetCell(), h.Run, h.Cell)
	}
	if got.GetClockDomainId() != string(h.ClockDomain) {
		t.Errorf("clock_domain_id = %q, want %q", got.GetClockDomainId(), h.ClockDomain)
	}
	if got.GetEmitter() != eventsv1.Emitter_EMITTER_ADAPTER {
		t.Errorf("emitter = %v, want adapter", got.GetEmitter())
	}
	if got.GetCohortId() != 17 || got.GetOrdinal() != 3 {
		t.Errorf("identity = c%d/o%d, want c17/o3", got.GetCohortId(), got.GetOrdinal())
	}
	if want := TritonRequestID(rec.Request()); got.GetTritonRequestId() != want {
		t.Errorf("triton_request_id = %q, want %q", got.GetTritonRequestId(), want)
	}
	if got.GetPresenceMask() != uint32(rec.Presence) {
		t.Errorf("presence_mask = %b, want %b", got.GetPresenceMask(), rec.Presence)
	}
	if got.GetMembershipCount() != 4 || len(got.GetMembershipUids()) != 4 {
		t.Errorf("membership = %d uids (count %d), want 4/4", len(got.GetMembershipUids()), got.GetMembershipCount())
	}

	// Absence must survive as absence: an optional field the record did not carry
	// must arrive nil, not zero.
	if got.TProxyRecv != nil {
		t.Errorf("t_proxy_recv should be absent, got %d", got.GetTProxyRecv())
	}
	if got.TProxySend != nil {
		t.Errorf("t_proxy_send should be absent, got %d", got.GetTProxySend())
	}
	for _, s := range AllStages() {
		if s == StageProxyRecv || s == StageProxySend {
			continue
		}
		ptr := generatedStage(&got, s)
		if ptr == nil {
			t.Errorf("%s should be present, got nil", s)
			continue
		}
		if want := rec.TS[s]; *ptr != want {
			t.Errorf("%s = %d, want %d", s, *ptr, want)
		}
	}
}

func TestGeneratedEncodingParsesAsHandRolledDecoding(t *testing.T) {
	h := testHeader()
	want := fullRecord()

	msg := &eventsv1.EventRecord{
		SchemaVersion:   SchemaVersion,
		ExperimentId:    string(h.Experiment),
		SessionId:       string(h.Session),
		RunId:           string(h.Run),
		Cell:            string(h.Cell),
		ClockDomainId:   string(h.ClockDomain),
		Emitter:         eventsv1.Emitter_EMITTER_ADAPTER,
		WriterId:        uint32(h.WriterID),
		Seq:             want.Seq,
		CohortId:        uint32(want.Cohort),
		Ordinal:         uint32(want.Ordinal),
		EnvelopeId:      uint64(want.EnvelopeID),
		ExecutionId:     uint64(want.ExecutionID),
		TritonRequestId: TritonRequestID(want.Request()),
		PresenceMask:    uint32(want.Presence),
		Status:          eventsv1.Status_STATUS_OK,
		LogicalBytes:    want.LogicalBytes,
		EnvelopeBytes:   want.EnvelopeBytes,
		BatchSize:       want.BatchSize,
		MembershipCount: want.MembershipCount,
	}
	for _, uid := range want.MembershipUIDs() {
		msg.MembershipUids = append(msg.MembershipUids, int64(uid))
	}
	for _, s := range AllStages() {
		if ts, ok := want.Stage(s); ok {
			setGeneratedStage(msg, s, ts)
		}
	}

	wire, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal generated message: %v", err)
	}
	got, err := Decode(wire)
	if err != nil {
		t.Fatalf("hand-rolled decoder rejected generated encoding: %v", err)
	}

	if got.Header != h {
		t.Errorf("header = %+v, want %+v", got.Header, h)
	}
	assertRecordsEqual(t, got.Record, want)
}

func TestRecordSurvivesRoundTripThroughOwnCodec(t *testing.T) {
	h := testHeader()
	want := fullRecord()

	got, err := Decode(encodeForTest(t, h, &want))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SchemaVersion != SchemaVersion {
		t.Errorf("schema version = %d, want %d", got.SchemaVersion, SchemaVersion)
	}
	if got.Header != h {
		t.Errorf("header = %+v, want %+v", got.Header, h)
	}
	assertRecordsEqual(t, got.Record, want)
}

// A record with no membership evidence at all must not acquire any on the round
// trip: an empty uid set is the honest encoding of "this stage attested nothing".
func TestEmptyMembershipStaysEmpty(t *testing.T) {
	h := testHeader()
	rec := Record{Emitter: identity.EmitterLoadGen, Cohort: 1, Ordinal: 0, Status: StatusOK}
	rec.SetStage(StageSched, 500)

	got, err := Decode(encodeForTest(t, h, &rec))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Record.MembershipStored != 0 || got.Record.MembershipCount != 0 {
		t.Errorf("membership = %d stored / count %d, want 0/0",
			got.Record.MembershipStored, got.Record.MembershipCount)
	}
	if _, ok := got.Record.Stage(StageClientRecv); ok {
		t.Error("t_client_recv should be absent")
	}
}

// Evidence larger than a record can hold must report the size the execution
// claimed, so truncation is visible to the validator instead of looking like a
// smaller execution.
func TestOversizedMembershipReportsTruncation(t *testing.T) {
	h := testHeader()
	uids := make([]identity.UID, MaxMembership+8)
	for i := range uids {
		uids[i] = identity.LogicalRequest{Cohort: 1, Ordinal: identity.Ordinal(i)}.UID()
	}
	rec := Record{Emitter: identity.EmitterAdapter, Cohort: 1, Status: StatusOK}
	rec.SetMembership(uids)

	got, err := Decode(encodeForTest(t, h, &rec))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Record.MembershipCount != uint32(len(uids)) {
		t.Errorf("membership_count = %d, want %d", got.Record.MembershipCount, len(uids))
	}
	if int(got.Record.MembershipStored) != MaxMembership {
		t.Errorf("stored = %d, want %d", got.Record.MembershipStored, MaxMembership)
	}
}

func TestTritonRequestIDRoundTrips(t *testing.T) {
	for _, want := range []identity.LogicalRequest{
		{Cohort: 0, Ordinal: 0},
		{Cohort: 17, Ordinal: 3},
		{Cohort: 4294967295, Ordinal: 15},
	} {
		id := TritonRequestID(want)
		got, err := ParseTritonRequestID(id)
		if err != nil {
			t.Fatalf("parse %q: %v", id, err)
		}
		if got != want {
			t.Errorf("%q parsed to %v, want %v", id, got, want)
		}
	}
	if _, err := ParseTritonRequestID("not-an-id"); err == nil {
		t.Error("malformed request id should not parse")
	}
}

func assertRecordsEqual(t *testing.T, got, want Record) {
	t.Helper()
	if got.Emitter != want.Emitter || got.Seq != want.Seq || got.Status != want.Status {
		t.Errorf("emitter/seq/status = %v/%d/%v, want %v/%d/%v",
			got.Emitter, got.Seq, got.Status, want.Emitter, want.Seq, want.Status)
	}
	if got.Request() != want.Request() {
		t.Errorf("request = %v, want %v", got.Request(), want.Request())
	}
	if got.EnvelopeID != want.EnvelopeID || got.ExecutionID != want.ExecutionID {
		t.Errorf("envelope/execution = %d/%d, want %d/%d",
			got.EnvelopeID, got.ExecutionID, want.EnvelopeID, want.ExecutionID)
	}
	if got.Presence != want.Presence {
		t.Errorf("presence = %v, want %v", got.Presence, want.Presence)
	}
	if got.LogicalBytes != want.LogicalBytes || got.EnvelopeBytes != want.EnvelopeBytes || got.BatchSize != want.BatchSize {
		t.Errorf("bytes/batch = %d/%d/%d, want %d/%d/%d",
			got.LogicalBytes, got.EnvelopeBytes, got.BatchSize,
			want.LogicalBytes, want.EnvelopeBytes, want.BatchSize)
	}
	for _, s := range AllStages() {
		gts, gok := got.Stage(s)
		wts, wok := want.Stage(s)
		if gok != wok {
			t.Errorf("%s presence = %v, want %v", s, gok, wok)
			continue
		}
		if gok && gts != wts {
			t.Errorf("%s = %d, want %d", s, gts, wts)
		}
	}
	if got.MembershipCount != want.MembershipCount {
		t.Errorf("membership_count = %d, want %d", got.MembershipCount, want.MembershipCount)
	}
	gm, wm := got.MembershipUIDs(), want.MembershipUIDs()
	if len(gm) != len(wm) {
		t.Fatalf("membership size = %d, want %d", len(gm), len(wm))
	}
	for i := range gm {
		if gm[i] != wm[i] {
			t.Errorf("membership[%d] = %d, want %d", i, gm[i], wm[i])
		}
	}
}

// generatedStage and setGeneratedStage bridge the schema's stage numbering to
// the generated struct's fields, so the tests above can iterate stages rather
// than repeating fifteen near-identical assertions.
func generatedStage(m *eventsv1.EventRecord, s Stage) *int64 {
	switch s {
	case StageSched:
		return m.TSched
	case StageClientSend:
		return m.TClientSend
	case StageProxyRecv:
		return m.TProxyRecv
	case StageCohortSeal:
		return m.TCohortSeal
	case StageProxySend:
		return m.TProxySend
	case StageAdapterRecv:
		return m.TAdapterRecv
	case StageAdapterDispatch:
		return m.TAdapterDispatch
	case StageQueueStart:
		return m.TQueueStart
	case StageComputeStart:
		return m.TComputeStart
	case StageComputeEnd:
		return m.TComputeEnd
	case StageAdapterResult:
		return m.TAdapterResult
	case StageAdapterSend:
		return m.TAdapterSend
	case StageProxyRespRecv:
		return m.TProxyRespRecv
	case StageProxyFanout:
		return m.TProxyFanout
	case StageClientRecv:
		return m.TClientRecv
	}
	return nil
}

func setGeneratedStage(m *eventsv1.EventRecord, s Stage, v int64) {
	switch s {
	case StageSched:
		m.TSched = &v
	case StageClientSend:
		m.TClientSend = &v
	case StageProxyRecv:
		m.TProxyRecv = &v
	case StageCohortSeal:
		m.TCohortSeal = &v
	case StageProxySend:
		m.TProxySend = &v
	case StageAdapterRecv:
		m.TAdapterRecv = &v
	case StageAdapterDispatch:
		m.TAdapterDispatch = &v
	case StageQueueStart:
		m.TQueueStart = &v
	case StageComputeStart:
		m.TComputeStart = &v
	case StageComputeEnd:
		m.TComputeEnd = &v
	case StageAdapterResult:
		m.TAdapterResult = &v
	case StageAdapterSend:
		m.TAdapterSend = &v
	case StageProxyRespRecv:
		m.TProxyRespRecv = &v
	case StageProxyFanout:
		m.TProxyFanout = &v
	case StageClientRecv:
		m.TClientRecv = &v
	}
}
