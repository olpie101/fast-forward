package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/olpie101/fast-forward/kv"
)

func TestCheckServerVersion(t *testing.T) {
	tests := []struct {
		version string
		wantErr bool
	}{
		{version: "2.10.0"},
		{version: "2.10.1"},
		{version: "3.0.0"},
		{version: "2.9.99", wantErr: true},
		{version: "1.99.0", wantErr: true},
		{version: "x.10.0", wantErr: true},
		{version: "2.x.0", wantErr: true},
		{version: "2", wantErr: true},
		{version: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			err := checkServerVersion(tt.version)
			if tt.wantErr {
				if err == nil {
					t.Fatal("checkServerVersion() error = nil, want error")
				}
				if !errors.Is(err, ErrUnsupportedServerVersion) {
					t.Fatalf("checkServerVersion() error = %v, want errors.Is ErrUnsupportedServerVersion", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("checkServerVersion() error = %v", err)
			}
		})
	}
}

func TestValidateStore(t *testing.T) {
	newValidStore := func() *Store {
		return &Store{
			js:           stubJetStream{},
			enc:          fakeEncoding{},
			writeLeaseKV: &fakeKeyValuer[*kv.NilValue]{},
			versionKV:    &fakeKeyValuer[*kv.UInt64Value]{},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*Store)
		wantErr string
	}{
		{name: "valid"},
		{name: "nil jetstream", mutate: func(s *Store) { s.js = nil }, wantErr: "jetstream cannot be nil"},
		{name: "nil encoding", mutate: func(s *Store) { s.enc = nil }, wantErr: "encoding cannot be nil"},
		{name: "nil write lease kv", mutate: func(s *Store) { s.writeLeaseKV = nil }, wantErr: "write lease kv cannot be nil"},
		{name: "nil version kv", mutate: func(s *Store) { s.versionKV = nil }, wantErr: "aggregate version kv cannot be nil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newValidStore()
			if tt.mutate != nil {
				tt.mutate(s)
			}
			err := validateStore(s)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateStore() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateStore() error = nil, want %q", tt.wantErr)
			}
			if err.Error() != tt.wantErr {
				t.Fatalf("validateStore() error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestWriteLeaseKey(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	got := writeLeaseKey("order", id)
	want := "order." + id.String()
	if got != want {
		t.Fatalf("writeLeaseKey() = %q, want %q", got, want)
	}
}

func TestGenPublishMsgs(t *testing.T) {
	aggregateID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	eventID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	eventTime := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	evt := event.New[any](
		"order.created.v2",
		"payload",
		event.ID(eventID),
		event.Time(eventTime),
		event.Aggregate(aggregateID, "order", 2),
	)
	s := Store{enc: fakeEncoding{}}
	leases := map[string]versionInfo{
		writeLeaseKey("order", aggregateID): {current: 1, next: 2, finalVersion: 2},
	}

	msgs, err := s.genPublishMsgs([]event.Event{evt}, leases)
	if err != nil {
		t.Fatalf("genPublishMsgs() error = %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	msg := msgs[0]
	if want := "es.order." + aggregateID.String() + ".2.order_created_v2"; msg.Subject != want {
		t.Fatalf("subject = %q, want %q", msg.Subject, want)
	}
	if got, want := string(msg.Data), "encoded:payload"; got != want {
		t.Fatalf("data = %q, want %q", got, want)
	}
	assertHeaderValue(t, msg.Header, MetadataKeyEventName, "order.created.v2")
	assertHeaderValue(t, msg.Header, MetadataKeyEventTime, eventTime.Format(time.RFC3339Nano))
	assertHeaderValue(t, msg.Header, MetadataKeyEventAggregateName, "order")
	assertHeaderValue(t, msg.Header, MetadataKeyEventAggregateId, aggregateID.String())
	assertHeaderValue(t, msg.Header, MetadataKeyEventAggregateVersion, "2")
	assertHeaderValue(t, msg.Header, jetstream.MsgIDHeader, eventID.String())
}

func TestGenPublishMsgsVersionMismatch(t *testing.T) {
	aggregateID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	evt := event.New[any]("order.created", "payload", event.Aggregate(aggregateID, "order", 3))
	s := Store{enc: fakeEncoding{}}
	leases := map[string]versionInfo{
		writeLeaseKey("order", aggregateID): {current: 1, next: 3, finalVersion: 3},
	}

	msgs, err := s.genPublishMsgs([]event.Event{evt}, leases)
	if !errors.Is(err, ErrValidationVersionMismatch) {
		t.Fatalf("genPublishMsgs() error = %v, want ErrValidationVersionMismatch", err)
	}
	if msgs != nil {
		t.Fatalf("msgs = %#v, want nil", msgs)
	}
}

func TestGenPublishMsgsMarshalError(t *testing.T) {
	aggregateID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	marshalErr := errors.New("marshal failed")
	evt := event.New[any]("order.created", "payload", event.Aggregate(aggregateID, "order", 2))
	s := Store{enc: fakeEncoding{marshalErr: marshalErr}}
	leases := map[string]versionInfo{
		writeLeaseKey("order", aggregateID): {current: 1, next: 2, finalVersion: 2},
	}

	msgs, err := s.genPublishMsgs([]event.Event{evt}, leases)
	if !errors.Is(err, marshalErr) {
		t.Fatalf("genPublishMsgs() error = %v, want %v", err, marshalErr)
	}
	if msgs != nil {
		t.Fatalf("msgs = %#v, want nil", msgs)
	}
}

func TestReleaseLeasesWithoutVersionUpdate(t *testing.T) {
	writeKV := &fakeKeyValuer[*kv.NilValue]{}
	s := Store{writeLeaseKV: writeKV, versionKV: &fakeKeyValuer[*kv.UInt64Value]{}}
	leases := map[string]versionInfo{"order.1": {finalVersion: 5, rev: 7}}

	if err := s.releaseLeases(context.Background(), leases, false); err != nil {
		t.Fatalf("releaseLeases() error = %v", err)
	}
	assertStringsEqual(t, writeKV.deletes, []string{"order.1"})
	versionKV := s.versionKV.(*fakeKeyValuer[*kv.UInt64Value])
	if len(versionKV.updates) != 0 {
		t.Fatalf("version updates = %#v, want none", versionKV.updates)
	}
}

func TestReleaseLeasesWithVersionUpdate(t *testing.T) {
	writeKV := &fakeKeyValuer[*kv.NilValue]{}
	versionKV := &fakeKeyValuer[*kv.UInt64Value]{}
	s := Store{writeLeaseKV: writeKV, versionKV: versionKV}
	leases := map[string]versionInfo{"order.1": {finalVersion: 5, rev: 7}}

	if err := s.releaseLeases(context.Background(), leases, true); err != nil {
		t.Fatalf("releaseLeases() error = %v", err)
	}
	if len(versionKV.updates) != 1 {
		t.Fatalf("len(version updates) = %d, want 1", len(versionKV.updates))
	}
	update := versionKV.updates[0]
	if update.key != "order.1" || update.lastRevision != 7 || update.value.Value != 5 {
		t.Fatalf("update = %#v, want key order.1 rev 7 value 5", update)
	}
	assertStringsEqual(t, writeKV.deletes, []string{"order.1"})
}

func TestReleaseLeasesDeleteError(t *testing.T) {
	deleteErr := errors.New("delete failed")
	writeKV := &fakeKeyValuer[*kv.NilValue]{deleteErr: deleteErr}
	s := Store{writeLeaseKV: writeKV, versionKV: &fakeKeyValuer[*kv.UInt64Value]{}}

	err := s.releaseLeases(context.Background(), map[string]versionInfo{"order.1": {}}, false)
	if !errors.Is(err, deleteErr) {
		t.Fatalf("releaseLeases() error = %v, want %v", err, deleteErr)
	}
}

func TestReleaseLeasesUpdateError(t *testing.T) {
	updateErr := errors.New("update failed")
	writeKV := &fakeKeyValuer[*kv.NilValue]{}
	versionKV := &fakeKeyValuer[*kv.UInt64Value]{updateErr: updateErr}
	s := Store{writeLeaseKV: writeKV, versionKV: versionKV}

	err := s.releaseLeases(context.Background(), map[string]versionInfo{"order.1": {finalVersion: 5, rev: 7}}, true)
	if err == nil {
		t.Fatal("releaseLeases() error = nil, want error")
	}
	if !errors.Is(err, updateErr) {
		t.Fatalf("releaseLeases() error = %v, want wrapped %v", err, updateErr)
	}
	if want := "unable to update aggregate version"; !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("releaseLeases() error = %q, want prefix %q", err.Error(), want)
	}
	if len(writeKV.deletes) != 0 {
		t.Fatalf("deletes = %#v, want none after update error", writeKV.deletes)
	}
}

func TestGetOrCreateCurrentVersionFound(t *testing.T) {
	versionKV := &fakeKeyValuer[*kv.UInt64Value]{
		getValue: &kv.UInt64Value{Value: 12},
		getRev:   34,
	}
	s := Store{versionKV: versionKV}

	value, rev, err := s.getOrCreateCurrentVersion(context.Background(), "order.1")
	if err != nil {
		t.Fatalf("getOrCreateCurrentVersion() error = %v", err)
	}
	if value != 12 || rev != 34 {
		t.Fatalf("value, rev = %d, %d; want 12, 34", value, rev)
	}
	if len(versionKV.creates) != 0 {
		t.Fatalf("creates = %#v, want none", versionKV.creates)
	}
}

// TestGetOrCreateCurrentVersionNotFoundSeedsZeroWhenNoEvents exercises the
// cold-key reconciliation path when the stream has NO events for the aggregate:
// GetLastMsgForSubject returns ErrMsgNotFound, so the version reconciles to 0
// and versionKV is seeded with 0 (preserving the prior seed-0 behavior). A real
// js is required because the not-found branch now resolves the stream and reads
// its last message.
func TestGetOrCreateCurrentVersionNotFoundSeedsZeroWhenNoEvents(t *testing.T) {
	h := newHarness(t, "order")
	aggID := uuid.New()
	key := "order." + aggID.String()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	value, _, err := h.store.getOrCreateCurrentVersion(ctx, key)
	if err != nil {
		t.Fatalf("getOrCreateCurrentVersion() error = %v", err)
	}
	if value != 0 {
		t.Fatalf("value = %d, want 0 (stream has no events for the aggregate)", value)
	}
	got, _, err := h.store.versionKV.Get(ctx, key)
	if err != nil {
		t.Fatalf("versionKV.Get: %v", err)
	}
	if got.Value != 0 {
		t.Fatalf("seeded versionKV = %d, want 0", got.Value)
	}
}

// TestGetOrCreateCurrentVersionReconcilesFromStream covers the cold-key
// reconciliation: after inserting v1..3 the versionKV key is wiped while the
// stream still holds the events; a fresh getOrCreateCurrentVersion must derive
// version 3 from the stream's last message and re-seed versionKV with it.
func TestGetOrCreateCurrentVersionReconcilesFromStream(t *testing.T) {
	const eventName = "order.created"
	h := newHarness(t, "order")
	h.registerString(eventName)

	aggID := uuid.New()
	key := "order." + aggID.String()
	base := time.Now().UTC().Truncate(time.Millisecond)
	insertEventsAt(t, h,
		mkEvent(t, eventName, aggID, 1, base),
		mkEvent(t, eventName, aggID, 2, base.Add(time.Millisecond)),
		mkEvent(t, eventName, aggID, 3, base.Add(2*time.Millisecond)),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.store.versionKV.Delete(ctx, key); err != nil {
		t.Fatalf("wipe versionKV: %v", err)
	}

	value, _, err := h.store.getOrCreateCurrentVersion(ctx, key)
	if err != nil {
		t.Fatalf("getOrCreateCurrentVersion() error = %v", err)
	}
	if value != 3 {
		t.Fatalf("reconciled version = %d, want 3", value)
	}
	got, _, err := h.store.versionKV.Get(ctx, key)
	if err != nil {
		t.Fatalf("versionKV.Get: %v", err)
	}
	if got.Value != 3 {
		t.Fatalf("seeded versionKV = %d, want 3", got.Value)
	}
}

func TestGetOrCreateCurrentVersionGetError(t *testing.T) {
	getErr := errors.New("get failed")
	s := Store{versionKV: &fakeKeyValuer[*kv.UInt64Value]{getErr: getErr}}

	_, _, err := s.getOrCreateCurrentVersion(context.Background(), "order.1")
	if !errors.Is(err, getErr) {
		t.Fatalf("getOrCreateCurrentVersion() error = %v, want %v", err, getErr)
	}
}

// TestGetOrCreateCurrentVersionCreateError uses a real js for reconciliation
// (empty stream → reconciles to 0) but swaps in a fake versionKV whose Create
// fails, asserting the Create error propagates from the not-found branch.
func TestGetOrCreateCurrentVersionCreateError(t *testing.T) {
	h := newHarness(t, "order")
	createErr := errors.New("create failed")
	h.store.versionKV = &fakeKeyValuer[*kv.UInt64Value]{getErr: nats.ErrKeyNotFound, createErr: createErr}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	aggID := uuid.New()
	_, _, err := h.store.getOrCreateCurrentVersion(ctx, "order."+aggID.String())
	if !errors.Is(err, createErr) {
		t.Fatalf("getOrCreateCurrentVersion() error = %v, want %v", err, createErr)
	}
}

type stubJetStream struct {
	jetstream.JetStream
}

type fakeEncoding struct {
	marshalErr error
}

func (e fakeEncoding) Marshal(v any) ([]byte, error) {
	if e.marshalErr != nil {
		return nil, e.marshalErr
	}
	return []byte(fmt.Sprintf("encoded:%v", v)), nil
}

func (e fakeEncoding) Unmarshal(b []byte, name string) (any, error) {
	return string(b) + ":" + name, nil
}

type fakeKeyValuer[T kv.MarshalerUnmarshaler] struct {
	getValue T
	getRev   uint64
	getErr   error

	createRev uint64
	createErr error
	creates   []keyValueCall[T]

	updateErr error
	updates   []updateCall[T]

	deleteErr error
	deletes   []string
}

type keyValueCall[T kv.MarshalerUnmarshaler] struct {
	key   string
	value T
}

type updateCall[T kv.MarshalerUnmarshaler] struct {
	key          string
	value        T
	lastRevision uint64
}

func (f *fakeKeyValuer[T]) Keys(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (f *fakeKeyValuer[T]) Get(ctx context.Context, key string) (T, uint64, error) {
	var zero T
	if f.getErr != nil {
		return zero, 0, f.getErr
	}
	return f.getValue, f.getRev, nil
}

func (f *fakeKeyValuer[T]) GetAll(ctx context.Context, keys []string) ([]T, error) {
	return nil, nil
}

func (f *fakeKeyValuer[T]) Create(ctx context.Context, key string, value T) (uint64, error) {
	f.creates = append(f.creates, keyValueCall[T]{key: key, value: value})
	if f.createErr != nil {
		return 0, f.createErr
	}
	return f.createRev, nil
}

func (f *fakeKeyValuer[T]) Put(ctx context.Context, key string, value T) (uint64, error) {
	return 0, nil
}

func (f *fakeKeyValuer[T]) Update(ctx context.Context, key string, value T, lastRevision uint64) (uint64, error) {
	f.updates = append(f.updates, updateCall[T]{key: key, value: value, lastRevision: lastRevision})
	if f.updateErr != nil {
		return 0, f.updateErr
	}
	return lastRevision + 1, nil
}

func (f *fakeKeyValuer[T]) LastRevision(ctx context.Context, key string) (uint64, error) {
	return 0, nil
}

func (f *fakeKeyValuer[T]) Delete(ctx context.Context, key string, opts ...nats.DeleteOpt) error {
	f.deletes = append(f.deletes, key)
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return nil
}

func (f *fakeKeyValuer[T]) WatchAll(ctx context.Context, opts ...nats.WatchOpt) (<-chan kv.WatchValue[T], <-chan error, error) {
	return nil, nil, nil
}

func (f *fakeKeyValuer[T]) Watch(ctx context.Context, sub string, opts ...nats.WatchOpt) (<-chan kv.WatchValue[T], <-chan error, error) {
	return nil, nil, nil
}
