package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/olpie101/fast-forward/kv"
)

// newOrderEvent builds an event under aggregate "order" using a deterministic
// payload string ("payload-vN").
func newOrderEvent(t *testing.T, aggID uuid.UUID, name string, version int) event.Event {
	t.Helper()
	return event.New[any](
		name,
		"payload",
		event.Aggregate(aggID, "order", version),
	).Any()
}

func TestInsertIntegration(t *testing.T) {
	const eventName = "order.created"

	t.Run("single_event", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		evt := newOrderEvent(t, aggID, eventName, 1)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.store.Insert(ctx, evt); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		// Subject + headers via JetStream stream introspection.
		str, err := h.js.Stream(ctx, h.streamName)
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		raw, err := str.GetMsg(ctx, 1)
		if err != nil {
			t.Fatalf("GetMsg(1): %v", err)
		}
		wantSubject := "es.order." + aggID.String() + ".1.order_created"
		if raw.Subject != wantSubject {
			t.Fatalf("Subject = %q, want %q", raw.Subject, wantSubject)
		}
		if got := raw.Header.Get(MetadataKeyEventName); got != eventName {
			t.Errorf("event-name = %q, want %q", got, eventName)
		}
		if got := raw.Header.Get(MetadataKeyEventAggregateVersion); got != "1" {
			t.Errorf("aggregate-version = %q, want 1", got)
		}
		if got := raw.Header.Get(jetstream.MsgIDHeader); got != evt.ID().String() {
			t.Errorf("MsgIDHeader = %q, want %q", got, evt.ID().String())
		}

		// versionKV updated to 1.
		v, _, err := h.store.versionKV.Get(ctx, "order."+aggID.String())
		if err != nil {
			t.Fatalf("versionKV.Get: %v", err)
		}
		if v.Value != 1 {
			t.Errorf("versionKV.Value = %d, want 1", v.Value)
		}

		// Write lease released.
		if _, _, err := h.store.writeLeaseKV.Get(ctx, "order."+aggID.String()); !errors.Is(err, nats.ErrKeyNotFound) {
			t.Errorf("writeLeaseKV.Get err = %v, want ErrKeyNotFound", err)
		}
	})

	t.Run("batch_same_aggregate", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		evts := []event.Event{
			newOrderEvent(t, aggID, eventName, 1),
			newOrderEvent(t, aggID, eventName, 2),
			newOrderEvent(t, aggID, eventName, 3),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.store.Insert(ctx, evts...); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		v, _, err := h.store.versionKV.Get(ctx, "order."+aggID.String())
		if err != nil {
			t.Fatalf("versionKV.Get: %v", err)
		}
		if v.Value != 3 {
			t.Errorf("versionKV.Value = %d, want 3", v.Value)
		}
		if _, _, err := h.store.writeLeaseKV.Get(ctx, "order."+aggID.String()); !errors.Is(err, nats.ErrKeyNotFound) {
			t.Errorf("writeLeaseKV.Get err = %v, want ErrKeyNotFound", err)
		}
	})

	t.Run("batch_multiple_aggregates", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggA, aggB := uuid.New(), uuid.New()
		evts := []event.Event{
			newOrderEvent(t, aggA, eventName, 1),
			newOrderEvent(t, aggB, eventName, 1),
			newOrderEvent(t, aggA, eventName, 2),
			newOrderEvent(t, aggB, eventName, 2),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.store.Insert(ctx, evts...); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		for _, id := range []uuid.UUID{aggA, aggB} {
			key := "order." + id.String()
			v, _, err := h.store.versionKV.Get(ctx, key)
			if err != nil {
				t.Fatalf("versionKV.Get(%s): %v", key, err)
			}
			if v.Value != 2 {
				t.Errorf("versionKV.Value(%s) = %d, want 2", key, v.Value)
			}
			if _, _, err := h.store.writeLeaseKV.Get(ctx, key); !errors.Is(err, nats.ErrKeyNotFound) {
				t.Errorf("writeLeaseKV(%s) leaked: err = %v", key, err)
			}
		}
	})

	t.Run("version_mismatch", func(t *testing.T) {
		// P0-1 fix: a fresh aggregate cannot trip ErrValidationVersionMismatch
		// (getOrCreateCurrentVersion returns ver=0; obtainLeases store.go:304-307
		// resets v.current = v.next-1 so genPublishMsgs store.go:228-231 always
		// sees current==next-1 on a fresh aggregate). Seed versionKV with v=1
		// first by inserting v=1; then call Insert at v=3 to trip the check.
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		key := "order." + aggID.String()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.store.Insert(ctx, newOrderEvent(t, aggID, eventName, 1)); err != nil {
			t.Fatalf("seed Insert v=1: %v", err)
		}

		// versionKV is now {Value:1}.
		err := h.store.Insert(ctx, newOrderEvent(t, aggID, eventName, 3))
		if !errors.Is(err, ErrValidationVersionMismatch) {
			t.Fatalf("Insert v=3 err = %v, want ErrValidationVersionMismatch", err)
		}

		// Lease post-state: deferred releaseLeases(_, _, true) at store.go:105-110
		// runs versionKV.Update at store.go:317-321. genPublishMsgs returned the
		// validation error BEFORE any js.PublishMsg call (store.go:112-114), so
		// the publish loop is skipped. The deferred release proceeds: Update
		// succeeds (rev still valid), then writeLeaseKV.Delete runs — lease key
		// is deleted on this path.
		if _, _, err := h.store.writeLeaseKV.Get(ctx, key); !errors.Is(err, nats.ErrKeyNotFound) {
			t.Errorf("writeLeaseKV after validation error: err = %v, want ErrKeyNotFound", err)
		}

		// versionKV side-effect [P1-2 — passing characterization of buggy behavior]:
		// BUG: store.go:105-110,317-321 — on validation error, versionKV must
		// remain unchanged; current code updates it via deferred
		// releaseLeases(ctx, leases, true). The lease's finalVersion is the
		// rejected version (3), so versionKV is bumped to 3 even though the
		// publish was rejected. Asserting current observed value to detect
		// drift; flip assertion when bug is fixed.
		v, _, err := h.store.versionKV.Get(ctx, key)
		if err != nil {
			t.Fatalf("versionKV.Get post-mismatch: %v", err)
		}
		if v.Value != 3 {
			t.Fatalf("versionKV.Value = %d, want 3 (current buggy behavior — flip when store.go:105-110,317-321 is fixed to skip versionKV update on validation failure)", v.Value)
		}
	})

	t.Run("lease_locked_contention", func(t *testing.T) {
		// P0-2 fix: pre-create the lease key directly (mirrors store.go:292),
		// then a single Insert hits the AlreadyExists branch at store.go:294-297
		// and returns ErrLeaseLocked deterministically.
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		key := "order." + aggID.String()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := h.store.writeLeaseKV.Create(ctx, key, &kv.NilValue{}); err != nil {
			t.Fatalf("pre-create lease: %v", err)
		}

		err := h.store.Insert(ctx, newOrderEvent(t, aggID, eventName, 1))
		if !errors.Is(err, ErrLeaseLocked) {
			t.Fatalf("Insert err = %v, want ErrLeaseLocked", err)
		}
	})

	t.Run("empty_events", func(t *testing.T) {
		h := newHarness(t, "order")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.store.Insert(ctx); err != nil {
			t.Fatalf("Insert(no events) err = %v, want nil", err)
		}

		// No KV writes occurred.
		keys, err := h.store.versionKV.Keys(ctx)
		if err != nil {
			t.Fatalf("versionKV.Keys: %v", err)
		}
		if len(keys) != 0 {
			t.Fatalf("versionKV keys = %v, want none", keys)
		}
		keys, err = h.store.writeLeaseKV.Keys(ctx)
		if err != nil {
			t.Fatalf("writeLeaseKV.Keys: %v", err)
		}
		if len(keys) != 0 {
			t.Fatalf("writeLeaseKV keys = %v, want none", keys)
		}
	})
}
