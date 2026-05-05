package nats

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/nats-io/nats.go"
	"github.com/olpie101/fast-forward/kv"
)

// leaseEvt is a minimal helper that builds an event keyed to (name, id, version)
// for lease-lifecycle tests. The event payload is irrelevant — these tests
// exercise lease/version KV state directly without publishing.
func leaseEvt(t *testing.T, aggID uuid.UUID, version int) event.Event {
	t.Helper()
	return event.New[any](
		"order.created",
		"payload",
		event.Aggregate(aggID, "order", version),
	).Any()
}

func TestLeasesIntegration(t *testing.T) {
	t.Run("obtain_then_release_happy", func(t *testing.T) {
		h := newHarness(t, "order")

		aggA, aggB := uuid.New(), uuid.New()
		evts := []event.Event{
			leaseEvt(t, aggA, 1),
			leaseEvt(t, aggB, 1),
			leaseEvt(t, aggA, 2),
			leaseEvt(t, aggB, 2),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		obtained, err := h.store.obtainLeases(ctx, evts)
		if err != nil {
			t.Fatalf("obtainLeases: %v", err)
		}
		if len(obtained) != 2 {
			t.Fatalf("len(obtained) = %d, want 2", len(obtained))
		}
		for k, vi := range obtained {
			// next is set from the FIRST event seen for this aggregate
			// (store.go:262-273). With evts (1,1,2,2) interleaved, both
			// aggregates' first events are version=1.
			if vi.next != 1 {
				t.Errorf("%s: next = %d, want 1", k, vi.next)
			}
			if vi.finalVersion != 2 {
				t.Errorf("%s: finalVersion = %d, want 2", k, vi.finalVersion)
			}
			// Fresh aggregate: getOrCreateCurrentVersion creates ver=0;
			// obtainLeases line 304-307 resets v.current = v.next-1 = 0.
			if vi.current != 0 {
				t.Errorf("%s: current = %d, want 0", k, vi.current)
			}
			if vi.rev == 0 {
				t.Errorf("%s: rev = 0, want non-zero", k)
			}
		}

		if err := h.store.releaseLeases(ctx, obtained, true); err != nil {
			t.Fatalf("releaseLeases: %v", err)
		}

		for _, id := range []uuid.UUID{aggA, aggB} {
			key := "order." + id.String()
			v, _, err := h.store.versionKV.Get(ctx, key)
			if err != nil {
				t.Fatalf("versionKV.Get(%s): %v", key, err)
			}
			if v.Value != 2 {
				t.Errorf("%s versionKV = %d, want 2", key, v.Value)
			}
			if _, _, err := h.store.writeLeaseKV.Get(ctx, key); !errors.Is(err, nats.ErrKeyNotFound) {
				t.Errorf("%s lease leaked: err = %v", key, err)
			}
		}
	})

	t.Run("release_without_update", func(t *testing.T) {
		// Q2: pre-seed versionKV with a known prior value, take and release a
		// lease without updateVersion, then assert the prior value is intact
		// AND the lease is gone.
		h := newHarness(t, "order")

		aggID := uuid.New()
		key := "order." + aggID.String()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := h.store.versionKV.Put(ctx, key, &kv.UInt64Value{Value: 7}); err != nil {
			t.Fatalf("seed versionKV: %v", err)
		}

		obtained, err := h.store.obtainLeases(ctx, []event.Event{leaseEvt(t, aggID, 8)})
		if err != nil {
			t.Fatalf("obtainLeases: %v", err)
		}
		if err := h.store.releaseLeases(ctx, obtained, false); err != nil {
			t.Fatalf("releaseLeases: %v", err)
		}
		v, _, err := h.store.versionKV.Get(ctx, key)
		if err != nil {
			t.Fatalf("versionKV.Get: %v", err)
		}
		if v.Value != 7 {
			t.Errorf("versionKV.Value = %d, want 7 (unchanged)", v.Value)
		}
		if _, _, err := h.store.writeLeaseKV.Get(ctx, key); !errors.Is(err, nats.ErrKeyNotFound) {
			t.Errorf("lease not deleted: err = %v", err)
		}
	})

	t.Run("contention_returns_ErrLeaseLocked", func(t *testing.T) {
		h := newHarness(t, "order")

		aggID := uuid.New()
		key := "order." + aggID.String()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := h.store.writeLeaseKV.Create(ctx, key, &kv.NilValue{}); err != nil {
			t.Fatalf("pre-create lease: %v", err)
		}
		// Snapshot lease KV revision so we can detect partial-leak side effects.
		_, preRev, err := h.store.writeLeaseKV.Get(ctx, key)
		if err != nil {
			t.Fatalf("snapshot lease rev: %v", err)
		}

		_, err = h.store.obtainLeases(ctx, []event.Event{leaseEvt(t, aggID, 1)})
		if !errors.Is(err, ErrLeaseLocked) {
			t.Fatalf("obtainLeases err = %v, want ErrLeaseLocked", err)
		}

		// Lease should still be present at the same revision (no Create or
		// Delete by the failed obtainLeases call on this key).
		_, postRev, err := h.store.writeLeaseKV.Get(ctx, key)
		if err != nil {
			t.Fatalf("re-fetch lease rev: %v", err)
		}
		if postRev != preRev {
			t.Errorf("lease rev = %d, want %d (no mutation expected)", postRev, preRev)
		}
	})

	t.Run("wrong_revision_on_update", func(t *testing.T) {
		// P1-3: passing characterization of buggy current behavior. Bump
		// versionKV revision between obtainLeases and releaseLeases(_, _, true)
		// so versionKV.Update at store.go:319 fails. Per store.go:317-326 the
		// Update error returns BEFORE the writeLeaseKV.Delete at store.go:323
		// — the lease key leaks. Pin the leak so a future production fix
		// flips the assertion.
		h := newHarness(t, "order")

		aggID := uuid.New()
		key := "order." + aggID.String()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		obtained, err := h.store.obtainLeases(ctx, []event.Event{leaseEvt(t, aggID, 1)})
		if err != nil {
			t.Fatalf("obtainLeases: %v", err)
		}

		// Bump the version KV revision externally so the cached rev becomes
		// stale.
		if _, err := h.store.versionKV.Put(ctx, key, &kv.UInt64Value{Value: 99}); err != nil {
			t.Fatalf("bump versionKV: %v", err)
		}

		err = h.store.releaseLeases(ctx, obtained, true)
		if err == nil {
			t.Fatal("releaseLeases err = nil, want wrapped error")
		}
		if !strings.Contains(err.Error(), "unable to update aggregate version") {
			t.Fatalf("err = %q, want substring %q", err.Error(), "unable to update aggregate version")
		}

		// BUG: store.go:317-326 — on versionKV.Update failure (line 319-321)
		// releaseLeases returns before reaching writeLeaseKV.Delete (line 323),
		// leaking the lease. Desired: lease must be released even when version
		// update fails. Asserting current leak to detect drift; flip
		// assertion when bug is fixed.
		if _, _, err := h.store.writeLeaseKV.Get(ctx, key); errors.Is(err, nats.ErrKeyNotFound) {
			t.Fatalf("lease was deleted; current buggy behavior leaks the lease — flip this assertion when store.go:317-326 is fixed")
		}
	})
}
