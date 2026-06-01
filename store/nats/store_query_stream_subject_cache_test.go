package nats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event/query"
)

// TestQueryStreamSubjectCache verifies that subscribe populates the
// stream-subject cache on first query and that a repeated query returns the
// same events without error. The cache lets steady-state queries skip the
// per-query Stream() round trip in subscribe.
func TestQueryStreamSubjectCache(t *testing.T) {
	const eventName = "order.created"
	h := newHarness(t, "order")
	h.registerString(eventName)

	aggID := uuid.New()
	base := time.Now().UTC().Truncate(time.Millisecond)
	insertEventsAt(t, h,
		mkEvent(t, eventName, aggID, 1, base),
		mkEvent(t, eventName, aggID, 2, base.Add(1*time.Millisecond)),
	)

	if got := h.cachedStreamSubject(); got != "" {
		t.Fatalf("cache should be empty before first query, got %q", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	evts1, errs1, err := h.store.Query(ctx, query.New(query.AggregateName("order")))
	if err != nil {
		t.Fatalf("first Query: %v", err)
	}
	got1, qErrs1 := drainQuery(t, evts1, errs1, 5*time.Second)
	if len(qErrs1) != 0 {
		t.Fatalf("first query errors: %v", qErrs1)
	}
	if len(got1) != 2 {
		t.Fatalf("first query len(got) = %d, want 2", len(got1))
	}

	if got := h.cachedStreamSubject(); got != "es.order.>" {
		t.Fatalf("cached stream subject = %q, want %q", got, "es.order.>")
	}

	evts2, errs2, err := h.store.Query(ctx, query.New(query.AggregateName("order")))
	if err != nil {
		t.Fatalf("second Query: %v", err)
	}
	got2, qErrs2 := drainQuery(t, evts2, errs2, 5*time.Second)
	if len(qErrs2) != 0 {
		t.Fatalf("second query errors: %v", qErrs2)
	}
	if len(got2) != len(got1) {
		t.Fatalf("second query len(got) = %d, want %d", len(got2), len(got1))
	}
	for i := range got1 {
		_, _, v1 := got1[i].Aggregate()
		_, _, v2 := got2[i].Aggregate()
		if got2[i].ID() != got1[i].ID() || v1 != v2 {
			t.Fatalf("event[%d] mismatch: first=(id=%s,v=%d) second=(id=%s,v=%d)",
				i, got1[i].ID(), v1, got2[i].ID(), v2)
		}
	}
}

// TestQueryStreamSubjectCacheConcurrentRace cold-starts the stream-subject
// cache and fires concurrent queries so multiple subscribe calls race to
// populate streamSubjectMapper. Run with -race to confirm the cache's
// RWMutex/get-or-load discipline holds.
//
//	go test -race -run TestQueryStreamSubjectCacheConcurrentRace ./store/nats/
func TestQueryStreamSubjectCacheConcurrentRace(t *testing.T) {
	const eventName = "order.created"
	h := newHarness(t, "order")
	h.registerString(eventName)

	aggID := uuid.New()
	base := time.Now().UTC().Truncate(time.Millisecond)
	insertEventsAt(t, h,
		mkEvent(t, eventName, aggID, 1, base),
		mkEvent(t, eventName, aggID, 2, base.Add(1*time.Millisecond)),
	)

	if got := h.cachedStreamSubject(); got != "" {
		t.Fatalf("cache should be empty before first query, got %q", got)
	}

	const n = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start // release all goroutines together to widen the overlap window
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			evts, errs, err := h.store.Query(ctx, query.New(query.AggregateName("order")))
			if err != nil {
				t.Errorf("Query: %v", err)
				return
			}
			got, qErrs := drainQuery(t, evts, errs, 5*time.Second)
			if len(qErrs) != 0 {
				t.Errorf("query errors: %v", qErrs)
			}
			if len(got) != 2 {
				t.Errorf("len(got) = %d, want 2", len(got))
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := h.cachedStreamSubject(); got != "es.order.>" {
		t.Fatalf("cached stream subject = %q, want %q", got, "es.order.>")
	}
}
