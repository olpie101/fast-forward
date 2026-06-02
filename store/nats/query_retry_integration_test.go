package nats

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query"
)

// TestQueryRetryPathCoverage is a Shape B regression guard: after scoping and
// documenting the two retry sites (outer materialized loop in store.go and
// streamReplay in query.go), BOTH query paths must still deliver correct,
// ordered, error-free results. The documentation/rename made no behavioral
// change; this pins that.
func TestQueryRetryPathCoverage(t *testing.T) {
	const eventName = "order.created"

	t.Run("streaming_fast_path_delivers_ordered", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggID, 1, base),
			mkEvent(t, eventName, aggID, 2, base.Add(1*time.Millisecond)),
			mkEvent(t, eventName, aggID, 3, base.Add(2*time.Millisecond)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Single aggregate + SortAggregateVersion/Asc triggers canStreamReplay.
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.SortBy(event.SortAggregateVersion, event.SortAsc),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, evts, errs, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 3 {
			t.Fatalf("len(got) = %d, want 3", len(got))
		}
		for i, e := range got {
			if _, _, v := e.Aggregate(); v != i+1 {
				t.Fatalf("got[%d] version = %d, want %d", i, v, i+1)
			}
		}
	})

	t.Run("materialized_path_delivers_ordered", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggID, 1, base),
			mkEvent(t, eventName, aggID, 2, base.Add(1*time.Millisecond)),
			mkEvent(t, eventName, aggID, 3, base.Add(2*time.Millisecond)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Aggregate-name-only (wildcard id) fails canStreamReplay, so this takes
		// the materialized path with the default time-ascending sort.
		evts, errs, err := h.store.Query(ctx, query.New(query.AggregateName("order")))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, evts, errs, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 3 {
			t.Fatalf("len(got) = %d, want 3", len(got))
		}
		for i := 1; i < len(got); i++ {
			if got[i].Time().Before(got[i-1].Time()) {
				t.Fatalf("materialized result not time-ascending: %v then %v", got[i-1].Time(), got[i].Time())
			}
		}
	})
}

func TestQueryRetryErrNoHeartbeatInjection_Skip(t *testing.T) {
	t.Skip("Forcing a real jetstream.ErrNoHeartbeat on each live retry path is " +
		"impractical against the embedded harness: heartbeats are NATS " +
		"server-timer-driven and not deterministically reproducible. Origin: " +
		"store/nats/query.go:520 (wrap); materialized retry: store/nats/store.go:187-202; " +
		"streaming retry (pre-emit-only invariant): store/nats/query.go:216-237. " +
		"Retry-orchestration (budget/backoff/error-class) is covered deterministically " +
		"via the queryAttempt/afterFn seams in TestQueryRetryConsolidation.")
}
