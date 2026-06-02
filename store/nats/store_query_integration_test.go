package nats

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query"
	qtime "github.com/modernice/goes/event/query/time"
	"github.com/modernice/goes/event/query/version"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// insertEventsAt seeds the harness's stream with the given events, calling
// Insert for each so the version-KV state advances cleanly.
func insertEventsAt(t *testing.T, h *integrationHarness, evts ...event.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, e := range evts {
		if err := h.store.Insert(ctx, e); err != nil {
			t.Fatalf("Insert(%s v=%d): %v", e.Name(), pickVersion(e), err)
		}
	}
}

func pickVersion(e event.Event) int {
	_, _, v := e.Aggregate()
	return v
}

func mkEvent(t *testing.T, name string, aggID uuid.UUID, version int, evtTime time.Time) event.Event {
	t.Helper()
	return event.New[any](
		name,
		"payload",
		event.Aggregate(aggID, "order", version),
		event.Time(evtTime),
	).Any()
}

func TestQueryIntegration(t *testing.T) {
	const eventName = "order.created"

	t.Run("by_aggregate_name", func(t *testing.T) {
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
		// Default sort is by event time ascending.
		for i := 1; i < len(got); i++ {
			if got[i].Time().Before(got[i-1].Time()) {
				t.Fatalf("default sort not ascending by Time: %v then %v", got[i-1].Time(), got[i].Time())
			}
		}
	})

	t.Run("by_aggregate_id", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggA, aggB := uuid.New(), uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggA, 1, base),
			mkEvent(t, eventName, aggB, 1, base.Add(1*time.Millisecond)),
			mkEvent(t, eventName, aggA, 2, base.Add(2*time.Millisecond)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggA),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, evts, errs, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 2 {
			t.Fatalf("len(got) = %d, want 2", len(got))
		}
		for _, e := range got {
			id, _, _ := e.Aggregate()
			if id != aggA {
				t.Errorf("event for aggregate %s, want %s", id, aggA)
			}
		}
	})

	t.Run("by_event_name_normalised", func(t *testing.T) {
		h := newHarness(t, "order")
		// Event name with a "." is normalised to "_" in the subject.
		const dotName = "order.complex.name"
		h.registerString(dotName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, dotName, aggID, 1, base),
		)

		// Stream message subject must use the normalised name.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		str, err := h.js.Stream(ctx, h.streamName)
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		raw, err := str.GetMsg(ctx, 1)
		if err != nil {
			t.Fatalf("GetMsg: %v", err)
		}
		want := fmt.Sprintf("es.order.%s.1.order_complex_name", aggID)
		if raw.Subject != want {
			t.Fatalf("Subject = %q, want %q", raw.Subject, want)
		}

		// Round-trip: query by original (un-normalised) name preserves the
		// original event Name() because the header carries the original.
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.Name(dotName),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, evts, errs, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 1 {
			t.Fatalf("len(got) = %d, want 1", len(got))
		}
		if got[0].Name() != dotName {
			t.Errorf("Name() = %q, want %q", got[0].Name(), dotName)
		}
	})

	t.Run("version_exact", func(t *testing.T) {
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
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.AggregateVersion(version.Exact(2)),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, evts, errs, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 1 {
			t.Fatalf("len(got) = %d, want 1", len(got))
		}
		if _, _, v := got[0].Aggregate(); v != 2 {
			t.Errorf("version = %d, want 2", v)
		}
	})

	t.Run("version_range", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		for i := 1; i <= 5; i++ {
			insertEventsAt(t, h, mkEvent(t, eventName, aggID, i, base.Add(time.Duration(i)*time.Millisecond)))
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.AggregateVersion(version.InRange(version.Range{2, 4})),
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
		for _, e := range got {
			_, _, v := e.Aggregate()
			if v < 2 || v > 4 {
				t.Errorf("version %d outside [2,4]", v)
			}
		}
	})

	t.Run("time_min", func(t *testing.T) {
		// Exercises subscribe's DeliverByStartTimePolicy path. Replaces the old
		// consumer-introspection assertion that walked Stream.ListConsumers.
		// nats.go's iterator Stop does not immediately delete the final ordered
		// consumer; server-side cleanup relies on the consumer's
		// InactiveThreshold, so introspecting consumer lists here is racy.
		//
		// Note: the final {3,4} assertion is also enforced by limitGuard
		// (query_limit_guard.go:57-64 filters by header time post-collect), so
		// this is an end-to-end correctness check rather than proof that
		// JetStream-side DeliverByStartTimePolicy filtering ran. Builder
		// behaviour is pinned by TestOrderedConsumerConfig.
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		t0 := time.Now().UTC().Truncate(time.Millisecond)
		// Insert v1..v2 stamped at t0; sleep beyond pullExpiry so JetStream
		// records distinct stored-publish wall-clocks for the two batches —
		// DeliverByStartTimePolicy filters by stored-publish time, not header
		// time, so the stored-time gap is the substrate of this test.
		insertEventsAt(t, h, mkEvent(t, eventName, aggID, 1, t0))
		insertEventsAt(t, h, mkEvent(t, eventName, aggID, 2, t0))
		time.Sleep(2 * time.Second)
		t1 := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h, mkEvent(t, eventName, aggID, 3, t1))
		insertEventsAt(t, h, mkEvent(t, eventName, aggID, 4, t1))

		cutoff := t1.Add(-500 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, errsCh, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.Time(qtime.Min(cutoff)),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, out, errsCh, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 2 {
			t.Fatalf("len(got) = %d, want 2", len(got))
		}
		for _, e := range got {
			_, _, v := e.Aggregate()
			if v != 3 && v != 4 {
				t.Errorf("version %d outside {3,4}", v)
			}
		}
	})

	t.Run("version_min", func(t *testing.T) {
		// Exercises the query.go fetchVersionAnchorSeq → OptStartSeq path.
		// version.Min(N) is the only constraint that populates
		// AggregateVersions().Min() and reaches guard.hasMinVersion (see
		// query_limit_guard.go:38-46); version.InRange does not — so the
		// version_range subtest above cannot stand in for this one.
		//
		// The sequence anchor is EXACT: fetchVersionAnchorSeq resolves the
		// stream sequence of the v=N-1 event via GetLastMsgForSubject and the
		// read starts there (DeliverByStartSequencePolicy, inclusive), so no
		// stored-publish-time gap workaround is needed. The final {3,4} set is
		// also enforced by the version guard (query_limit_guard.go:38-45);
		// builder behaviour is pinned by TestOrderedConsumerConfig.
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggID, 1, base),
			mkEvent(t, eventName, aggID, 2, base.Add(time.Millisecond)),
			mkEvent(t, eventName, aggID, 3, base.Add(2*time.Millisecond)),
			mkEvent(t, eventName, aggID, 4, base.Add(3*time.Millisecond)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, errsCh, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.AggregateVersion(version.Min(3)),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, out, errsCh, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 2 {
			t.Fatalf("len(got) = %d, want 2", len(got))
		}
		for _, e := range got {
			_, _, v := e.Aggregate()
			if v != 3 && v != 4 {
				t.Errorf("version %d outside {3,4}", v)
			}
		}
	})

	t.Run("version_min_anchor_absent_non_fatal", func(t *testing.T) {
		// When the v=N-1 anchor event does not exist, fetchVersionAnchorSeq
		// returns jetstream.ErrMsgNotFound. The query MUST NOT fail or hang: it
		// falls back to the time/zero anchor and the version guard still filters
		// the result. Here only v1,v2 exist and version.Min(4) excludes them, so
		// the anchor (v=3) is absent and the result is empty — proving the
		// ErrMsgNotFound fallback is non-fatal.
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggID, 1, base),
			mkEvent(t, eventName, aggID, 2, base.Add(time.Millisecond)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, errsCh, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.AggregateVersion(version.Min(4)),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, out, errsCh, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 0 {
			t.Fatalf("len(got) = %d, want 0 (anchor absent; guard filters v1,v2)", len(got))
		}
	})

	t.Run("explicit_sort_desc", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		for i := 1; i <= 3; i++ {
			insertEventsAt(t, h, mkEvent(t, eventName, aggID, i, base.Add(time.Duration(i)*time.Millisecond)))
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.SortBy(event.SortAggregateVersion, event.SortDesc),
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
		for i := 1; i < len(got); i++ {
			_, _, prev := got[i-1].Aggregate()
			_, _, cur := got[i].Aggregate()
			if cur > prev {
				t.Fatalf("sort not descending by version: %d then %d", prev, cur)
			}
		}
	})

	t.Run("no_stream_for_aggregate", func(t *testing.T) {
		// P1-5 fix: query.go:159 returns fmt.Errorf("no stream for aggregate (%s)", a)
		// with no exported sentinel — pin via strings.Contains.
		h := newHarness(t, "order")
		h.registerString(eventName)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, err := h.store.Query(ctx, query.New(query.AggregateName("does_not_exist")))
		if err == nil {
			t.Fatal("expected synchronous error, got nil")
		}
		if !strings.Contains(err.Error(), "no stream for aggregate") {
			t.Fatalf("err = %q, want substring %q", err.Error(), "no stream for aggregate")
		}
	})
}

func TestQuerySyncCollectTODO_Skip(t *testing.T) {
	t.Skip("store/nats/query.go:84 collect is sync — query() blocks on collect before returning channels; desired: collect concurrently so caller can stream events while consumer is still draining. Characterize by inserting a large batch and asserting first event arrives on evts before collect fully completes.")
}

// TestQueryStreamingFastPath covers the SortAggregateVersion/Asc streaming fast
// path and the fallback shapes that must NOT take it. Ordering correctness is
// the testable contract; lower latency is not asserted (see plan). Predicate
// boundaries are pinned separately by TestCanStreamReplay.
func TestQueryStreamingFastPath(t *testing.T) {
	const eventName = "order.created"

	t.Run("fast_path_streams_in_version_order", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		// Insert in version order but with DESCENDING event times. This pins the
		// ordering CONTRACT for the fast-path-eligible shape: the result is
		// version-ascending and NOT default time order. (It does not by itself
		// prove the streaming branch was taken — fallback with an explicit
		// version sort would also yield version order; branch eligibility is
		// pinned by TestCanStreamReplay.)
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggID, 1, base.Add(2*time.Millisecond)),
			mkEvent(t, eventName, aggID, 2, base.Add(1*time.Millisecond)),
			mkEvent(t, eventName, aggID, 3, base),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
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
				t.Fatalf("got[%d] version = %d, want %d (version-ascending stream order)", i, v, i+1)
			}
		}
	})

	t.Run("fast_path_with_version_min_applies_guard", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggID, 1, base),
			mkEvent(t, eventName, aggID, 2, base.Add(1*time.Millisecond)),
			mkEvent(t, eventName, aggID, 3, base.Add(2*time.Millisecond)),
			mkEvent(t, eventName, aggID, 4, base.Add(3*time.Millisecond)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.AggregateVersion(version.Min(3)),
			query.SortBy(event.SortAggregateVersion, event.SortAsc),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, evts, errs, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 2 {
			t.Fatalf("len(got) = %d, want 2", len(got))
		}
		for i, e := range got {
			if _, _, v := e.Aggregate(); v != i+3 {
				t.Fatalf("got[%d] version = %d, want %d", i, v, i+3)
			}
		}
	})

	t.Run("fallback_aggregate_id_and_event_name_no_name", func(t *testing.T) {
		// Subject is es.*.<id>.*.<evt> (wildcard aggregate name) → predicate
		// false → fallback. Result correctness must be preserved.
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h, mkEvent(t, eventName, aggID, 1, base))

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateID(aggID),
			query.Name(eventName),
			query.SortBy(event.SortAggregateVersion, event.SortAsc),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		got, gotErrs := drainQuery(t, evts, errs, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != 1 {
			t.Fatalf("len(got) = %d, want 1", len(got))
		}
	})

	t.Run("fallback_no_sort_uses_time_order", func(t *testing.T) {
		// Single concrete aggregate, no explicit sort, with non-monotonic event
		// times → fallback default SortTime ascending, not version/stream order.
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggID, 1, base.Add(2*time.Millisecond)),
			mkEvent(t, eventName, aggID, 2, base),
			mkEvent(t, eventName, aggID, 3, base.Add(1*time.Millisecond)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
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
		for i := 1; i < len(got); i++ {
			if got[i].Time().Before(got[i-1].Time()) {
				t.Fatalf("fallback not time-ascending: %v then %v", got[i-1].Time(), got[i].Time())
			}
		}
	})

	t.Run("fallback_explicit_sort_time", func(t *testing.T) {
		// Explicit SortTime/Asc → predicate false → fallback time-ascending.
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggID, 1, base.Add(2*time.Millisecond)),
			mkEvent(t, eventName, aggID, 2, base),
			mkEvent(t, eventName, aggID, 3, base.Add(1*time.Millisecond)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.SortBy(event.SortTime, event.SortAsc),
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
		for i := 1; i < len(got); i++ {
			if got[i].Time().Before(got[i-1].Time()) {
				t.Fatalf("fallback not time-ascending: %v then %v", got[i-1].Time(), got[i].Time())
			}
		}
	})

	t.Run("fast_path_async_decode_error_delivered", func(t *testing.T) {
		// Publish a raw message with valid metadata headers but undecodable
		// data to the concrete aggregate subject, then run a fast-path query.
		// processQueryMsg fails on Unmarshal and the error must arrive on the
		// returned error channel (not the synchronous third return value).
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		subject := fmt.Sprintf("es.order.%s.1.%s", aggID, normaliseEventName(eventName))
		header := make(nats.Header)
		header.Set(MetadataKeyEventName, eventName)
		header.Set(MetadataKeyEventTime, time.Now().UTC().Format(time.RFC3339Nano))
		header.Set(jetstream.MsgIDHeader, uuid.New().String())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := h.js.PublishMsg(ctx, &nats.Msg{
			Subject: subject,
			Header:  header,
			Data:    []byte("{not valid json for a string}"),
		}); err != nil {
			t.Fatalf("PublishMsg: %v", err)
		}

		evts, errs, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.SortBy(event.SortAggregateVersion, event.SortAsc),
		))
		if err != nil {
			t.Fatalf("Query returned synchronous error: %v", err)
		}
		got, gotErrs := drainQuery(t, evts, errs, 5*time.Second)
		if len(got) != 0 {
			t.Fatalf("len(got) = %d, want 0 (message is undecodable)", len(got))
		}
		if len(gotErrs) == 0 {
			t.Fatal("expected at least one async decode error, got none")
		}
	})
}

// TestQueryMaterializedParallelDecode covers the bounded parallel-decode pool on
// the materialized read path (#5): correct delivery under many events, the
// multi-error contract (regression for the former cap-1 opErrs deadlock), and
// prompt return under context cancellation.
func TestQueryMaterializedParallelDecode(t *testing.T) {
	const eventName = "order.created"

	t.Run("many_events_all_delivered_ordered", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		// More than queryDecodeWorkers so multiple workers are exercised.
		const n = queryDecodeWorkers * 2
		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		evts := make([]event.Event, 0, n)
		for i := 0; i < n; i++ {
			evts = append(evts, mkEvent(t, eventName, aggID, i+1, base.Add(time.Duration(i)*time.Millisecond)))
		}
		insertEventsAt(t, h, evts...)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Aggregate-name-only (wildcard id) takes the materialized path.
		got, gotErrs := drainQueryFor(t, h, ctx, query.New(query.AggregateName("order")))
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}
		if len(got) != n {
			t.Fatalf("len(got) = %d, want %d", len(got), n)
		}
		// Default sort is event.Time ascending; SortMulti must restore order
		// regardless of decode/emit order from the worker pool.
		for i := 1; i < len(got); i++ {
			if got[i].Time().Before(got[i-1].Time()) {
				t.Fatalf("result not time-ascending at %d: %v then %v", i, got[i-1].Time(), got[i].Time())
			}
		}
	})

	t.Run("multiple_decode_errors_all_delivered_no_deadlock", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Publish >=2 malformed messages so processQueryMsg fails on multiple
		// messages. A cap-1 opErrs channel would deadlock the decode pool here.
		const bad = 3
		for i := 0; i < bad; i++ {
			aggID := uuid.New()
			subject := fmt.Sprintf("es.order.%s.1.%s", aggID, normaliseEventName(eventName))
			header := make(nats.Header)
			header.Set(MetadataKeyEventName, eventName)
			header.Set(MetadataKeyEventTime, time.Now().UTC().Format(time.RFC3339Nano))
			header.Set(jetstream.MsgIDHeader, uuid.New().String())
			if _, err := h.js.PublishMsg(ctx, &nats.Msg{
				Subject: subject,
				Header:  header,
				Data:    []byte("{not valid json for a string}"),
			}); err != nil {
				t.Fatalf("PublishMsg: %v", err)
			}
		}

		got, gotErrs := drainQueryFor(t, h, ctx, query.New(query.AggregateName("order")))
		if len(got) != 0 {
			t.Fatalf("len(got) = %d, want 0 (all messages undecodable)", len(got))
		}
		if len(gotErrs) < 2 {
			t.Fatalf("len(gotErrs) = %d, want >= 2 (multi-error contract)", len(gotErrs))
		}
	})

	t.Run("context_cancellation_returns_promptly", func(t *testing.T) {
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		insertEventsAt(t, h,
			mkEvent(t, eventName, aggID, 1, base),
			mkEvent(t, eventName, aggID, 2, base.Add(1*time.Millisecond)),
		)

		// A cancelled context must make Query unwind promptly without leaking
		// decode workers; drainQuery's deadline bounds the assertion.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		evts, errs, err := h.store.Query(ctx, query.New(query.AggregateName("order")))
		if err != nil {
			// A synchronous cancellation error is acceptable.
			return
		}
		drainQuery(t, evts, errs, 5*time.Second)
	})
}

// drainQueryFor runs a materialized query and drains both channels with a
// deadline, failing the test on a synchronous error.
func drainQueryFor(t *testing.T, h *integrationHarness, ctx context.Context, q event.Query) ([]event.Event, []error) {
	t.Helper()
	evts, errs, err := h.store.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query returned synchronous error: %v", err)
	}
	return drainQuery(t, evts, errs, 5*time.Second)
}
