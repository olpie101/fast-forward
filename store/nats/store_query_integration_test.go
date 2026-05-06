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
	"github.com/modernice/goes/event/query/version"
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

	t.Run("version_min_uses_fetchVersionMinTime", func(t *testing.T) {
		// P1-1 fix: option (b) consumer introspection. We do NOT assert the
		// returned event count because store/nats/query.go:272-275 configures
		// DeliverByStartTimePolicy + OptStartTime which filters by JetStream
		// stored publication wall-clock, NOT by event header times. Setting
		// only evt.Time() and asserting count is FORBIDDEN. Instead we verify
		// fetchVersionMinTime fired correctly by introspecting the consumer
		// `subscribe` created via js.Stream(...).ListConsumers and asserting
		// info.Config.OptStartTime equals the v=2 message header time.
		h := newHarness(t, "order")
		h.registerString(eventName)

		aggID := uuid.New()
		base := time.Now().UTC().Truncate(time.Millisecond)
		var headerTimeOfV2 time.Time
		evts := make([]event.Event, 0, 5)
		for i := 1; i <= 5; i++ {
			ht := base.Add(time.Duration(i*100) * time.Millisecond)
			if i == 2 {
				headerTimeOfV2 = ht
			}
			evts = append(evts, mkEvent(t, eventName, aggID, i, ht))
		}
		insertEventsAt(t, h, evts...)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Per query.go:61 fetchVersionMinTime is called with
		// (guard.minVersion - 1). Choose Min(3) so the lookup targets v=2.
		out, errsCh, err := h.store.Query(ctx, query.New(
			query.AggregateName("order"),
			query.AggregateID(aggID),
			query.AggregateVersion(version.Min(3)),
		))
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		_, gotErrs := drainQuery(t, out, errsCh, 5*time.Second)
		if len(gotErrs) != 0 {
			t.Fatalf("query errors: %v", gotErrs)
		}

		// Locate the `subscribe`-created consumer. Both fetchVersionMinTime
		// and subscribe build ephemeral consumers; disambiguate by
		// DeliverPolicy == DeliverByStartTimePolicy.
		str, err := h.js.Stream(ctx, h.streamName)
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		var startConsumer *jetstream.ConsumerInfo
		lc := str.ListConsumers(ctx)
		for info := range lc.Info() {
			if info.Config.DeliverPolicy == jetstream.DeliverByStartTimePolicy && info.Config.OptStartTime != nil {
				cp := *info
				startConsumer = &cp
				break
			}
		}
		if err := lc.Err(); err != nil {
			t.Fatalf("ListConsumers: %v", err)
		}
		if startConsumer == nil {
			t.Fatal("no consumer with DeliverByStartTimePolicy found")
		}
		if startConsumer.Config.OptStartTime == nil {
			t.Fatal("OptStartTime is nil")
		}
		if !startConsumer.Config.OptStartTime.Equal(headerTimeOfV2) {
			t.Fatalf("OptStartTime = %v, want %v (v=2 header time)", startConsumer.Config.OptStartTime, headerTimeOfV2)
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

func TestQueryIntegrationMalformedSubjectPanic_Skip(t *testing.T) {
	t.Skip("store/nats/query.go:190-191 fetchVersionMinTime: condition uses && instead of ||; well-formed subjects with parts[3]==\"*\" wrongly pass while malformed (len!=5) subjects panic on parts[3] index; desired: if len(parts) != 5 || parts[3] != \"*\" { return errors.New(...) }. Cannot exercise via Store.Query because subjects are constructed by buildQuery (always 5 parts).")
}

func TestQuerySyncCollectTODO_Skip(t *testing.T) {
	t.Skip("store/nats/query.go:84 collect is sync — query() blocks on collect before returning channels; desired: collect concurrently so caller can stream events while consumer is still draining. Characterize by inserting a large batch and asserting first event arrives on evts before collect fully completes.")
}
