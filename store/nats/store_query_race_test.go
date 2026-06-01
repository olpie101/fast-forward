package nats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event/query"
)

// TestQueryConcurrentStreamMapperRace confirms risk #3: Store.aggStreamMapper
// and Store.evtStreamMapper are plain maps written without synchronization by
// identifyStreams and processQueryMsg. Insert does not touch either map, so
// after seeding the caches are cold and a concurrent burst of Query calls
// forces overlapping map writes. Goroutines alternate between aggregate-name and
// event-name queries so both identifyStreams discovery writes (aggregate and
// event branches) and the processQueryMsg narrowing write race concurrently.
//
// Run with the race detector:
//
//	go test -race -run TestQueryConcurrentStreamMapperRace ./store/nats/
func TestQueryConcurrentStreamMapperRace(t *testing.T) {
	const eventName = "order.created"
	h := newHarness(t, "order")
	h.registerString(eventName)

	aggID := uuid.New()
	base := time.Now().UTC().Truncate(time.Millisecond)
	insertEventsAt(t, h,
		mkEvent(t, eventName, aggID, 1, base),
		mkEvent(t, eventName, aggID, 2, base.Add(1*time.Millisecond)),
		mkEvent(t, eventName, aggID, 3, base.Add(2*time.Millisecond)),
	)

	const n = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		q := query.New(query.AggregateName("order"))
		if i%2 == 1 {
			q = query.New(query.Name(eventName))
		}
		go func() {
			defer wg.Done()
			<-start // release all goroutines together to widen the overlap window
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			evts, errs, err := h.store.Query(ctx, q)
			if err != nil {
				return
			}
			done := make(chan struct{})
			go func() {
				for range errs {
				}
				close(done)
			}()
			for range evts {
			}
			<-done
		}()
	}
	close(start)
	wg.Wait()
}
