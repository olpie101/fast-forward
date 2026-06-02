package nats

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// immediateAfter returns an afterFn seam substitute that fires immediately (a
// closed channel) and counts invocations, so the outer-loop retry-count
// assertions run without any real wall-clock sleep.
func immediateAfter(count *int) func(time.Duration) <-chan time.Time {
	return func(time.Duration) <-chan time.Time {
		*count++
		ch := make(chan time.Time)
		close(ch)
		return ch
	}
}

// TestQueryRetryConsolidation exercises the outer (materialized-path) retry
// loop in Store.Query via the queryAttempt and afterFn seams: budget, give-up,
// and error-class branching, with zero real wall-clock sleep. Real
// ErrNoHeartbeat origination is server-timer-driven and is covered as a Skipped
// characterization in query_retry_integration_test.go.
func TestQueryRetryConsolidation(t *testing.T) {
	wrappedNoHeartbeat := fmt.Errorf("err consuming messages (stream: es): %w", jetstream.ErrNoHeartbeat)
	q := query.New(query.AggregateName("order"))

	newStore := func(retryCount uint, afterCalls *int) *Store {
		return &Store{
			logger:     zap.NewNop().Sugar(),
			retryCount: retryCount,
			afterFn:    immediateAfter(afterCalls),
		}
	}

	t.Run("retries min(failures, retryCount-1) times then succeeds", func(t *testing.T) {
		var attempts, afterCalls int
		s := newStore(3, &afterCalls)
		s.queryAttempt = func(context.Context, event.Query, []string) (<-chan event.Event, <-chan error, error) {
			attempts++
			if attempts <= 2 {
				return nil, nil, wrappedNoHeartbeat
			}
			return nil, nil, nil
		}

		if _, _, err := s.Query(context.Background(), q); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if attempts != 3 {
			t.Errorf("queryAttempt calls = %d, want 3", attempts)
		}
		if afterCalls != 2 {
			t.Errorf("afterFn calls = %d, want 2", afterCalls)
		}
	})

	t.Run("gives up after retryCount attempts and returns wrapped ErrNoHeartbeat", func(t *testing.T) {
		var attempts, afterCalls int
		s := newStore(3, &afterCalls)
		s.queryAttempt = func(context.Context, event.Query, []string) (<-chan event.Event, <-chan error, error) {
			attempts++
			return nil, nil, wrappedNoHeartbeat
		}

		_, _, err := s.Query(context.Background(), q)
		if !errors.Is(err, jetstream.ErrNoHeartbeat) {
			t.Fatalf("err = %v, want errors.Is ErrNoHeartbeat", err)
		}
		if attempts != 3 {
			t.Errorf("queryAttempt calls = %d, want 3", attempts)
		}
		if afterCalls != 2 {
			t.Errorf("afterFn calls = %d, want 2", afterCalls)
		}
	})

	t.Run("non-ErrNoHeartbeat error is not retried", func(t *testing.T) {
		errBoom := errors.New("boom")
		var attempts, afterCalls int
		s := newStore(3, &afterCalls)
		s.queryAttempt = func(context.Context, event.Query, []string) (<-chan event.Event, <-chan error, error) {
			attempts++
			return nil, nil, errBoom
		}

		_, _, err := s.Query(context.Background(), q)
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want errBoom", err)
		}
		if attempts != 1 {
			t.Errorf("queryAttempt calls = %d, want 1", attempts)
		}
		if afterCalls != 0 {
			t.Errorf("afterFn calls = %d, want 0 (no backoff on non-retryable error)", afterCalls)
		}
	})
}
