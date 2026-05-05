package nats_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	natsgo "github.com/nats-io/nats.go"
	pnats "github.com/olpie101/fast-forward/projection/nats"
)

var mismatchErr = &natsgo.APIError{ErrorCode: 10071}

// newRetryActionFor builds a sentinel event with a fresh UUID and an action
// closure that asserts every invocation receives that exact event by ID.
// behavior(call) supplies the per-call return error (call is 1-indexed). The
// returned counter pointer is shared with the closure for invocation-count
// assertions.
func newRetryActionFor(
	t *testing.T,
	behavior func(call int) error,
) (event.Of[any], func(got event.Of[any]) error, *int) {
	t.Helper()
	want := event.New[any]("e", struct{}{}, event.ID(uuid.New())).Any()
	calls := 0
	action := func(got event.Of[any]) error {
		calls++
		if got.ID() != want.ID() {
			t.Errorf("event identity drift: got %s want %s", got.ID(), want.ID())
		}
		return behavior(calls)
	}
	return want, action, &calls
}

func TestRetrySuccessFirstTry(t *testing.T) {
	want, action, calls := newRetryActionFor(t, func(int) error { return nil })
	pnats.Retry(action, 5)(want)
	if *calls != 1 {
		t.Errorf("calls: got %d want 1", *calls)
	}
}

func TestRetryMismatchThenSuccess(t *testing.T) {
	want, action, calls := newRetryActionFor(t, func(call int) error {
		if call < 3 {
			return mismatchErr
		}
		return nil
	})
	pnats.Retry(action, 5)(want)
	if *calls != 3 {
		t.Errorf("calls: got %d want 3", *calls)
	}
}

func TestRetryMaxRetriesZero(t *testing.T) {
	want, action, calls := newRetryActionFor(t, func(int) error {
		t.Fatalf("action invoked with maxRetries=0")
		return nil
	})
	pnats.Retry(action, 0)(want)
	if *calls != 0 {
		t.Errorf("calls: got %d want 0", *calls)
	}
}

// BUG: projection/nats/kv_retryable.go:17-23 — non-retryable errors are
// retried instead of returning. The loop only `continue`s on
// isMismatchErr; on any other error it falls through past the err==nil
// check and re-enters the loop, so the action runs maxRetries times.
// Desired: return on the first non-retryable error (optionally
// surfacing it). Asserting current observed behavior to detect drift;
// flip when fixed.
func TestRetryNonRetryableErrorIsRetriedBUG(t *testing.T) {
	want, action, calls := newRetryActionFor(t, func(int) error {
		return errors.New("non-retryable")
	})
	pnats.Retry(action, 4)(want)
	if *calls != 4 {
		t.Errorf("calls: got %d want 4 (current buggy behavior)", *calls)
	}
}

// BUG: projection/nats/kv_retryable.go:14-25 (TODO at line 23) — final
// failure after exhaustion is silently dropped. Desired: surface the
// last error via a callback, sentinel return, or logger. Asserting
// current silent-drop behavior; flip when the contract is decided.
func TestRetrySilentDropOnExhaustionBUG(t *testing.T) {
	want, action, calls := newRetryActionFor(t, func(int) error {
		return mismatchErr
	})
	// Wrapper returns nothing; success here is simply that the call
	// completes (no panic, no deadlock) and the action ran exactly
	// maxRetries times.
	pnats.Retry(action, 3)(want)
	if *calls != 3 {
		t.Errorf("calls: got %d want 3 (current silent-drop behavior)", *calls)
	}
}

// BUG: projection/nats/kv_retryable.go:17-23 — a non-retryable error on
// the first call does not short-circuit even when subsequent calls
// would otherwise be retryable. The loop keeps iterating until
// maxRetries is exhausted.
func TestRetryNonRetryableThenMismatchBUG(t *testing.T) {
	want, action, calls := newRetryActionFor(t, func(call int) error {
		if call == 1 {
			return errors.New("non-retryable")
		}
		return mismatchErr
	})
	pnats.Retry(action, 4)(want)
	if *calls != 4 {
		t.Errorf("calls: got %d want 4 (current buggy behavior)", *calls)
	}
}
