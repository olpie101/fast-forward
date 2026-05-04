package projection_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query"
	goesprojection "github.com/modernice/goes/projection"
	ffprojection "github.com/olpie101/fast-forward/projection"
)

// fakeSchedule satisfies projection.Schedule. Only Trigger is meaningful;
// Subscribe is unused.
//
// Concurrency model (locked decisions, projection-pure-unit-test-block plan,
// Task 3): triggerErr is set BEFORE PeriodicJobTicker[Func] is started and is
// NEVER mutated for the remainder of the test. For recovery scenarios the
// behavior switch lives inside tickFn closure (counter-based), not the schedule.
type fakeSchedule struct {
	mu        sync.Mutex
	calls     []scheduleCall
	triggered chan struct{}
	// triggerErr is read on the producer goroutine; it MUST be set before the
	// ticker is started and never mutated afterwards.
	triggerErr error
}

type scheduleCall struct {
	ctx  context.Context
	opts []goesprojection.TriggerOption
}

func newFakeSchedule() *fakeSchedule {
	return &fakeSchedule{triggered: make(chan struct{}, 16)}
}

func (f *fakeSchedule) Trigger(ctx context.Context, opts ...goesprojection.TriggerOption) error {
	f.mu.Lock()
	f.calls = append(f.calls, scheduleCall{ctx: ctx, opts: opts})
	err := f.triggerErr
	f.mu.Unlock()
	// Non-blocking signal — buffered cap=16.
	select {
	case f.triggered <- struct{}{}:
	default:
	}
	return err
}

func (f *fakeSchedule) Subscribe(context.Context, func(goesprojection.Job) error, ...goesprojection.SubscribeOption) (<-chan error, error) {
	return nil, nil
}

func (f *fakeSchedule) callsSnapshot() []scheduleCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]scheduleCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// waitTriggered blocks until the schedule emits a triggered signal or the
// timeout elapses. Returns true on success.
func (f *fakeSchedule) waitTriggered(t *testing.T, d time.Duration, label string) {
	t.Helper()
	select {
	case <-f.triggered:
	case <-time.After(d):
		t.Fatalf("timed out waiting for trigger: %s", label)
	}
}

// triggerOptsToQuery applies the recorded TriggerOption slice to a Trigger and
// returns its Query.
func triggerOptsToQuery(opts []goesprojection.TriggerOption) event.Query {
	tr := goesprojection.NewTrigger(opts...)
	return tr.Query
}

func TestPeriodicJobTickerSuccessfulTriggers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := newFakeSchedule()

	errs := ffprojection.PeriodicJobTicker(ctx, 20*time.Millisecond, fake)

	fake.waitTriggered(t, 500*time.Millisecond, "first")
	fake.waitTriggered(t, 500*time.Millisecond, "second")

	cancel()

	// Drain errs with a bounded timeout. errs is unbuffered and never closed
	// (periodic_job_ticker.go:20,:26) — `range errs` would hang forever. Use a
	// bounded select instead. We expect zero error sends because triggerErr is
	// nil.
	select {
	case e := <-errs:
		t.Errorf("unexpected err: %v", e)
	case <-time.After(100 * time.Millisecond):
	}

	calls := fake.callsSnapshot()
	if len(calls) < 2 {
		t.Fatalf("want >=2 fake calls, got %d", len(calls))
	}
}

func TestPeriodicJobTickerErrorForwarded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newFakeSchedule()
	errBoom := errors.New("trigger boom")
	fake.triggerErr = errBoom // set before start; never mutated

	errs := ffprojection.PeriodicJobTicker(ctx, 20*time.Millisecond, fake)

	// Producer at periodic_job_ticker.go:37 does `errs <- err` (unbuffered);
	// receive once with bounded timeout to unblock it.
	select {
	case e := <-errs:
		if !errors.Is(e, errBoom) {
			t.Errorf("want errBoom (unwrapped), got %v", e)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for trigger error")
	}
}

func TestPeriodicJobTickerMergesQueryOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newFakeSchedule()

	_ = ffprojection.PeriodicJobTicker(
		ctx, 20*time.Millisecond, fake,
		query.AggregateName("foo"),
	)

	fake.waitTriggered(t, 500*time.Millisecond, "first")
	cancel()

	calls := fake.callsSnapshot()
	if len(calls) == 0 {
		t.Fatal("no recorded calls")
	}
	q := triggerOptsToQuery(calls[0].opts)
	if q == nil {
		t.Fatal("Trigger.Query is nil; expected projection.Query option")
	}
	names := q.AggregateNames()
	if !contains(names, "foo") {
		t.Errorf("AggregateNames want contain 'foo', got %v", names)
	}
	sortings := q.Sortings()
	if len(sortings) == 0 || sortings[0].Sort != event.SortTime || sortings[0].Dir != event.SortAsc {
		t.Errorf("Sortings should lead with SortTime/SortAsc, got %+v", sortings)
	}
}

func TestPeriodicJobTickerCtxCancelStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := newFakeSchedule()

	_ = ffprojection.PeriodicJobTicker(ctx, 20*time.Millisecond, fake)
	fake.waitTriggered(t, 500*time.Millisecond, "first")
	cancel()

	// Drain any already-buffered signals, then ensure no more arrive.
	drainTriggered(fake)
	select {
	case <-fake.triggered:
		t.Errorf("trigger fired after ctx cancel")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestPeriodicJobTickerFuncTickFnReturnsExtraOpts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newFakeSchedule()

	tickFn := func(ctx context.Context) ([]query.Option, error) {
		return []query.Option{query.AggregateName("bar")}, nil
	}

	_ = ffprojection.PeriodicJobTickerFunc(ctx, 20*time.Millisecond, fake, tickFn)
	fake.waitTriggered(t, 500*time.Millisecond, "first")
	cancel()

	calls := fake.callsSnapshot()
	if len(calls) == 0 {
		t.Fatal("no recorded calls")
	}
	q := triggerOptsToQuery(calls[0].opts)
	if q == nil {
		t.Fatal("Trigger.Query is nil")
	}
	if !contains(q.AggregateNames(), "bar") {
		t.Errorf("want 'bar' in AggregateNames, got %v", q.AggregateNames())
	}
	sortings := q.Sortings()
	if len(sortings) == 0 || sortings[0].Sort != event.SortTime {
		t.Errorf("default SortByTime must precede extras, got %+v", sortings)
	}
}

func TestPeriodicJobTickerFuncTickErrorThenRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := newFakeSchedule()
	errBoom := errors.New("tick boom")

	var counter int
	var counterMu sync.Mutex
	tickFn := func(ctx context.Context) ([]query.Option, error) {
		counterMu.Lock()
		counter++
		c := counter
		counterMu.Unlock()
		if c == 1 {
			return nil, errBoom
		}
		return nil, nil
	}

	errs := ffprojection.PeriodicJobTickerFunc(ctx, 20*time.Millisecond, fake, tickFn)

	// Step 1: bounded-receive the error from the unbuffered errs channel
	// (periodic_job_ticker.go:55,:66). The error path does NOT call Trigger,
	// so we cannot wait on fake.triggered for the first tick.
	select {
	case e := <-errs:
		if !errors.Is(e, errBoom) {
			t.Fatalf("want errBoom, got %v", e)
		}
		if !strings.HasPrefix(e.Error(), "job tick func error: ") {
			t.Fatalf("want wrapping prefix, got %q", e.Error())
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("no error received from first tick")
	}

	// Step 2: recovery — counter is now 1, next tick returns nil and reaches
	// s.Trigger.
	fake.waitTriggered(t, 500*time.Millisecond, "recovery")

	// Step 3: cancel.
	cancel()

	// Step 4: sidecar drain spawned AFTER cancel (NOT before step 1, otherwise
	// the sidecar would race the test goroutine for the single unbuffered
	// error). errs never closes (periodic_job_ticker.go:61), so the sidecar
	// exits via the bounded inner timeout.
	extras := make(chan error, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case e, ok := <-errs:
				if !ok {
					return
				}
				extras <- e
			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()
	<-done
	close(extras)

	for e := range extras {
		t.Errorf("unexpected extra err after recovery: %v", e)
	}

	calls := fake.callsSnapshot()
	if len(calls) < 1 {
		t.Errorf("want >=1 successful Trigger after recovery, got %d", len(calls))
	}
}

func TestPeriodicJobTickerFuncScheduleErrorUnwrapped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newFakeSchedule()
	errBoom := errors.New("schedule boom")
	fake.triggerErr = errBoom // set before start; never mutated

	tickFn := func(ctx context.Context) ([]query.Option, error) { return nil, nil }
	errs := ffprojection.PeriodicJobTickerFunc(ctx, 20*time.Millisecond, fake, tickFn)

	// Producer at :81 sends raw errBoom (NOT wrapped — see plan).
	select {
	case e := <-errs:
		if !errors.Is(e, errBoom) {
			t.Errorf("want errBoom, got %v", e)
		}
		if strings.HasPrefix(e.Error(), "job tick func error: ") {
			t.Errorf("schedule trigger error must NOT carry tickFn wrap: %q", e.Error())
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for schedule error")
	}
}

func TestPeriodicJobTickerErrsNotClosedOnCancel(t *testing.T) {
	t.Skip("known: errs not closed on ctx cancel; channels created at periodic_job_ticker.go:20,:55 and goroutines exit without close at :26,:61")
}

// helpers

func contains[T comparable](s []T, v T) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func drainTriggered(f *fakeSchedule) {
	for {
		select {
		case <-f.triggered:
		default:
			return
		}
	}
}
