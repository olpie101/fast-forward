package lookup_test

import (
	"sync"
	"testing"
	"time"

	"github.com/modernice/goes/event"
	"github.com/olpie101/fast-forward/projection/lookup"
)

// recorder collects original-handler calls so the test can assert that the
// returned wrapper invokes original synchronously and forwards the event.
type recorder struct {
	mu  sync.Mutex
	got []event.Event
}

func (r *recorder) handle(evt event.Event) {
	r.mu.Lock()
	r.got = append(r.got, evt)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []event.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]event.Event, len(r.got))
	copy(out, r.got)
	return out
}

func newEvent() event.Event {
	return event.New[any]("e", struct{}{}).Any()
}

func TestDebounceReadyOriginalCalledSynchronously(t *testing.T) {
	ready := make(chan struct{})
	rec := &recorder{}
	fn := lookup.DebounceReady(50*time.Millisecond, ready)
	evt := newEvent()

	fn(evt, rec.handle)

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("want 1 recorded event, got %d", len(got))
	}
	if got[0].ID() != evt.ID() {
		t.Errorf("recorded event ID mismatch")
	}
}

func TestDebounceReadyClosesAfterIntervalNoActivity(t *testing.T) {
	ready := make(chan struct{})
	_ = lookup.DebounceReady(100*time.Millisecond, ready)

	select {
	case <-ready:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ready did not close within bound")
	}
}

func TestDebounceReadyResetExtendsClose(t *testing.T) {
	ready := make(chan struct{})
	rec := &recorder{}
	fn := lookup.DebounceReady(100*time.Millisecond, ready)

	// Reset well before the original fire (~30ms in).
	time.Sleep(30 * time.Millisecond)
	fn(newEvent(), rec.handle)

	// ready must NOT close within 50ms of the reset (window = 100ms).
	select {
	case <-ready:
		t.Fatal("ready closed too early after reset")
	case <-time.After(50 * time.Millisecond):
	}

	// Then await close within another 200ms.
	select {
	case <-ready:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ready did not close after reset")
	}
}

func TestDebounceReadyStrictResetSemantics(t *testing.T) {
	// Pinned on Go 1.23: timer channels are synchronous, so Timer.Reset on a
	// not-yet-stopped timer no longer races as the older Go 1.22 docs warned.
	ready := make(chan struct{})
	rec := &recorder{}
	start := time.Now()
	fn := lookup.DebounceReady(100*time.Millisecond, ready)

	// Reset just before fire (~90ms in). After reset, the next fire is
	// ~190ms from start.
	time.Sleep(90 * time.Millisecond)
	fn(newEvent(), rec.handle)

	// ready must NOT close within +60ms after the reset.
	select {
	case <-ready:
		t.Fatalf("ready closed too early; elapsed=%v", time.Since(start))
	case <-time.After(60 * time.Millisecond):
	}

	// Await close within +200ms.
	select {
	case <-ready:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ready did not close after reset window")
	}

	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Errorf("elapsed since start should be ≥ ~150ms, got %v", elapsed)
	}
	// Post-fix Timer (debounce_ready.go:10,:17) leaves no background timer
	// goroutine after <-t.C returns. Not asserted via runtime.NumGoroutine
	// (flaky); structural under Go 1.23.
}
