package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// drainErrs starts a goroutine that pulls from errs into a thread-safe slice,
// returning a closure to read collected errors. Tests MUST call this when
// invoking Watch/WatchAll because errs is unbuffered (kv/nats.go:200) and
// senders at kv/nats.go:207 and kv/nats.go:232 will deadlock the watcher
// goroutine if no reader is present.
func drainErrs(t *testing.T, errs <-chan error) func() []error {
	t.Helper()
	var (
		mu   sync.Mutex
		got  []error
		done = make(chan struct{})
	)
	go func() {
		defer close(done)
		for e := range errs {
			mu.Lock()
			got = append(got, e)
			mu.Unlock()
		}
	}()
	// Note: errs is never closed by the production code (kv/nats.go:198-244).
	// The drain goroutine therefore lives until test exit — bounded by
	// TestMain shutdown, which closes the underlying connection and unblocks
	// any pending sends.
	_ = done
	return func() []error {
		mu.Lock()
		defer mu.Unlock()
		out := make([]error, len(got))
		copy(out, got)
		return out
	}
}

// recvVal pulls one WatchValue with a timeout.
func recvVal[T MarshalerUnmarshaler](t *testing.T, vals <-chan WatchValue[T], d time.Duration) WatchValue[T] {
	t.Helper()
	select {
	case v := <-vals:
		return v
	case <-time.After(d):
		t.Fatalf("timed out waiting for WatchValue after %v", d)
		return WatchValue[T]{}
	}
}

func TestWatchAll(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctxSetup := context.Background()
	if _, err := kv.Create(ctxSetup, "k1", &UInt64Value{Value: 1}); err != nil {
		t.Fatalf("Create k1: %v", err)
	}
	if _, err := kv.Create(ctxSetup, "k2", &UInt64Value{Value: 2}); err != nil {
		t.Fatalf("Create k2: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	vals, errs, err := kv.WatchAll(ctx)
	if err != nil {
		t.Fatalf("WatchAll: %v", err)
	}
	getErrs := drainErrs(t, errs)

	// Initial values: two Puts (k1, k2) followed by terminator (Key="").
	gotInitial := map[string]uint64{}
	var sawTerminator bool
	for i := 0; i < 3; i++ {
		v := recvVal(t, vals, 2*time.Second)
		if v.Key == "" {
			sawTerminator = true
			if v.Value == nil || v.Value.Value != 0 {
				t.Fatalf("terminator Value = %#v, want resolved zero", v.Value)
			}
			break
		}
		if v.Op != nats.KeyValuePut {
			t.Fatalf("initial entry op = %v, want KeyValuePut", v.Op)
		}
		gotInitial[v.Key] = v.Value.Value
	}
	if !sawTerminator {
		t.Fatal("did not observe end-of-initial-values terminator")
	}
	if gotInitial["k1"] != 1 || gotInitial["k2"] != 2 {
		t.Fatalf("initial values = %v, want {k1:1, k2:2}", gotInitial)
	}

	if errs := getErrs(); len(errs) > 0 {
		t.Fatalf("unexpected errs during initial: %v", errs)
	}
}

func TestWatch_FilterAndPut(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctxSetup := context.Background()
	if _, err := kv.Create(ctxSetup, "k1", &UInt64Value{Value: 1}); err != nil {
		t.Fatalf("Create k1: %v", err)
	}
	if _, err := kv.Create(ctxSetup, "k2", &UInt64Value{Value: 2}); err != nil {
		t.Fatalf("Create k2: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	vals, errs, err := kv.Watch(ctx, "k1")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	_ = drainErrs(t, errs)

	// Initial: k1 Put, then terminator. k2 must NOT appear.
	first := recvVal(t, vals, 2*time.Second)
	if first.Key != "k1" || first.Op != nats.KeyValuePut || first.Value.Value != 1 {
		t.Fatalf("first = %+v, want k1 Put 1", first)
	}
	terminator := recvVal(t, vals, 2*time.Second)
	if terminator.Key != "" {
		t.Fatalf("expected terminator, got %+v", terminator)
	}

	// Mutate k2 — should not surface within 500ms.
	if _, err := kv.Put(ctxSetup, "k2", &UInt64Value{Value: 99}); err != nil {
		t.Fatalf("Put k2: %v", err)
	}
	select {
	case v := <-vals:
		t.Fatalf("unexpected value for filter k1: %+v", v)
	case <-time.After(500 * time.Millisecond):
	}

	// Now mutate k1 — should surface.
	if _, err := kv.Put(ctxSetup, "k1", &UInt64Value{Value: 9}); err != nil {
		t.Fatalf("Put k1: %v", err)
	}
	upd := recvVal(t, vals, 2*time.Second)
	if upd.Key != "k1" || upd.Op != nats.KeyValuePut || upd.Value.Value != 9 {
		t.Fatalf("update = %+v, want k1 Put 9", upd)
	}
}

func TestWatch_DeleteEmitsZeroValue(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctxSetup := context.Background()
	if _, err := kv.Create(ctxSetup, "k1", &UInt64Value{Value: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	vals, errs, err := kv.Watch(ctx, "k1")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	_ = drainErrs(t, errs)

	// Drain initial Put + terminator.
	_ = recvVal(t, vals, 2*time.Second)
	_ = recvVal(t, vals, 2*time.Second)

	// Delete (no purge).
	if err := kv.Delete(ctxSetup, "k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	v := recvVal(t, vals, 2*time.Second)
	if v.Op != nats.KeyValueDelete {
		t.Fatalf("op = %v, want KeyValueDelete", v.Op)
	}
	if v.Key != "k1" {
		t.Fatalf("key = %q, want k1", v.Key)
	}
	// Per kv/nats.go:222-228, Value is left at the zero of T (typed-nil pointer).
	if v.Value != nil {
		t.Fatalf("Value = %#v, want nil (typed-zero of *UInt64Value)", v.Value)
	}
}

func TestWatch_PurgeEmitsZeroValue(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctxSetup := context.Background()
	if _, err := kv.Create(ctxSetup, "k1", &UInt64Value{Value: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	vals, errs, err := kv.Watch(ctx, "k1")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	_ = drainErrs(t, errs)

	_ = recvVal(t, vals, 2*time.Second)
	_ = recvVal(t, vals, 2*time.Second)

	if err := kv.Delete(WithPurge(ctxSetup), "k1"); err != nil {
		t.Fatalf("Delete(WithPurge): %v", err)
	}
	v := recvVal(t, vals, 2*time.Second)
	if v.Op != nats.KeyValuePurge {
		t.Fatalf("op = %v, want KeyValuePurge", v.Op)
	}
	if v.Key != "k1" {
		t.Fatalf("key = %q, want k1", v.Key)
	}
	if v.Value != nil {
		t.Fatalf("Value = %#v, want nil (typed-zero of *UInt64Value)", v.Value)
	}
}

func TestWatch_InvalidSubject(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// "  " (whitespace) is rejected by NATS subject validation.
	vals, errs, err := kv.Watch(ctx, "  ")
	if err == nil {
		t.Fatal("Watch err = nil, want non-nil")
	}
	if vals != nil {
		t.Fatalf("vals = %v, want nil", vals)
	}
	if errs != nil {
		t.Fatalf("errs = %v, want nil", errs)
	}
	// No timing claim — receiving from a nil channel blocks forever.
}

func TestWatch_UnmarshalError(t *testing.T) {
	kv, bucket, _ := newKV[*UInt64Value](t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	vals, errs, err := kv.Watch(ctx, "k")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	getErrs := drainErrs(t, errs)

	// Drain terminator (no initial values; bucket empty at watch time).
	_ = recvVal(t, vals, 2*time.Second)

	// Inject invalid JSON via the underlying KV — bypasses our typed wrapper.
	if _, err := bucket.PutString("k", "not-json"); err != nil {
		t.Fatalf("PutString: %v", err)
	}

	// Wait for the unmarshal error to be drained.
	deadline := time.After(2 * time.Second)
	for {
		if len(getErrs()) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("unmarshal error not observed within 2s")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Subsequent valid Put MUST be delivered (proves watcher goroutine survived).
	if _, err := kv.Put(ctx, "k", &UInt64Value{Value: 5}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v := recvVal(t, vals, 2*time.Second)
	if v.Op != nats.KeyValuePut || v.Key != "k" || v.Value == nil || v.Value.Value != 5 {
		t.Fatalf("post-error update = %+v, want k Put 5", v)
	}
}

func TestWatch_CancelStopsStream(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctx, cancel := context.WithCancel(context.Background())
	vals, errs, err := kv.Watch(ctx, "k")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	_ = drainErrs(t, errs)
	// Drain terminator.
	_ = recvVal(t, vals, 2*time.Second)

	cancel()
	// Give the watcher goroutine time to observe ctx.Done() and call Stop().
	time.Sleep(50 * time.Millisecond)

	// Mutate after cancel — must NOT be delivered within 500ms.
	if _, err := kv.Put(context.Background(), "k", &UInt64Value{Value: 7}); err != nil {
		t.Fatalf("Put after cancel: %v", err)
	}
	select {
	case v := <-vals:
		t.Fatalf("unexpected value after cancel: %+v", v)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestWatch_NoCloseOnCancel_Skipped characterizes that watch() never closes
// out/errs (kv/nats.go:198-244). Range-loop consumers would hang. Desired:
// close both channels on ctx.Done() and Stop() error paths so consumers can
// `range` safely. Tests in this block use bounded `select` and never `range`.
func TestWatch_NoCloseOnCancel_Skipped(t *testing.T) {
	t.Skip("known: watch() never closes out/errs on ctx cancel; see kv/nats.go:198-244.")
}
