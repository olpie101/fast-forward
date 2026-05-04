package kv

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestUnwrapValues(t *testing.T) {
	t.Run("forwards initial value then resolved zero marker", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctxSetup := context.Background()
		if _, err := kv.Create(ctxSetup, "k1", &UInt64Value{Value: 1}); err != nil {
			t.Fatalf("Create: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		in, errs, err := kv.WatchAll(ctx)
		if err != nil {
			t.Fatalf("WatchAll: %v", err)
		}

		out, outErrs, err := UnwrapValues[*UInt64Value](ctx, in, errs)
		if err != nil {
			t.Fatalf("UnwrapValues: %v", err)
		}
		_ = drainAnyErrs(t, outErrs)

		v1 := recvOne(t, out, 2*time.Second)
		if v1 == nil || v1.Value != 1 {
			t.Fatalf("first = %#v, want &UInt64Value{1}", v1)
		}
		marker := recvOne(t, out, 2*time.Second)
		if marker == nil || marker.Value != 0 {
			t.Fatalf("marker = %#v, want &UInt64Value{0}", marker)
		}
	})

	t.Run("WithStopOnZero exits after marker", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctxSetup := context.Background()
		if _, err := kv.Create(ctxSetup, "k1", &UInt64Value{Value: 1}); err != nil {
			t.Fatalf("Create: %v", err)
		}

		ctx, cancel := context.WithCancel(WithStopOnZero(context.Background()))
		t.Cleanup(cancel)
		in, errs, err := kv.WatchAll(ctx)
		if err != nil {
			t.Fatalf("WatchAll: %v", err)
		}
		out, outErrs, err := UnwrapValues[*UInt64Value](ctx, in, errs)
		if err != nil {
			t.Fatalf("UnwrapValues: %v", err)
		}
		_ = drainAnyErrs(t, outErrs)

		v1 := recvOne(t, out, 2*time.Second)
		if v1 == nil || v1.Value != 1 {
			t.Fatalf("first = %#v, want &UInt64Value{1}", v1)
		}
		// After the marker triggers stop-on-zero, the goroutine exits; out
		// closes via deferred valClose(). Subsequent Puts must NOT arrive.
		if _, err := kv.Put(ctxSetup, "k1", &UInt64Value{Value: 9}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		// out should close (or at minimum not deliver the new value) within 500ms.
		deadline := time.After(500 * time.Millisecond)
		for {
			select {
			case v, ok := <-out:
				if !ok {
					return // channel closed — pass
				}
				if v != nil && v.Value == 9 {
					t.Fatalf("received Put after stop-on-zero: %#v", v)
				}
				// Could be the marker draining through; keep looping until
				// timeout or close.
			case <-deadline:
				return // never received the post-marker Put — pass
			}
		}
	})

	t.Run("without WithStopOnZero, Puts after marker are forwarded", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctxSetup := context.Background()
		if _, err := kv.Create(ctxSetup, "k1", &UInt64Value{Value: 1}); err != nil {
			t.Fatalf("Create: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		in, errs, err := kv.WatchAll(ctx)
		if err != nil {
			t.Fatalf("WatchAll: %v", err)
		}
		out, outErrs, err := UnwrapValues[*UInt64Value](ctx, in, errs)
		if err != nil {
			t.Fatalf("UnwrapValues: %v", err)
		}
		_ = drainAnyErrs(t, outErrs)

		_ = recvOne(t, out, 2*time.Second) // initial value
		_ = recvOne(t, out, 2*time.Second) // marker

		if _, err := kv.Put(ctxSetup, "k1", &UInt64Value{Value: 9}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		v := recvOne(t, out, 2*time.Second)
		if v == nil || v.Value != 9 {
			t.Fatalf("post-marker Put = %#v, want &UInt64Value{9}", v)
		}
	})

	t.Run("forwards unmarshal error through merged err channel", func(t *testing.T) {
		kv, bucket, _ := newKV[*UInt64Value](t)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		in, errs, err := kv.Watch(ctx, "k")
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		out, outErrs, err := UnwrapValues[*UInt64Value](ctx, in, errs)
		if err != nil {
			t.Fatalf("UnwrapValues: %v", err)
		}
		getErrs := drainAnyErrs(t, outErrs)
		// Drain marker.
		_ = recvOne(t, out, 2*time.Second)

		// Inject invalid JSON via the underlying KV.
		if _, err := bucket.PutString("k", "not-json"); err != nil {
			t.Fatalf("PutString: %v", err)
		}

		deadline := time.After(2 * time.Second)
		for {
			if len(getErrs()) > 0 {
				return
			}
			select {
			case <-deadline:
				t.Fatal("unmarshal error not forwarded within 2s")
			case <-time.After(20 * time.Millisecond):
			}
		}
	})

	t.Run("cancel closes both output channels", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctx, cancel := context.WithCancel(context.Background())

		in, errs, err := kv.Watch(ctx, "k")
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		out, outErrs, err := UnwrapValues[*UInt64Value](ctx, in, errs)
		if err != nil {
			t.Fatalf("UnwrapValues: %v", err)
		}

		// Drain marker so the goroutine has run at least one iteration.
		_ = recvOne(t, out, 2*time.Second)

		cancel()

		// Both out and outErrs must close within 500ms.
		valsClosed := waitClosed(t, out, 500*time.Millisecond)
		errsClosed := waitClosed(t, outErrs, 500*time.Millisecond)
		if !valsClosed {
			t.Error("out did not close within 500ms after cancel")
		}
		if !errsClosed {
			t.Error("outErrs did not close within 500ms after cancel")
		}
	})
}

// drainAnyErrs is the typed-error-channel sibling of drainErrs.
func drainAnyErrs(t *testing.T, errs <-chan error) func() []error {
	t.Helper()
	var (
		mu  sync.Mutex
		got []error
	)
	go func() {
		for e := range errs {
			mu.Lock()
			got = append(got, e)
			mu.Unlock()
		}
	}()
	return func() []error {
		mu.Lock()
		defer mu.Unlock()
		out := make([]error, len(got))
		copy(out, got)
		return out
	}
}

func recvOne[T any](t *testing.T, ch <-chan T, d time.Duration) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		var zero T
		t.Fatalf("timed out waiting for value after %v", d)
		return zero
	}
}

// waitClosed reports whether ch closes within d.
func waitClosed[T any](t *testing.T, ch <-chan T, d time.Duration) bool {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return true
			}
			// Drain any residual values.
		case <-deadline:
			return false
		}
	}
}
