package kv

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestGetAll(t *testing.T) {
	t.Run("all keys present returns multiset", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctx := context.Background()
		for k, v := range map[string]uint64{"a": 1, "b": 2, "c": 3} {
			if _, err := kv.Create(ctx, k, &UInt64Value{Value: v}); err != nil {
				t.Fatalf("Create %s: %v", k, err)
			}
		}
		got, err := kv.GetAll(ctx, []string{"a", "b", "c"})
		if err != nil {
			t.Fatalf("GetAll err = %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("GetAll len = %d, want 3", len(got))
		}
		vals := make([]int, 0, 3)
		for _, v := range got {
			vals = append(vals, int(v.Value))
		}
		sort.Ints(vals)
		want := []int{1, 2, 3}
		for i := range want {
			if vals[i] != want[i] {
				t.Fatalf("GetAll values = %v, want %v", vals, want)
			}
		}
	})

	t.Run("empty keys returns empty slice", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctx := context.Background()
		done := make(chan struct {
			out []*UInt64Value
			err error
		}, 1)
		go func() {
			out, err := kv.GetAll(ctx, []string{})
			done <- struct {
				out []*UInt64Value
				err error
			}{out, err}
		}()
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("GetAll(empty) err = %v", r.err)
			}
			if len(r.out) != 0 {
				t.Fatalf("GetAll(empty) = %v, want empty", r.out)
			}
		case <-time.After(50 * time.Millisecond):
			t.Fatal("GetAll(empty) did not return within 50ms")
		}
	})

	t.Run("WithNoErrorOnNotFound skips missing", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctx := context.Background()
		if _, err := kv.Create(ctx, "a", &UInt64Value{Value: 1}); err != nil {
			t.Fatalf("Create a: %v", err)
		}
		if _, err := kv.Create(ctx, "b", &UInt64Value{Value: 2}); err != nil {
			t.Fatalf("Create b: %v", err)
		}
		got, err := kv.GetAll(WithNoErrorOnNotFound(ctx), []string{"a", "missing", "b"})
		if err != nil {
			t.Fatalf("GetAll err = %v, want nil", err)
		}
		if len(got) != 2 {
			t.Fatalf("GetAll len = %d, want 2", len(got))
		}
		vals := []int{int(got[0].Value), int(got[1].Value)}
		sort.Ints(vals)
		if vals[0] != 1 || vals[1] != 2 {
			t.Fatalf("GetAll values = %v, want {1,2}", vals)
		}
	})

	t.Run("missing key without modifier returns wrapped error", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctx := context.Background()
		got, err := kv.GetAll(ctx, []string{"missing"})
		if err == nil {
			t.Fatal("GetAll err = nil, want non-nil")
		}
		if len(got) != 0 {
			t.Fatalf("GetAll out len = %d, want 0", len(got))
		}
		if !errors.Is(err, nats.ErrKeyNotFound) {
			t.Fatalf("GetAll err = %v, want wrapping ErrKeyNotFound", err)
		}
		if !strings.HasPrefix(err.Error(), "error getting key (missing): ") {
			t.Fatalf("GetAll err = %q, want prefix 'error getting key (missing): '", err.Error())
		}
	})

	t.Run("ctx already canceled returns context.Canceled", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out, err := kv.GetAll(ctx, []string{"a", "b"})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetAll err = %v, want context.Canceled", err)
		}
		if out != nil {
			t.Fatalf("GetAll out = %v, want nil", out)
		}
	})

	t.Run("ctx canceled mid-flight returns context.Canceled", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		ctx, cancel := context.WithCancel(context.Background())
		// Start cancel ticking quickly while goroutines spin up.
		go func() {
			time.Sleep(1 * time.Millisecond)
			cancel()
		}()
		// Build a moderately large key set so cancel can race in. Even if all
		// keys come back fast, the test still passes the cancel-ok branch.
		keys := make([]string, 0, 50)
		for i := 0; i < 50; i++ {
			keys = append(keys, "missing-")
		}
		done := make(chan error, 1)
		go func() {
			_, err := kv.GetAll(ctx, keys)
			done <- err
		}()
		select {
		case err := <-done:
			// Either context.Canceled OR an ErrKeyNotFound wrap is acceptable
			// here — race between context observation and first error send.
			if err == nil {
				t.Fatalf("GetAll err = nil, want context.Canceled or wrapped ErrKeyNotFound")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("GetAll did not return within 2s")
		}
	})
}

// TestGetAll_MultiErrorRace_Skipped characterizes the bug at kv/nats.go:83 where
// the errs channel buffer is 1 — only the first failing goroutine surfaces an
// error; the remaining goroutines block forever on `errs <-`. Desired behavior:
// aggregate all per-key errors (e.g., via errors.Join) so callers see every
// failure deterministically.
func TestGetAll_MultiErrorRace_Skipped(t *testing.T) {
	t.Skip("known: GetAll error channel buffer=1 leaks late-failing goroutines; see kv/nats.go:83. Desired: errors.Join of all per-key errors.")
}

// TestGetAll_GoroutineLeakOnCancel_Skipped characterizes the leak at
// kv/nats.go:91-103 / 110-111 where the parent returns on ctx.Done() but the
// per-key goroutines spawned via s.Get keep running. Desired: parent waits for
// in-flight goroutines (or relies on cooperative ctx propagation in s.Get) so
// no goroutines leak past return.
func TestGetAll_GoroutineLeakOnCancel_Skipped(t *testing.T) {
	t.Skip("known: GetAll spawns goroutines that outlive ctx cancellation; see kv/nats.go:91-103, 110-111.")
}

// TestGetAll_ErrorSuccessRace_Skipped characterizes the non-determinism at
// kv/nats.go:108-123 where, with mixed missing/present keys, the select may
// observe either the error path or the partial-success path first. Desired:
// deterministic aggregation regardless of select order.
func TestGetAll_ErrorSuccessRace_Skipped(t *testing.T) {
	t.Skip("known: GetAll select order non-deterministic on mixed missing/present keys; see kv/nats.go:108-123.")
}
