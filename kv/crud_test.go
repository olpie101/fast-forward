package kv

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

// failingValue triggers MarshalValue / UnmarshalValue errors on demand for
// CRUD/marshal-error tests.
type failingValue struct {
	err error
}

func (f *failingValue) MarshalValue() ([]byte, error) {
	return nil, f.err
}

func (f *failingValue) UnmarshalValue([]byte) error {
	return f.err
}

func TestKeys(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctx := context.Background()

	t.Run("empty bucket returns nil/nil", func(t *testing.T) {
		keys, err := kv.Keys(ctx)
		if err != nil {
			t.Fatalf("Keys err = %v, want nil", err)
		}
		if keys != nil {
			t.Fatalf("Keys = %v, want nil", keys)
		}
	})

	t.Run("after creates returns set", func(t *testing.T) {
		for _, k := range []string{"a", "b", "c"} {
			if _, err := kv.Create(ctx, k, &UInt64Value{Value: 1}); err != nil {
				t.Fatalf("Create %q: %v", k, err)
			}
		}
		keys, err := kv.Keys(ctx)
		if err != nil {
			t.Fatalf("Keys err = %v, want nil", err)
		}
		sort.Strings(keys)
		want := []string{"a", "b", "c"}
		if len(keys) != len(want) {
			t.Fatalf("Keys = %v, want %v", keys, want)
		}
		for i := range want {
			if keys[i] != want[i] {
				t.Fatalf("Keys = %v, want %v", keys, want)
			}
		}
	})
}

func TestCreate(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctx := context.Background()

	t.Run("first create returns rev=1", func(t *testing.T) {
		rev, err := kv.Create(ctx, "k", &UInt64Value{Value: 1})
		if err != nil {
			t.Fatalf("Create err = %v", err)
		}
		if rev != 1 {
			t.Fatalf("Create rev = %d, want 1", rev)
		}
	})

	t.Run("second create on same key returns ErrKeyExists", func(t *testing.T) {
		_, err := kv.Create(ctx, "k", &UInt64Value{Value: 2})
		if err == nil {
			t.Fatal("Create err = nil, want non-nil")
		}
		if !errors.Is(err, nats.ErrKeyExists) {
			t.Fatalf("Create err = %v, want ErrKeyExists", err)
		}
	})

	t.Run("marshal error propagates and bucket unchanged", func(t *testing.T) {
		kv2, bucket, _ := newKV[*failingValue](t)
		sentinel := errors.New("marshal boom")
		rev, err := kv2.Create(ctx, "k", &failingValue{err: sentinel})
		if !errors.Is(err, sentinel) {
			t.Fatalf("Create err = %v, want %v", err, sentinel)
		}
		if rev != 0 {
			t.Fatalf("Create rev = %d, want 0", rev)
		}
		keys, err := bucket.Keys()
		if err != nil && !errors.Is(err, nats.ErrNoKeysFound) {
			t.Fatalf("bucket.Keys err = %v", err)
		}
		if len(keys) != 0 {
			t.Fatalf("bucket keys = %v, want empty", keys)
		}
	})
}

func TestGet(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctx := context.Background()

	if _, err := kv.Create(ctx, "k", &UInt64Value{Value: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	t.Run("existing key returns value and revision", func(t *testing.T) {
		v, rev, err := kv.Get(ctx, "k")
		if err != nil {
			t.Fatalf("Get err = %v", err)
		}
		if rev != 1 {
			t.Fatalf("Get rev = %d, want 1", rev)
		}
		if v == nil || v.Value != 1 {
			t.Fatalf("Get value = %#v, want &UInt64Value{1}", v)
		}
	})

	t.Run("missing key returns resolved zero and ErrKeyNotFound", func(t *testing.T) {
		v, rev, err := kv.Get(ctx, "missing")
		if !errors.Is(err, nats.ErrKeyNotFound) {
			t.Fatalf("Get err = %v, want ErrKeyNotFound", err)
		}
		if rev != 0 {
			t.Fatalf("Get rev = %d, want 0", rev)
		}
		if v == nil {
			t.Fatal("Get value = nil, want resolved zero pointer")
		}
		if v.Value != 0 {
			t.Fatalf("Get value = %#v, want zero", v)
		}
	})
}

func TestPut(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctx := context.Background()

	if _, err := kv.Create(ctx, "k", &UInt64Value{Value: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	rev, err := kv.Put(ctx, "k", &UInt64Value{Value: 2})
	if err != nil {
		t.Fatalf("Put err = %v", err)
	}
	if rev != 2 {
		t.Fatalf("Put rev = %d, want 2", rev)
	}
	v, gotRev, err := kv.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get err = %v", err)
	}
	if gotRev != 2 || v.Value != 2 {
		t.Fatalf("Get after Put = (%#v, %d), want (Value:2, rev:2)", v, gotRev)
	}

	t.Run("marshal error propagates and bucket unchanged", func(t *testing.T) {
		kv2, bucket, _ := newKV[*failingValue](t)
		// Pre-populate so we can verify Put-with-marshal-error doesn't add or
		// rewrite keys.
		if _, err := bucket.PutString("seed", "raw"); err != nil {
			t.Fatalf("PutString seed: %v", err)
		}
		sentinel := errors.New("put marshal boom")
		rev, err := kv2.Put(context.Background(), "k", &failingValue{err: sentinel})
		if !errors.Is(err, sentinel) {
			t.Fatalf("Put err = %v, want %v", err, sentinel)
		}
		if rev != 0 {
			t.Fatalf("Put rev = %d, want 0", rev)
		}
		keys, err := bucket.Keys()
		if err != nil && !errors.Is(err, nats.ErrNoKeysFound) {
			t.Fatalf("bucket.Keys err = %v", err)
		}
		if len(keys) != 1 || keys[0] != "seed" {
			t.Fatalf("bucket keys = %v, want [seed]", keys)
		}
	})
}

func TestUpdate(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctx := context.Background()

	if _, err := kv.Create(ctx, "k", &UInt64Value{Value: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	rev, err := kv.Put(ctx, "k", &UInt64Value{Value: 2})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if rev != 2 {
		t.Fatalf("Put rev = %d, want 2", rev)
	}

	t.Run("update with correct lastRevision succeeds", func(t *testing.T) {
		newRev, err := kv.Update(ctx, "k", &UInt64Value{Value: 3}, 2)
		if err != nil {
			t.Fatalf("Update err = %v", err)
		}
		if newRev != 3 {
			t.Fatalf("Update rev = %d, want 3", newRev)
		}
	})

	t.Run("update with stale lastRevision fails with wrong-last-sequence", func(t *testing.T) {
		_, err := kv.Update(ctx, "k", &UInt64Value{Value: 4}, 2)
		if err == nil {
			t.Fatal("Update err = nil, want non-nil")
		}
		if !strings.Contains(err.Error(), "wrong last sequence") {
			t.Fatalf("Update err = %v, want contains 'wrong last sequence'", err)
		}
	})

	t.Run("marshal error propagates and bucket unchanged", func(t *testing.T) {
		kv2, bucket, _ := newKV[*failingValue](t)
		if _, err := bucket.PutString("k", "raw"); err != nil {
			t.Fatalf("PutString seed: %v", err)
		}
		// Capture pre-state revision.
		entry, err := bucket.Get("k")
		if err != nil {
			t.Fatalf("bucket.Get seed: %v", err)
		}
		seedRev := entry.Revision()

		sentinel := errors.New("update marshal boom")
		rev, err := kv2.Update(context.Background(), "k", &failingValue{err: sentinel}, seedRev)
		if !errors.Is(err, sentinel) {
			t.Fatalf("Update err = %v, want %v", err, sentinel)
		}
		if rev != 0 {
			t.Fatalf("Update rev = %d, want 0", rev)
		}
		// Revision must be unchanged.
		after, err := bucket.Get("k")
		if err != nil {
			t.Fatalf("bucket.Get after: %v", err)
		}
		if after.Revision() != seedRev {
			t.Fatalf("bucket revision = %d, want unchanged %d", after.Revision(), seedRev)
		}
	})
}

func TestLastRevision(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctx := context.Background()

	if _, err := kv.Create(ctx, "k", &UInt64Value{Value: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := kv.Put(ctx, "k", &UInt64Value{Value: 2}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Update(ctx, "k", &UInt64Value{Value: 3}, 2); err != nil {
		t.Fatalf("Update: %v", err)
	}

	t.Run("returns latest revision", func(t *testing.T) {
		rev, err := kv.LastRevision(ctx, "k")
		if err != nil {
			t.Fatalf("LastRevision err = %v", err)
		}
		if rev != 3 {
			t.Fatalf("LastRevision = %d, want 3", rev)
		}
	})

	t.Run("missing key returns ErrKeyNotFound", func(t *testing.T) {
		rev, err := kv.LastRevision(ctx, "missing")
		if !errors.Is(err, nats.ErrKeyNotFound) {
			t.Fatalf("LastRevision err = %v, want ErrKeyNotFound", err)
		}
		if rev != 0 {
			t.Fatalf("LastRevision rev = %d, want 0", rev)
		}
	})
}

func TestDelete_Basic(t *testing.T) {
	kv, _, _ := newKV[*UInt64Value](t)
	ctx := context.Background()

	if _, err := kv.Create(ctx, "k", &UInt64Value{Value: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := kv.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete err = %v", err)
	}
	_, _, err := kv.Get(ctx, "k")
	if !errors.Is(err, nats.ErrKeyNotFound) {
		t.Fatalf("Get after Delete err = %v, want ErrKeyNotFound", err)
	}
}
