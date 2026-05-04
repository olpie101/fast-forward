package kv

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestDelete(t *testing.T) {
	t.Run("Delete leaves a delete marker in history", func(t *testing.T) {
		kv, bucket, _ := newKV[*UInt64Value](t)
		ctx := context.Background()

		if _, err := kv.Create(ctx, "k", &UInt64Value{Value: 1}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := kv.Put(ctx, "k", &UInt64Value{Value: 2}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := kv.Delete(ctx, "k"); err != nil {
			t.Fatalf("Delete err = %v", err)
		}

		hist, err := bucket.History("k")
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(hist) == 0 {
			t.Fatal("History empty; expected entries")
		}
		last := hist[len(hist)-1]
		if last.Operation() != nats.KeyValueDelete {
			t.Fatalf("last op = %v, want KeyValueDelete", last.Operation())
		}
	})

	t.Run("Delete on non-existent key is nil", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		if err := kv.Delete(context.Background(), "missing"); err != nil {
			t.Fatalf("Delete missing err = %v, want nil", err)
		}
	})
}

func TestPurge(t *testing.T) {
	t.Run("WithPurge replaces history with single purge marker", func(t *testing.T) {
		kv, bucket, _ := newKV[*UInt64Value](t)
		ctx := context.Background()

		if _, err := kv.Create(ctx, "k", &UInt64Value{Value: 1}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := kv.Put(ctx, "k", &UInt64Value{Value: 2}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := kv.Delete(WithPurge(ctx), "k"); err != nil {
			t.Fatalf("Delete(WithPurge): %v", err)
		}

		hist, err := bucket.History("k")
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if len(hist) != 1 {
			t.Fatalf("History len = %d, want 1", len(hist))
		}
		if hist[0].Operation() != nats.KeyValuePurge {
			t.Fatalf("op = %v, want KeyValuePurge", hist[0].Operation())
		}

		_, _, getErr := kv.Get(ctx, "k")
		if !errors.Is(getErr, nats.ErrKeyNotFound) {
			t.Fatalf("Get after Purge err = %v, want ErrKeyNotFound", getErr)
		}
	})

	t.Run("WithPurge on non-existent key is nil", func(t *testing.T) {
		kv, _, _ := newKV[*UInt64Value](t)
		if err := kv.Delete(WithPurge(context.Background()), "missing"); err != nil {
			t.Fatalf("Delete(WithPurge) missing err = %v, want nil", err)
		}
	})
}

func TestModifiers(t *testing.T) {
	ctx := context.Background()

	t.Run("isDeletePurge", func(t *testing.T) {
		if isDeletePurge(ctx) {
			t.Error("isDeletePurge(plain) = true, want false")
		}
		if !isDeletePurge(WithPurge(ctx)) {
			t.Error("isDeletePurge(WithPurge) = false, want true")
		}
	})

	t.Run("isStopOnZero", func(t *testing.T) {
		if isStopOnZero(ctx) {
			t.Error("isStopOnZero(plain) = true, want false")
		}
		if !isStopOnZero(WithStopOnZero(ctx)) {
			t.Error("isStopOnZero(WithStopOnZero) = false, want true")
		}
	})

	t.Run("isNoErrorOnNotFound", func(t *testing.T) {
		if isNoErrorOnNotFound(ctx) {
			t.Error("isNoErrorOnNotFound(plain) = true, want false")
		}
		if !isNoErrorOnNotFound(WithNoErrorOnNotFound(ctx)) {
			t.Error("isNoErrorOnNotFound(WithNoErrorOnNotFound) = false, want true")
		}
	})

	t.Run("composition independence", func(t *testing.T) {
		c := WithPurge(WithNoErrorOnNotFound(WithStopOnZero(ctx)))
		if !isDeletePurge(c) {
			t.Error("isDeletePurge composed = false")
		}
		if !isStopOnZero(c) {
			t.Error("isStopOnZero composed = false")
		}
		if !isNoErrorOnNotFound(c) {
			t.Error("isNoErrorOnNotFound composed = false")
		}
	})
}
