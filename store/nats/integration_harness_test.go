package nats

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modernice/goes/helper/streams"
	"github.com/nats-io/nats.go/jetstream"
)

func TestHarnessSanity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, js, nc := newStore(t, "harness")
	if s == nil || js == nil || nc == nil {
		t.Fatal("newStore returned a nil component")
	}

	// Stream named "es_<sanitized>_<uuid8>" was created and filters
	// "es.harness.>".
	names, err := streams.Drain(ctx, js.StreamNames(ctx).Name())
	if err != nil {
		t.Fatalf("StreamNames: %v", err)
	}
	var found string
	for _, n := range names {
		if strings.HasPrefix(n, "es_TestHarnessSanity_") {
			found = n
			break
		}
	}
	if found == "" {
		t.Fatalf("expected stream prefix es_TestHarnessSanity_ in %v", names)
	}
	str, err := js.Stream(ctx, found)
	if err != nil {
		t.Fatalf("Stream %q: %v", found, err)
	}
	cfg := str.CachedInfo().Config
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "es.harness.>" {
		t.Fatalf("Subjects = %v, want [es.harness.>]", cfg.Subjects)
	}
	if cfg.Storage != jetstream.MemoryStorage {
		t.Fatalf("Storage = %v, want MemoryStorage", cfg.Storage)
	}
}

func TestHarnessParallelIsolation(t *testing.T) {
	t.Run("a", func(t *testing.T) {
		t.Parallel()
		s, _, _ := newStore(t, "agg_a")
		if s == nil {
			t.Fatal("nil store")
		}
	})
	t.Run("b", func(t *testing.T) {
		t.Parallel()
		s, _, _ := newStore(t, "agg_b")
		if s == nil {
			t.Fatal("nil store")
		}
	})
}
