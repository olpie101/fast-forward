package nats

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/codec"
	"github.com/modernice/goes/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/olpie101/fast-forward/kv"
	"go.uber.org/zap"
)

// sanitizeBucketName makes a t.Name() safe for JetStream bucket and stream
// naming (only ^[a-zA-Z0-9_-]+$ is allowed).
func sanitizeBucketName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// shortID returns the first 8 chars of a hyphen-stripped UUID.
func shortID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
}

// integrationHarness bundles the per-test JetStream resources used by store
// integration tests. The exposed jetstream.JetStream is a TEST-SIDE handle
// (independent jetstream.New(nc) call) intended for stream/KV/consumer
// introspection from tests; it is distinct from Store.js (unexported).
type integrationHarness struct {
	store      *Store
	js         jetstream.JetStream
	nc         *nats.Conn
	codec      *codec.Registry
	streamName string
}

// cachedStreamSubject returns the store's cached stream subject for this
// harness's stream under the store's RWMutex, so white-box reads of
// streamSubjectMapper stay synchronized with subscribe's writes.
func (h *integrationHarness) cachedStreamSubject() string {
	h.store.streamSubjectMu.RLock()
	defer h.store.streamSubjectMu.RUnlock()
	return h.store.streamSubjectMapper[h.streamName]
}

// newStore builds the per-test embedded harness for the given aggregate name.
// Stream and KV buckets are created with unique names, and full teardown is
// registered via t.Cleanup. The returned (*Store, jetstream.JetStream, *nats.Conn)
// matches the locked harness signature documented in the plan.
func newStore(t *testing.T, aggregate string, opts ...StoreOption) (*Store, jetstream.JetStream, *nats.Conn) {
	t.Helper()
	h := newHarness(t, aggregate, opts...)
	return h.store, h.js, h.nc
}

func newHarness(t *testing.T, aggregate string, opts ...StoreOption) *integrationHarness {
	t.Helper()

	nc, err := nats.Connect(testServer.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		t.Fatalf("jetstream.New: %v", err)
	}

	jsLegacy, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatalf("nc.JetStream: %v", err)
	}

	suffix := fmt.Sprintf("%s_%s", sanitizeBucketName(t.Name()), shortID())
	streamName := "es_" + suffix
	leaseBucket := "lease_" + suffix
	versionBucket := "version_" + suffix

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subject := fmt.Sprintf("es.%s.>", aggregate)
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		nc.Close()
		t.Fatalf("CreateStream %q: %v", streamName, err)
	}

	leaseRaw, err := jsLegacy.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  leaseBucket,
		Storage: nats.MemoryStorage,
	})
	if err != nil {
		_ = js.DeleteStream(ctx, streamName)
		nc.Close()
		t.Fatalf("CreateKeyValue %q: %v", leaseBucket, err)
	}

	versionRaw, err := jsLegacy.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  versionBucket,
		Storage: nats.MemoryStorage,
	})
	if err != nil {
		_ = jsLegacy.DeleteKeyValue(leaseBucket)
		_ = js.DeleteStream(ctx, streamName)
		nc.Close()
		t.Fatalf("CreateKeyValue %q: %v", versionBucket, err)
	}

	leaseKV := kv.New[*kv.NilValue](leaseRaw)
	versionKV := kv.New[*kv.UInt64Value](versionRaw)

	reg := codec.New()

	allOpts := []StoreOption{
		WithLogger(zap.NewNop().Sugar()),
		WithRetryCount(1),
		WithPullExpiry(1 * time.Second),
		WithWriteLeaseKV(leaseKV),
		WithVersionKV(versionKV),
	}
	allOpts = append(allOpts, opts...)

	s, err := New(nc, reg, allOpts...)
	if err != nil {
		_ = jsLegacy.DeleteKeyValue(leaseBucket)
		_ = jsLegacy.DeleteKeyValue(versionBucket)
		_ = js.DeleteStream(ctx, streamName)
		nc.Close()
		t.Fatalf("Store.New: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = jsLegacy.DeleteKeyValue(leaseBucket)
		_ = jsLegacy.DeleteKeyValue(versionBucket)
		_ = js.DeleteStream(cleanupCtx, streamName)
		_ = nc.Drain()
		deadline := time.After(2 * time.Second)
		for nc.Status() != nats.CLOSED {
			select {
			case <-deadline:
				nc.Close()
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	})

	return &integrationHarness{
		store:      s,
		js:         js,
		nc:         nc,
		codec:      reg,
		streamName: streamName,
	}
}

// nilLeaseKV creates a real KV bucket and a typed wrapper for the lease KV
// surface. The bucket is registered for cleanup via t.Cleanup.
func nilLeaseKV(t *testing.T, nc *nats.Conn, bucket string) kv.KeyValuer[*kv.NilValue] {
	t.Helper()
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("nc.JetStream: %v", err)
	}
	raw, err := jsLegacy.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  bucket,
		Storage: nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("CreateKeyValue %q: %v", bucket, err)
	}
	t.Cleanup(func() { _ = jsLegacy.DeleteKeyValue(bucket) })
	return kv.New[*kv.NilValue](raw)
}

// nilVersionKV mirrors nilLeaseKV for the version KV surface.
func nilVersionKV(t *testing.T, nc *nats.Conn, bucket string) kv.KeyValuer[*kv.UInt64Value] {
	t.Helper()
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("nc.JetStream: %v", err)
	}
	raw, err := jsLegacy.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  bucket,
		Storage: nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("CreateKeyValue %q: %v", bucket, err)
	}
	t.Cleanup(func() { _ = jsLegacy.DeleteKeyValue(bucket) })
	return kv.New[*kv.UInt64Value](raw)
}

// registerString registers a trivial string-payload event type with the
// harness codec under the given event name. The default JSON marshalers handle
// the round trip.
func (h *integrationHarness) registerString(name string) {
	codec.Register[string](h.codec, name)
}

// drainQuery drains both the events and errors channels returned by Query
// concurrently with a hard timeout. Per query.go:82 opErrs is closed via defer
// at Query return, so concurrent drain is safe.
func drainQuery(t *testing.T, evts <-chan event.Event, errs <-chan error, timeout time.Duration) ([]event.Event, []error) {
	t.Helper()
	if evts == nil {
		t.Fatal("drainQuery: events channel is nil")
	}
	if errs == nil {
		t.Fatal("drainQuery: errors channel is nil")
	}

	var (
		gotEvts []event.Event
		gotErrs []error
		mu      sync.Mutex
		wg      sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for e := range evts {
			mu.Lock()
			gotEvts = append(gotEvts, e)
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for e := range errs {
			mu.Lock()
			gotErrs = append(gotErrs, e)
			mu.Unlock()
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("drainQuery: timed out after %s", timeout)
	}

	return gotEvts, gotErrs
}
