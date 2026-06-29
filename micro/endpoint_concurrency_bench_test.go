package micro_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	natsmicro "github.com/nats-io/nats.go/micro"
	ffmicro "github.com/olpie101/fast-forward/micro"
)

// newService spins up a micro service on the embedded server and returns its
// root group plus a connection for issuing requests. Both are cleaned up when
// the benchmark ends.
func newService(b *testing.B, name string) (natsmicro.Group, *nats.Conn) {
	b.Helper()

	svcConn, err := nats.Connect(testServer.ClientURL())
	if err != nil {
		b.Fatalf("service connect: %v", err)
	}

	svc, err := natsmicro.AddService(svcConn, natsmicro.Config{
		Name:    name,
		Version: "0.0.1",
	})
	if err != nil {
		svcConn.Close()
		b.Fatalf("AddService: %v", err)
	}

	reqConn, err := nats.Connect(testServer.ClientURL())
	if err != nil {
		_ = svc.Stop()
		svcConn.Close()
		b.Fatalf("request connect: %v", err)
	}

	b.Cleanup(func() {
		_ = svc.Stop()
		svcConn.Close()
		reqConn.Close()
	})

	return svc.AddGroup("bench"), reqConn
}

// batchSize is the number of requests fired concurrently per benchmark
// iteration. It is the maximum concurrency the endpoint could exhibit, so the
// measured peak distinguishes a serial endpoint (peak stays 1) from a
// concurrent one (peak climbs toward batchSize).
const batchSize = 32

// fireBatch issues batchSize requests concurrently and waits for all of them,
// counting as one benchmark iteration. Firing a known concurrent batch (rather
// than relying on RunParallel's throughput-oriented scheduling) deterministically
// exposes how many handler invocations the endpoint allows to overlap.
func fireBatch(b *testing.B, nc *nats.Conn, subject string) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for j := 0; j < batchSize; j++ {
			go func() {
				defer wg.Done()
				if _, err := nc.Request(subject, nil, 5*time.Second); err != nil {
					b.Errorf("request: %v", err)
				}
			}()
		}
		wg.Wait()
	}
}

const handlerWork = 2 * time.Millisecond

// BenchmarkEndpointInlineSerial registers an endpoint through the fast-forward
// wrapper (ffmicro.AddEndpoint) using the default inline serial path
// (maxInFlight == 1). The NATS subscription delivers one message at a time, so
// handler invocations never overlap. Expectation: peak-concurrency == 1 even
// though each iteration fires batchSize requests at once; ns/op ≈ batchSize ×
// handlerWork because the batch drains serially.
func BenchmarkEndpointInlineSerial(b *testing.B) {
	group, nc := newService(b, "bench_inline")
	probe := &concurrencyProbe{}

	handler := func(ctx context.Context, req any) (any, error) {
		probe.enter()
		time.Sleep(handlerWork)
		probe.leave()
		return nil, nil
	}

	err := ffmicro.AddEndpoint("echo", group, handler,
		ffmicro.WithDecoderFn(func(b []byte) (any, error) { return b, nil }),
		ffmicro.WithEncoderFn(func(v any) ([]byte, error) { return nil, nil }),
	)
	if err != nil {
		b.Fatalf("AddEndpoint: %v", err)
	}

	fireBatch(b, nc, "bench.echo")

	b.ReportMetric(float64(probe.peakValue()), "peak-concurrency")
	if got := probe.peakValue(); got != 1 {
		b.Errorf("peak concurrency = %d, want 1 (inline endpoint must deliver serially)", got)
	}
}

// BenchmarkEndpointWithConcurrency registers an endpoint through the
// fast-forward wrapper (ffmicro.AddEndpoint) with WithConcurrency(batchSize).
// Each request is offloaded to a worker goroutine (semaphore-bounded) so
// handler invocations can overlap up to batchSize. Expectation:
// peak-concurrency > 1; ns/op much lower than BenchmarkEndpointInlineSerial
// because the batch runs in parallel rather than serially.
func BenchmarkEndpointWithConcurrency(b *testing.B) {
	group, nc := newService(b, "bench_concurrent")
	probe := &concurrencyProbe{}

	handler := func(ctx context.Context, req any) (any, error) {
		probe.enter()
		time.Sleep(handlerWork)
		probe.leave()
		return nil, nil
	}

	err := ffmicro.AddEndpoint("echo", group, handler,
		ffmicro.WithDecoderFn(func(b []byte) (any, error) { return b, nil }),
		ffmicro.WithEncoderFn(func(v any) ([]byte, error) { return nil, nil }),
		ffmicro.WithConcurrency(batchSize),
	)
	if err != nil {
		b.Fatalf("AddEndpoint: %v", err)
	}

	fireBatch(b, nc, "bench.echo")

	peak := probe.peakValue()
	b.ReportMetric(float64(peak), "peak-concurrency")
	if peak <= 1 {
		b.Errorf("peak concurrency = %d, want > 1 (WithConcurrency endpoint should overlap)", peak)
	}
}

// BenchmarkEndpointRawOffload is a raw nats.go micro comparison baseline: it
// registers a handler that spawns a goroutine and returns immediately, freeing
// the subscription delivery goroutine to accept the next message. This is the
// manual unbounded-concurrency approach that WithConcurrency replaces. It is
// retained here for throughput comparison only; it uses raw natsmicro.Group
// because the fast-forward wrapper owns Respond and does not expose the Request
// to the handler.
func BenchmarkEndpointRawOffload(b *testing.B) {
	group, nc := newService(b, "bench_rawoffload")
	probe := &concurrencyProbe{}

	err := group.AddEndpoint("echo", natsmicro.HandlerFunc(func(r natsmicro.Request) {
		go func() {
			probe.enter()
			time.Sleep(handlerWork)
			probe.leave()
			_ = r.Respond(nil)
		}()
	}))
	if err != nil {
		b.Fatalf("AddEndpoint: %v", err)
	}

	fireBatch(b, nc, "bench.echo")

	peak := probe.peakValue()
	b.ReportMetric(float64(peak), "peak-concurrency")
	if peak <= 1 {
		b.Errorf("peak concurrency = %d, want > 1 (raw-offload handler should overlap)", peak)
	}
}
