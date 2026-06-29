package micro_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	natsmicro "github.com/nats-io/nats.go/micro"
	ffmicro "github.com/olpie101/fast-forward/micro"
)

// TestEndpointAsyncConcurrencyCap proves WithConcurrency(N>1) processes requests
// concurrently up to N and never beyond. Overlap is created deterministically by
// blocking handlers at a barrier (not by sleeps); wall-clock waits are failsafes
// only.
func TestEndpointAsyncConcurrencyCap(t *testing.T) {
	const cap = 4
	const total = 8

	group, svcConn, nc := newServiceT(t, "asynccap")
	probe := &concurrencyProbe{}
	entered := make(chan struct{}, total)
	release := make(chan struct{})

	handler := func(ctx context.Context, req any) (any, error) {
		probe.enter()
		entered <- struct{}{}
		<-release
		probe.leave()
		return []byte("ok"), nil
	}

	err := ffmicro.AddEndpoint("echo", group, handler,
		ffmicro.WithDecoderFn(func(b []byte) (any, error) { return b, nil }),
		ffmicro.WithEncoderFn(func(v any) ([]byte, error) { return v.([]byte), nil }),
		ffmicro.WithConcurrency(cap),
	)
	if err != nil {
		t.Fatalf("AddEndpoint: %v", err)
	}
	if err := svcConn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	resps := make(chan *nats.Msg, total)
	reqErrs := make(chan error, total)
	for i := 0; i < total; i++ {
		go func() {
			m, e := nc.Request("asynccap.echo", nil, 5*time.Second)
			if e != nil {
				reqErrs <- e
				return
			}
			resps <- m
		}()
	}

	// Wait until exactly cap handlers are simultaneously in flight.
	for i := 0; i < cap; i++ {
		select {
		case <-entered:
		case e := <-reqErrs:
			t.Fatalf("request err while filling cap: %v", e)
		case <-time.After(5 * time.Second):
			t.Fatalf("only saw %d handlers enter, want %d", i, cap)
		}
	}

	// No further handler may enter while the cap is saturated.
	select {
	case <-entered:
		t.Fatalf("more than %d handlers in flight: cap exceeded", cap)
	case <-time.After(300 * time.Millisecond):
	}

	close(release)

	for i := 0; i < total; i++ {
		select {
		case m := <-resps:
			if string(m.Data) != "ok" {
				t.Errorf("response data: want ok, got %q", m.Data)
			}
		case e := <-reqErrs:
			t.Fatalf("request err: %v", e)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for responses")
		}
	}

	if got := probe.peakValue(); got != cap {
		t.Errorf("peak concurrency: want %d, got %d", cap, got)
	}
}

// TestEndpointAsyncErrorHeaders proves async errors are delivered through Respond
// carrying the nats.go micro error headers (Nats-Service-Error[-Code]), matching
// request.Error's wire format. Paired with -race it confirms no broad
// respondError race, since successful publishes write no respondError.
func TestEndpointAsyncErrorHeaders(t *testing.T) {
	group, svcConn, nc := newServiceT(t, "asyncerr")

	handler := func(ctx context.Context, req any) (any, error) { return nil, errors.New("boom") }
	errFn := func(error) (string, string, []byte, natsmicro.Headers) {
		return "499", "custom", []byte("c"), nil
	}

	err := ffmicro.AddEndpoint("echo", group, handler,
		ffmicro.WithDecoderFn(func(b []byte) (any, error) { return b, nil }),
		ffmicro.WithEncoderFn(func(v any) ([]byte, error) { return nil, nil }),
		ffmicro.WithErrFn(errFn),
		ffmicro.WithConcurrency(2),
	)
	if err != nil {
		t.Fatalf("AddEndpoint: %v", err)
	}
	if err := svcConn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	const total = 4
	type result struct {
		msg *nats.Msg
		err error
	}
	results := make(chan result, total)
	for i := 0; i < total; i++ {
		go func() {
			m, e := nc.Request("asyncerr.echo", nil, 5*time.Second)
			results <- result{msg: m, err: e}
		}()
	}

	for i := 0; i < total; i++ {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("request err: %v", r.err)
			}
			if code := r.msg.Header.Get(natsmicro.ErrorCodeHeader); code != "499" {
				t.Errorf("error code header: want 499, got %q", code)
			}
			if desc := r.msg.Header.Get(natsmicro.ErrorHeader); desc != "custom" {
				t.Errorf("error header: want custom, got %q", desc)
			}
			if string(r.msg.Data) != "c" {
				t.Errorf("error data: want c, got %q", r.msg.Data)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for error responses")
		}
	}
}
