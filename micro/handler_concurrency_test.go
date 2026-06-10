package micro

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	natsmicro "github.com/nats-io/nats.go/micro"
)

func buildEndpoint(t *testing.T, fn Handler, opts ...EndpointOption) natsmicro.Handler {
	t.Helper()
	e := defaultEndpointOptions(fn)
	for i, o := range opts {
		if err := o(e); err != nil {
			t.Fatalf("opt[%d]: %v", i, err)
		}
	}
	if e.decFn == nil {
		e.decFn = func(b []byte) (any, error) { return b, nil }
	}
	if e.encFn == nil {
		e.encFn = func(v any) ([]byte, error) { return nil, nil }
	}
	return e.Handler()
}

// TestHandlerInlineBlocksUntilHandlerReturns proves the inline path runs the
// handler on the calling goroutine: Handle must not return until the handler
// returns. A semaphore-of-1 async implementation would NOT exhibit this, so
// this distinguishes genuine inline execution from bounded async with N=1.
func TestHandlerInlineBlocksUntilHandlerReturns(t *testing.T) {
	cases := []struct {
		name string
		opts []EndpointOption
	}{
		{"default", nil},
		{"concurrency_0", []EndpointOption{WithConcurrency(0)}},
		{"concurrency_1", []EndpointOption{WithConcurrency(1)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			fn := func(ctx context.Context, r any) (any, error) {
				close(entered)
				<-release
				return "ok", nil
			}
			h := buildEndpoint(t, fn, tc.opts...)
			req := newFakeRequest("s", nil, []byte("x"))

			returned := make(chan struct{})
			go func() {
				h.Handle(req)
				close(returned)
			}()

			<-entered
			select {
			case <-returned:
				t.Fatal("inline Handle returned before handler was released")
			case <-time.After(100 * time.Millisecond):
			}

			close(release)
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("Handle did not return after handler released")
			}
		})
	}
}

// TestHandlerInlinePanicPropagates pins the inline path's panic semantics: a
// handler panic propagates on the delivery goroutine, and the per-request
// context is still cancelled during unwinding (no leak).
func TestHandlerInlinePanicPropagates(t *testing.T) {
	var captured context.Context
	fn := func(ctx context.Context, r any) (any, error) {
		captured = ctx
		panic("boom")
	}
	h := buildEndpoint(t, fn)
	req := newFakeRequest("s", nil, []byte("x"))

	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected inline handler panic to propagate")
			}
		}()
		h.Handle(req)
	}()

	if captured == nil {
		t.Fatal("handler context was not captured")
	}
	if !errors.Is(captured.Err(), context.Canceled) {
		t.Errorf("per-request context must be cancelled on panic, got %v", captured.Err())
	}
}

// TestHandlerAsyncErrorSkipsPublishOnEmptyCode mirrors nats.go request.Error's
// empty-arg guard (request.go:134-139): when the resolved error code is empty,
// the async path must not publish a reply.
func TestHandlerAsyncErrorSkipsPublishOnEmptyCode(t *testing.T) {
	ran := make(chan struct{})
	boom := errors.New("boom")
	se := NewError(boom, "", nil, nil) // ServiceError with empty code
	fn := func(ctx context.Context, r any) (any, error) {
		close(ran)
		return nil, se
	}
	h := buildEndpoint(t, fn, WithConcurrency(2))
	req := newFakeRequest("s", nil, []byte("x"))

	h.Handle(req)
	<-ran
	time.Sleep(100 * time.Millisecond) // settle failsafe for the async skip branch

	req.mu.Lock()
	defer req.mu.Unlock()
	if len(req.respondCalls) != 0 || len(req.errorCalls) != 0 {
		t.Errorf("empty code must skip publish: respond=%d error=%d", len(req.respondCalls), len(req.errorCalls))
	}
}

// TestHandlerAsyncErrorSkipsPublishOnEmptyDesc covers the empty-description arm
// of the same guard, reached via a custom error func returning a non-empty code
// but empty description.
func TestHandlerAsyncErrorSkipsPublishOnEmptyDesc(t *testing.T) {
	ran := make(chan struct{})
	boom := errors.New("boom")
	errFn := func(error) (string, string, []byte, natsmicro.Headers) { return "499", "", nil, nil }
	fn := func(ctx context.Context, r any) (any, error) {
		close(ran)
		return nil, boom
	}
	h := buildEndpoint(t, fn, WithErrFn(errFn), WithConcurrency(2))
	req := newFakeRequest("s", nil, []byte("x"))

	h.Handle(req)
	<-ran
	time.Sleep(100 * time.Millisecond) // settle failsafe for the async skip branch

	req.mu.Lock()
	defer req.mu.Unlock()
	if len(req.respondCalls) != 0 || len(req.errorCalls) != 0 {
		t.Errorf("empty desc must skip publish: respond=%d error=%d", len(req.respondCalls), len(req.errorCalls))
	}
}

// TestHandlerAsyncErrorPublishesViaRespond proves the async path emits errors
// through Respond (never r.Error) and carries the nats.go micro error headers,
// matching request.Error's wire format.
func TestHandlerAsyncErrorPublishesViaRespond(t *testing.T) {
	boom := errors.New("boom")
	errFn := func(error) (string, string, []byte, natsmicro.Headers) {
		return "499", "custom", []byte("c"), natsmicro.Headers{"X": []string{"y"}}
	}
	fn := func(ctx context.Context, r any) (any, error) { return nil, boom }
	h := buildEndpoint(t, fn, WithErrFn(errFn), WithConcurrency(2))
	req := newFakeRequest("s", nil, []byte("x"))

	h.Handle(req)

	deadline := time.After(time.Second)
	for {
		req.mu.Lock()
		n := len(req.respondCalls)
		req.mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("async error response was not published via Respond")
		case <-time.After(5 * time.Millisecond):
		}
	}

	req.mu.Lock()
	defer req.mu.Unlock()
	if len(req.errorCalls) != 0 {
		t.Errorf("async path must never call r.Error, got %d calls", len(req.errorCalls))
	}
	c := req.respondCalls[0]
	if string(c.data) != "c" {
		t.Errorf("respond data: want c, got %q", c.data)
	}
	// Two WithHeaders opts: the error headers plus the custom headers.
	if len(c.opts) != 2 {
		t.Errorf("respond opts: want 2 (error headers + custom), got %d", len(c.opts))
	}
}

// TestWithLoggerFieldsNoAliasUnderConcurrency proves WithLoggerFields copies the
// existing field slice before appending: concurrent derivations from a shared
// base context must not mutate or race the base slice's backing array.
func TestWithLoggerFieldsNoAliasUnderConcurrency(t *testing.T) {
	base := make([]any, 0, 8) // spare capacity exposes any aliasing append
	base = append(base, "base", 0)
	ctx := context.WithValue(context.Background(), loggerFieldsContextKey{}, base)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := WithLoggerFields(ctx, "g", i)
			if got := LoggerFieldsFromContext(c); len(got) != 4 {
				t.Errorf("derived fields: want len 4, got %v", got)
			}
		}(i)
	}
	wg.Wait()

	got := LoggerFieldsFromContext(ctx)
	if len(got) != 2 || got[0] != "base" || got[1] != 0 {
		t.Errorf("base slice was mutated by concurrent derivations: %v", got)
	}
}
