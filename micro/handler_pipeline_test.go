package micro

// Handler-pipeline tests. NOTE: production code at endpointer.go:61,63,69,75,83
// emits stdout debug prints on every request; this file does NOT capture
// stdout — the prints are tolerated noise (precedent: projection/streams.go:139).

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/invopop/jsonschema"
	natsmicro "github.com/nats-io/nats.go/micro"
)

// fakeRequest implements natsmicro.Request. Records all calls; never holds a
// NATS connection.
type fakeRequest struct {
	mu               sync.Mutex
	subject          string
	headers          natsmicro.Headers
	data             []byte
	respondCalls     []respondCall
	respondJSONCalls []respondJSONCall
	errorCalls       []errorCall
	respondErr       error
	respondJSONErr   error
	errorErr         error
}

type respondCall struct {
	data []byte
	opts []natsmicro.RespondOpt
}
type respondJSONCall struct {
	v    any
	opts []natsmicro.RespondOpt
}
type errorCall struct {
	code, desc string
	data       []byte
	opts       []natsmicro.RespondOpt
}

func newFakeRequest(subject string, headers natsmicro.Headers, data []byte) *fakeRequest {
	return &fakeRequest{subject: subject, headers: headers, data: data}
}

func (f *fakeRequest) Data() []byte                { return f.data }
func (f *fakeRequest) Headers() natsmicro.Headers  { return f.headers }
func (f *fakeRequest) Subject() string             { return f.subject }
func (f *fakeRequest) Reply() string               { return "" }

func (f *fakeRequest) Respond(b []byte, opts ...natsmicro.RespondOpt) error {
	f.mu.Lock()
	f.respondCalls = append(f.respondCalls, respondCall{data: b, opts: opts})
	err := f.respondErr
	f.mu.Unlock()
	return err
}
func (f *fakeRequest) RespondJSON(v any, opts ...natsmicro.RespondOpt) error {
	f.mu.Lock()
	f.respondJSONCalls = append(f.respondJSONCalls, respondJSONCall{v: v, opts: opts})
	err := f.respondJSONErr
	f.mu.Unlock()
	return err
}
func (f *fakeRequest) Error(code, desc string, data []byte, opts ...natsmicro.RespondOpt) error {
	f.mu.Lock()
	f.errorCalls = append(f.errorCalls, errorCall{code: code, desc: desc, data: data, opts: opts})
	err := f.errorErr
	f.mu.Unlock()
	return err
}

// fakeGroup implements natsmicro.Group; records AddEndpoint calls.
type fakeGroup struct {
	mu     sync.Mutex
	calls  []addEndpointCall
	addErr error
}

type addEndpointCall struct {
	name    string
	handler natsmicro.Handler
	opts    []natsmicro.EndpointOpt
}

func (g *fakeGroup) AddEndpoint(name string, h natsmicro.Handler, opts ...natsmicro.EndpointOpt) error {
	g.mu.Lock()
	g.calls = append(g.calls, addEndpointCall{name: name, handler: h, opts: opts})
	err := g.addErr
	g.mu.Unlock()
	return err
}

func (g *fakeGroup) AddGroup(name string, opts ...natsmicro.GroupOpt) natsmicro.Group {
	panic("unused")
}

// runHandler builds an endpointOpts via fluent options and invokes the
// micro.Handler returned by Handler() against req.
func runHandler(t *testing.T, fn Handler, req *fakeRequest, opts ...EndpointOption) *endpointOpts {
	t.Helper()
	e := defaultEndpointOptions(fn)
	for i, o := range opts {
		if err := o(e); err != nil {
			t.Fatalf("opt[%d]: %v", i, err)
		}
	}
	h := e.Handler()
	h.Handle(req)
	return e
}

func TestHandlerSuccessPath(t *testing.T) {
	var capturedReq any
	fn := func(ctx context.Context, r any) (any, error) {
		capturedReq = r
		return "result", nil
	}
	dec := func(b []byte) (any, error) { return "decoded", nil }
	enc := func(v any) ([]byte, error) { return []byte("RESULT"), nil }
	req := newFakeRequest("subj", nil, []byte("RAW"))

	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc))

	if capturedReq != "decoded" {
		t.Errorf("fn req: want decoded, got %v", capturedReq)
	}
	if len(req.respondCalls) != 1 {
		t.Fatalf("respondCalls: want 1, got %d", len(req.respondCalls))
	}
	if string(req.respondCalls[0].data) != "RESULT" {
		t.Errorf("respond data: want RESULT, got %q", req.respondCalls[0].data)
	}
	if len(req.errorCalls) != 0 {
		t.Errorf("errorCalls: want 0, got %d", len(req.errorCalls))
	}
}

func TestHandlerDecoderErrorMappedToDefault(t *testing.T) {
	called := 0
	fn := func(ctx context.Context, r any) (any, error) { called++; return nil, nil }
	errDec := errors.New("bad-data")
	dec := func(b []byte) (any, error) { return nil, errDec }
	enc := func(v any) ([]byte, error) { return nil, nil }
	req := newFakeRequest("subj", nil, []byte("RAW"))
	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc))
	if called != 0 {
		t.Errorf("fn must not be called when decoder errors")
	}
	if len(req.errorCalls) != 1 {
		t.Fatalf("errorCalls: want 1, got %d", len(req.errorCalls))
	}
	if req.errorCalls[0].code != "500" || req.errorCalls[0].desc != "an unknown error occurred" {
		t.Errorf("decoder error → default 500: got (%q,%q)", req.errorCalls[0].code, req.errorCalls[0].desc)
	}
}

func TestHandlerDecoderErrorErrDecodingErrorMapsTo400(t *testing.T) {
	fn := func(ctx context.Context, r any) (any, error) { return nil, nil }
	dec := func(b []byte) (any, error) { return nil, ErrDecodingError }
	enc := func(v any) ([]byte, error) { return nil, nil }
	req := newFakeRequest("subj", nil, []byte("RAW"))
	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc))
	if len(req.errorCalls) != 1 {
		t.Fatalf("errorCalls: want 1, got %d", len(req.errorCalls))
	}
	if req.errorCalls[0].code != "400" || req.errorCalls[0].desc != "invalid request" {
		t.Errorf("got (%q,%q)", req.errorCalls[0].code, req.errorCalls[0].desc)
	}
}

func TestHandlerHandlerErrorMapsToTimeout(t *testing.T) {
	encCalled := 0
	fn := func(ctx context.Context, r any) (any, error) { return nil, context.Canceled }
	dec := func(b []byte) (any, error) { return "x", nil }
	enc := func(v any) ([]byte, error) { encCalled++; return nil, nil }
	req := newFakeRequest("subj", nil, []byte("RAW"))
	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc))
	if encCalled != 0 {
		t.Errorf("encFn must not be called on handler err")
	}
	if len(req.errorCalls) != 1 || req.errorCalls[0].code != "408" {
		t.Errorf("got %v", req.errorCalls)
	}
}

func TestHandlerEncoderError(t *testing.T) {
	fn := func(ctx context.Context, r any) (any, error) { return "v", nil }
	dec := func(b []byte) (any, error) { return "x", nil }
	enc := func(v any) ([]byte, error) { return nil, errors.New("enc-fail") }
	req := newFakeRequest("subj", nil, []byte("RAW"))
	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc))
	if len(req.respondCalls) != 0 {
		t.Errorf("respond must not be called on encoder err")
	}
	if len(req.errorCalls) != 1 || req.errorCalls[0].code != "500" {
		t.Errorf("got %v", req.errorCalls)
	}
}

func TestHandlerServiceErrorPath(t *testing.T) {
	errBoom := errors.New("boom")
	se := NewError(errBoom, "418", []byte("teapot"), natsmicro.Headers{"X": []string{"y"}})
	fn := func(ctx context.Context, r any) (any, error) { return nil, se }
	dec := func(b []byte) (any, error) { return "x", nil }
	enc := func(v any) ([]byte, error) { return nil, nil }
	req := newFakeRequest("subj", nil, []byte("RAW"))
	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc))
	if len(req.errorCalls) != 1 {
		t.Fatalf("errorCalls: want 1, got %d", len(req.errorCalls))
	}
	c := req.errorCalls[0]
	if c.code != "418" {
		t.Errorf("code: want 418, got %q", c.code)
	}
	if c.desc != "service error: boom" {
		t.Errorf("desc: want service error: boom, got %q", c.desc)
	}
	if string(c.data) != "teapot" {
		t.Errorf("data: want teapot, got %q", c.data)
	}
	if len(c.opts) != 1 {
		t.Errorf("want 1 RespondOpt (WithHeaders), got %d", len(c.opts))
	}
}

func TestHandlerCustomErrFnMapsCode(t *testing.T) {
	fn := func(ctx context.Context, r any) (any, error) { return nil, errors.New("plain") }
	dec := func(b []byte) (any, error) { return "x", nil }
	enc := func(v any) ([]byte, error) { return nil, nil }
	customErr := func(error) (string, string, []byte, natsmicro.Headers) {
		return "499", "custom", []byte("c"), nil
	}
	req := newFakeRequest("subj", nil, []byte("RAW"))
	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc), WithErrFn(customErr))
	if len(req.errorCalls) != 1 {
		t.Fatalf("errorCalls: want 1, got %d", len(req.errorCalls))
	}
	c := req.errorCalls[0]
	if c.code != "499" || string(c.data) != "c" {
		t.Errorf("got code=%q data=%q", c.code, string(c.data))
	}
}

func TestHandlerTimeoutContextPropagation(t *testing.T) {
	fn := func(ctx context.Context, r any) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
			return nil, errors.New("did not cancel")
		}
	}
	dec := func(b []byte) (any, error) { return "x", nil }
	enc := func(v any) ([]byte, error) { return nil, nil }
	req := newFakeRequest("subj", nil, []byte("RAW"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc), WithTimeout(50*time.Millisecond))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return within wall-clock cap")
	}
	if len(req.errorCalls) != 1 || req.errorCalls[0].code != "408" {
		t.Errorf("got %v", req.errorCalls)
	}
}

func TestHandlerSubMillisecondTimeoutHasNoDeadline(t *testing.T) {
	var capturedHasDeadline bool
	fn := func(ctx context.Context, r any) (any, error) {
		_, ok := ctx.Deadline()
		capturedHasDeadline = ok
		return "v", nil
	}
	dec := func(b []byte) (any, error) { return "x", nil }
	enc := func(v any) ([]byte, error) { return []byte("R"), nil }
	req := newFakeRequest("subj", nil, []byte("RAW"))
	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc), WithTimeout(500*time.Microsecond))
	if capturedHasDeadline {
		t.Errorf("sub-millisecond timeout must produce ctx WITHOUT deadline (handlerCtx WithCancel branch)")
	}
}

func TestHandlerCtxFnInjectsValuesAndArgs(t *testing.T) {
	type kT struct{}
	hdr := natsmicro.Headers{"H": []string{"v"}}
	var capSubj string
	var capHdr natsmicro.Headers
	var capV any
	ctxFn := func(ctx context.Context, subj string, h natsmicro.Headers) context.Context {
		capSubj = subj
		capHdr = h
		return context.WithValue(ctx, kT{}, "v")
	}
	fn := func(ctx context.Context, r any) (any, error) {
		capV = ctx.Value(kT{})
		return "ok", nil
	}
	dec := func(b []byte) (any, error) { return "x", nil }
	enc := func(v any) ([]byte, error) { return []byte("R"), nil }
	req := newFakeRequest("subj", hdr, []byte("RAW"))
	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc), WithContextFn(ctxFn))
	if capSubj != "subj" {
		t.Errorf("subj: want subj, got %q", capSubj)
	}
	if capHdr["H"][0] != "v" {
		t.Errorf("hdr: want H=v, got %v", capHdr)
	}
	if capV != "v" {
		t.Errorf("ctx value: want v, got %v", capV)
	}
}

func TestHandlerInjectsRequestLengthLoggerField(t *testing.T) {
	var fields []any
	fn := func(ctx context.Context, r any) (any, error) {
		fields = LoggerFieldsFromContext(ctx)
		return "ok", nil
	}
	dec := func(b []byte) (any, error) { return "x", nil }
	enc := func(v any) ([]byte, error) { return []byte("R"), nil }
	req := newFakeRequest("subj", nil, []byte("hello"))
	runHandler(t, fn, req, WithDecoderFn(dec), WithEncoderFn(enc))
	// Look for "micro_request_length" key followed by the int length.
	found := false
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == "micro_request_length" && fields[i+1] == 5 {
			found = true
		}
	}
	if !found {
		t.Errorf("micro_request_length=5 not in fields: %v", fields)
	}
}

func TestHandlerRespErrDroppedCharacterization(t *testing.T) {
	t.Skip("known: respErr from r.Error/r.Respond is dropped at endpointer.go:62-64,87 (assigned to local respErr, never returned/logged); desired: surface to caller or log via requestLogger")
}

// AddEndpoint tests

func TestAddEndpointSuccessRecordsCall(t *testing.T) {
	g := &fakeGroup{}
	fn := func(ctx context.Context, r any) (any, error) { return "ok", nil }
	dec := func(b []byte) (any, error) { return "x", nil }
	if err := AddEndpoint("subj", g, fn, WithDecoderFn(dec)); err != nil {
		t.Fatalf("AddEndpoint err: %v", err)
	}
	if len(g.calls) != 1 {
		t.Fatalf("want 1 call, got %d", len(g.calls))
	}
	c := g.calls[0]
	if c.name != "subj" {
		t.Errorf("name: want subj, got %q", c.name)
	}
	if c.handler == nil {
		t.Error("handler must be non-nil")
	}
	if len(c.opts) != 1 {
		t.Errorf("opts: want 1 (metadata), got %d", len(c.opts))
	}
}

func TestAddEndpointMissingDecoderReturnsError(t *testing.T) {
	g := &fakeGroup{}
	fn := func(ctx context.Context, r any) (any, error) { return "ok", nil }
	err := AddEndpoint("subj", g, fn)
	if err == nil || err.Error() != "decoder function cannot be nil" {
		t.Errorf("want \"decoder function cannot be nil\", got %v", err)
	}
	if len(g.calls) != 0 {
		t.Errorf("group must not be called on opt error")
	}
}

func TestAddEndpointOptionErrorPropagates(t *testing.T) {
	g := &fakeGroup{}
	fn := func(ctx context.Context, r any) (any, error) { return "ok", nil }
	err := AddEndpoint("subj", g, fn, WithDecoderFn(nil))
	if err == nil || err.Error() != "nil decoder" {
		t.Errorf("want \"nil decoder\", got %v", err)
	}
	if len(g.calls) != 0 {
		t.Errorf("group must not be called on opt error")
	}
}

func TestAddEndpointGroupErrorPropagates(t *testing.T) {
	errBoom := errors.New("group boom")
	g := &fakeGroup{addErr: errBoom}
	fn := func(ctx context.Context, r any) (any, error) { return "ok", nil }
	dec := func(b []byte) (any, error) { return "x", nil }
	err := AddEndpoint("subj", g, fn, WithDecoderFn(dec))
	if !errors.Is(err, errBoom) {
		t.Errorf("want errBoom, got %v", err)
	}
}

func TestAddEndpointWithSchemaPassesMetadata(t *testing.T) {
	g := &fakeGroup{}
	fn := func(ctx context.Context, r any) (any, error) { return "ok", nil }
	dec := func(b []byte) (any, error) { return "x", nil }
	type schemaReq struct{ A string }
	type schemaRes struct{ B int }
	err := AddEndpoint("subj", g, fn,
		WithDecoderFn(dec),
		WithJsonSchema(jsonschema.Reflector{}, &schemaReq{}, &schemaRes{}),
	)
	if err != nil {
		t.Fatalf("AddEndpoint err: %v", err)
	}
	if len(g.calls) != 1 {
		t.Fatalf("want 1 call, got %d", len(g.calls))
	}
	if len(g.calls[0].opts) != 1 {
		t.Errorf("opts: want 1 (metadata), got %d", len(g.calls[0].opts))
	}
}
