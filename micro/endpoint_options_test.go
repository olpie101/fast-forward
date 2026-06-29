package micro

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/invopop/jsonschema"
	natsmicro "github.com/nats-io/nats.go/micro"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type ctxK struct{}

type optReq struct{ A string }
type optRes struct{ B int }

func TestDefaultEndpointOptions(t *testing.T) {
	e := defaultEndpointOptions(nil)
	if e == nil {
		t.Fatal("nil endpointOpts")
	}
	if e.metadata == nil || len(e.metadata) != 0 {
		t.Errorf("metadata: want non-nil empty, got %v", e.metadata)
	}
	if e.timeout != 10*time.Second {
		t.Errorf("timeout: want 10s, got %v", e.timeout)
	}
	if e.ctx == nil {
		t.Error("ctx must be non-nil")
	}
	if e.ctxFn == nil {
		t.Fatal("ctxFn must be non-nil")
	}
	got := e.ctxFn(context.Background(), "subj", nil)
	if got != context.Background() {
		t.Errorf("default ctxFn must be pass-through")
	}
	if e.errFn == nil {
		t.Fatal("errFn must be non-nil")
	}
	c, d, b, h := e.errFn(errAsErr("x"))
	if c != "" || d != "x" || b != nil || h != nil {
		t.Errorf("default errFn unexpected: (%q,%q,%v,%v)", c, d, b, h)
	}
	if e.logger == nil {
		t.Error("logger must be non-nil")
	}
	if e.middlewares == nil || len(e.middlewares) != 0 {
		t.Errorf("middlewares: want non-nil empty")
	}
	if e.decFn != nil || e.encFn != nil {
		t.Error("decFn/encFn must be nil by default")
	}
}

type errStr string

func (e errStr) Error() string { return string(e) }

func errAsErr(s string) error { return errStr(s) }

func TestWithTimeoutSetsDuration(t *testing.T) {
	e := defaultEndpointOptions(nil)
	if err := WithTimeout(2 * time.Second)(e); err != nil {
		t.Fatalf("WithTimeout err: %v", err)
	}
	if e.timeout != 2*time.Second {
		t.Errorf("timeout: want 2s, got %v", e.timeout)
	}
}

func TestDefaultEndpointOptionsConcurrencyIsOne(t *testing.T) {
	e := defaultEndpointOptions(nil)
	if e.maxInFlight != 1 {
		t.Errorf("default maxInFlight: want 1 (inline serial), got %d", e.maxInFlight)
	}
}

func TestWithConcurrencySetsField(t *testing.T) {
	for _, n := range []int{0, 1, 4} {
		e := defaultEndpointOptions(nil)
		if err := WithConcurrency(n)(e); err != nil {
			t.Fatalf("WithConcurrency(%d) err: %v", n, err)
		}
		if e.maxInFlight != n {
			t.Errorf("WithConcurrency(%d): maxInFlight = %d", n, e.maxInFlight)
		}
	}
}

func TestWithConcurrencyNegativeReturnsError(t *testing.T) {
	e := defaultEndpointOptions(nil)
	original := e.maxInFlight
	err := WithConcurrency(-1)(e)
	if err == nil || err.Error() != "concurrency cannot be negative" {
		t.Errorf("want \"concurrency cannot be negative\", got %v", err)
	}
	if e.maxInFlight != original {
		t.Errorf("maxInFlight must be unchanged on error: want %d, got %d", original, e.maxInFlight)
	}
}

func TestWithContextSetsContext(t *testing.T) {
	e := defaultEndpointOptions(nil)
	ctx := context.WithValue(context.Background(), ctxK{}, "v")
	if err := WithContext(ctx)(e); err != nil {
		t.Fatalf("WithContext err: %v", err)
	}
	if e.ctx != ctx {
		t.Errorf("ctx identity mismatch")
	}
}

func TestWithContextFnNilReturnsError(t *testing.T) {
	e := defaultEndpointOptions(nil)
	defaultFn := e.ctxFn
	err := WithContextFn(nil)(e)
	if err == nil || err.Error() != "nil context func" {
		t.Errorf("want \"nil context func\", got %v", err)
	}
	// ctxFn unchanged
	if reflect.ValueOf(e.ctxFn).Pointer() != reflect.ValueOf(defaultFn).Pointer() {
		t.Errorf("ctxFn must be unchanged on nil error")
	}
}

func TestWithContextFnSetsFn(t *testing.T) {
	e := defaultEndpointOptions(nil)
	called := false
	fn := func(ctx context.Context, subj string, h natsmicro.Headers) context.Context {
		called = true
		return ctx
	}
	if err := WithContextFn(fn)(e); err != nil {
		t.Fatalf("WithContextFn err: %v", err)
	}
	e.ctxFn(context.Background(), "subj", nil)
	if !called {
		t.Errorf("ctxFn was not invoked")
	}
}

func TestWithDecoderFnNilReturnsError(t *testing.T) {
	e := defaultEndpointOptions(nil)
	err := WithDecoderFn(nil)(e)
	if err == nil || err.Error() != "nil decoder" {
		t.Errorf("want \"nil decoder\", got %v", err)
	}
}

func TestWithDecoderFnSetsFn(t *testing.T) {
	e := defaultEndpointOptions(nil)
	fn := func(b []byte) (any, error) { return string(b), nil }
	if err := WithDecoderFn(fn)(e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if e.decFn == nil {
		t.Error("decFn nil after Set")
	}
}

func TestWithEncoderFnNilReturnsError(t *testing.T) {
	e := defaultEndpointOptions(nil)
	err := WithEncoderFn(nil)(e)
	if err == nil || err.Error() != "nil encoder" {
		t.Errorf("want \"nil encoder\", got %v", err)
	}
}

func TestWithEncoderFnSetsFn(t *testing.T) {
	e := defaultEndpointOptions(nil)
	fn := func(v any) ([]byte, error) { return []byte("x"), nil }
	if err := WithEncoderFn(fn)(e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if e.encFn == nil {
		t.Error("encFn nil after Set")
	}
}

func TestWithErrFnNilReturnsError(t *testing.T) {
	e := defaultEndpointOptions(nil)
	err := WithErrFn(nil)(e)
	if err == nil || err.Error() != "nil err fn" {
		t.Errorf("want \"nil err fn\", got %v", err)
	}
}

func TestWithErrFnSetsFn(t *testing.T) {
	e := defaultEndpointOptions(nil)
	fn := func(error) (string, string, []byte, natsmicro.Headers) { return "", "", nil, nil }
	if err := WithErrFn(fn)(e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if e.errFn == nil {
		t.Error("errFn nil after Set")
	}
}

func TestWithMiddlewaresAppendsInOrder(t *testing.T) {
	e := defaultEndpointOptions(nil)
	mw := func(label string) Middleware {
		return func(h Handler) Handler {
			return func(ctx context.Context, r any) (any, error) { return label, nil }
		}
	}
	m1, m2, m3 := mw("a"), mw("b"), mw("c")
	if err := WithMiddlewares(m1, m2)(e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := WithMiddlewares(m3)(e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(e.middlewares) != 3 {
		t.Fatalf("want 3 mws, got %d", len(e.middlewares))
	}
	// Verify order via behavioral identity (each mw returns a distinct label).
	for i, want := range []string{"a", "b", "c"} {
		got, _ := e.middlewares[i](nil)(context.Background(), nil)
		if got != want {
			t.Errorf("mw[%d]: want %q, got %v", i, want, got)
		}
	}
}

func TestWithLoggerOddArgsReturnsError(t *testing.T) {
	e := defaultEndpointOptions(nil)
	original := e.logger
	err := WithLogger(zap.NewNop().Sugar(), "odd")(e)
	if err == nil || err.Error() != "logger fields has odd number of entries" {
		t.Errorf("want odd-entries error, got %v", err)
	}
	if e.logger != original {
		t.Errorf("logger must be unchanged on error")
	}
}

func TestWithLoggerSetsLoggerWithFields(t *testing.T) {
	e := defaultEndpointOptions(nil)
	core, obs := observer.New(zap.InfoLevel)
	base := zap.New(core).Sugar()
	if err := WithLogger(base, "k1", 1, "k2", 2)(e); err != nil {
		t.Fatalf("err: %v", err)
	}
	e.logger.Info("msg")
	entries := obs.All()
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	cm := entries[0].ContextMap()
	if v, ok := cm["k1"]; !ok || v != int64(1) {
		t.Errorf("k1: want 1, got %v", cm["k1"])
	}
	if v, ok := cm["k2"]; !ok || v != int64(2) {
		t.Errorf("k2: want 2, got %v", cm["k2"])
	}
}

func TestWithLoggerRequestFieldsOddReturnsError(t *testing.T) {
	e := defaultEndpointOptions(nil)
	err := WithLoggerRequestFields("odd")(e)
	if err == nil || err.Error() != "logger request fields has odd number of entries" {
		t.Errorf("want odd-entries error, got %v", err)
	}
}

func TestWithLoggerRequestFieldsSetsArgs(t *testing.T) {
	e := defaultEndpointOptions(nil)
	if err := WithLoggerRequestFields("k", "v")(e); err != nil {
		t.Fatalf("err: %v", err)
	}
	want := []interface{}{"k", "v"}
	if !reflect.DeepEqual(e.loggerRequestFields, want) {
		t.Errorf("want %v, got %v", want, e.loggerRequestFields)
	}
}

func TestWithJsonSchemaPopulatesMetadata(t *testing.T) {
	e := defaultEndpointOptions(nil)
	if err := WithJsonSchema(jsonschema.Reflector{}, &optReq{}, &optRes{})(e); err != nil {
		t.Fatalf("err: %v", err)
	}
	raw, ok := e.metadata["schema"]
	if !ok || raw == "" {
		t.Fatalf("schema metadata missing or empty")
	}
	var s struct {
		Request, Response string
	}
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("schema not valid JSON: %v", err)
	}
	var req map[string]any
	if err := json.Unmarshal([]byte(s.Request), &req); err != nil {
		t.Fatalf("Request not valid JSON: %v", err)
	}
	if req["title"] != "optReq" {
		t.Errorf("Request title: want optReq, got %v", req["title"])
	}
}

func TestWithJsonSchemaRebuildsNilMetadata(t *testing.T) {
	e := defaultEndpointOptions(nil)
	e.metadata = nil
	if err := WithJsonSchema(jsonschema.Reflector{}, &optReq{}, &optRes{})(e); err != nil {
		t.Fatalf("err: %v", err)
	}
	if e.metadata == nil {
		t.Fatal("metadata must be re-created on nil")
	}
	if _, ok := e.metadata["schema"]; !ok {
		t.Error("schema key missing after rebuild")
	}
}
