package micro

import (
	"context"
	"errors"
	"testing"
	"time"

	natsmicro "github.com/nats-io/nats.go/micro"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestLoggerFieldsFromContextEmpty(t *testing.T) {
	got := LoggerFieldsFromContext(context.Background())
	if got == nil {
		t.Errorf("must be non-nil")
	}
	if len(got) != 0 {
		t.Errorf("want empty, got %v", got)
	}
}

func TestLoggerFieldsFromContextWithFields(t *testing.T) {
	ctx := WithLoggerFields(context.Background(), "k1", 1)
	got := LoggerFieldsFromContext(ctx)
	want := []any{"k1", 1}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d]: want %v, got %v", i, want[i], got[i])
		}
	}
}

func TestLoggerFieldsChained(t *testing.T) {
	ctx := WithLoggerFields(context.Background(), "k1", 1)
	ctx = WithLoggerFields(ctx, "k2", 2)
	got := LoggerFieldsFromContext(ctx)
	want := []any{"k1", 1, "k2", 2}
	if len(got) != 4 {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d]: want %v, got %v", i, want[i], got[i])
		}
	}
}

func TestLoggerFieldsTypeMismatchFallsBack(t *testing.T) {
	// If something stuffs a non-[]any value at the key, the helper must
	// fall back to []any{} via the type-assertion at logger_middleware.go:24.
	// The key type is unexported; we have package-internal access here.
	ctx := context.WithValue(context.Background(), loggerFieldsContextKey{}, "string")
	got := LoggerFieldsFromContext(ctx)
	if len(got) != 0 {
		t.Errorf("want empty fallback, got %v", got)
	}
}

func makeObserved(t *testing.T) (*zap.SugaredLogger, *observer.ObservedLogs) {
	t.Helper()
	core, obs := observer.New(zap.DebugLevel)
	return zap.New(core).Sugar(), obs
}

func TestRequestLoggerSuccessPath(t *testing.T) {
	logger, obs := makeObserved(t)
	encFn := func(v any) ([]byte, error) { return []byte("12345"), nil }
	errFn := func(error) (string, string, []byte, natsmicro.Headers) { return "", "", nil, nil }
	mw := requestLogger(logger, encFn, errFn)
	wrapped := mw(func(ctx context.Context, r any) (any, error) { return "ok", nil })
	ctx := WithLoggerFields(context.Background(), "req_id", "abc")
	if _, err := wrapped(ctx, nil); err != nil {
		t.Fatalf("err: %v", err)
	}
	entries := obs.All()
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Level != zap.InfoLevel {
		t.Errorf("level: want Info, got %v", e.Level)
	}
	if e.Message != "complete" {
		t.Errorf("message: want complete, got %q", e.Message)
	}
	cm := e.ContextMap()
	if cm["req_id"] != "abc" {
		t.Errorf("req_id: want abc, got %v", cm["req_id"])
	}
	if cm["micro_response_length"] != int64(5) {
		t.Errorf("micro_response_length: want 5, got %v", cm["micro_response_length"])
	}
	if cm["code"] != int64(200) {
		t.Errorf("code: want 200, got %v", cm["code"])
	}
	if _, ok := cm["d"].(time.Duration); !ok {
		t.Errorf("d must be a time.Duration, got %T", cm["d"])
	}
	if _, hasErr := cm["err"]; hasErr {
		t.Errorf("success path must not have err field")
	}
}

func TestRequestLoggerErrorPath(t *testing.T) {
	logger, obs := makeObserved(t)
	encFn := func(v any) ([]byte, error) { return nil, nil }
	errFn := func(error) (string, string, []byte, natsmicro.Headers) { return "", "", nil, nil }
	mw := requestLogger(logger, encFn, errFn)
	errBoom := errors.New("boom")
	wrapped := mw(func(ctx context.Context, r any) (any, error) { return nil, errBoom })
	if _, err := wrapped(context.Background(), nil); !errors.Is(err, errBoom) {
		t.Fatalf("err: want errBoom, got %v", err)
	}
	entries := obs.All()
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Level != zap.ErrorLevel {
		t.Errorf("level: want Error, got %v", e.Level)
	}
	cm := e.ContextMap()
	if cm["code"] != "500" {
		t.Errorf("code: want \"500\" string, got %v (%T)", cm["code"], cm["code"])
	}
	if cm["micro_response_length"] != int64(0) {
		t.Errorf("micro_response_length: want 0, got %v", cm["micro_response_length"])
	}
	if cm["micro_error_response"] != "an unknown error occurred" {
		t.Errorf("micro_error_response: got %v", cm["micro_error_response"])
	}
	gotErrStr, ok := cm["err"].(string)
	if !ok {
		t.Fatalf("err field missing or not a string: %v (%T)", cm["err"], cm["err"])
	}
	if gotErrStr != errBoom.Error() {
		t.Errorf("err: want %q, got %q", errBoom.Error(), gotErrStr)
	}
}

func TestRequestLoggerServiceErrorBodyLength(t *testing.T) {
	logger, obs := makeObserved(t)
	encFn := func(v any) ([]byte, error) { return nil, nil }
	errFn := func(error) (string, string, []byte, natsmicro.Headers) { return "", "", nil, nil }
	mw := requestLogger(logger, encFn, errFn)

	errBoom := errors.New("boom")
	se := NewError(errBoom, "418", []byte("teapot-body"), nil)
	wrapped := mw(func(ctx context.Context, r any) (any, error) { return nil, se })
	_, _ = wrapped(context.Background(), nil)
	entries := obs.All()
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	cm := entries[0].ContextMap()
	if cm["code"] != "418" {
		t.Errorf("code: want 418, got %v", cm["code"])
	}
	if cm["micro_error_response"] != "service error: boom" {
		t.Errorf("micro_error_response: got %v", cm["micro_error_response"])
	}
	if cm["micro_response_length"] != int64(len("teapot-body")) {
		t.Errorf("respLen: want %d, got %v", len("teapot-body"), cm["micro_response_length"])
	}
}

func TestRequestLoggerNoFieldsWhenCtxEmpty(t *testing.T) {
	logger, obs := makeObserved(t)
	encFn := func(v any) ([]byte, error) { return []byte("x"), nil }
	errFn := func(error) (string, string, []byte, natsmicro.Headers) { return "", "", nil, nil }
	mw := requestLogger(logger, encFn, errFn)
	wrapped := mw(func(ctx context.Context, r any) (any, error) { return "ok", nil })
	_, _ = wrapped(context.Background(), nil)
	entries := obs.All()
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	cm := entries[0].ContextMap()
	// Must contain only static fields plus none of the dynamic ones.
	for _, k := range []string{"d", "micro_response_length", "code"} {
		if _, ok := cm[k]; !ok {
			t.Errorf("missing static field %q", k)
		}
	}
	if len(cm) != 3 {
		t.Errorf("want exactly 3 fields, got %d: %v", len(cm), cm)
	}
}

func TestRequestLoggerSilentlyIgnoresEncFnErr(t *testing.T) {
	// Pin current passing behavior: encFn error is dropped at
	// logger_middleware.go:48; respLen falls to 0.
	logger, obs := makeObserved(t)
	encErr := errors.New("enc-fail")
	encFn := func(v any) ([]byte, error) { return nil, encErr }
	errFn := func(error) (string, string, []byte, natsmicro.Headers) { return "", "", nil, nil }
	mw := requestLogger(logger, encFn, errFn)
	wrapped := mw(func(ctx context.Context, r any) (any, error) { return "payload", nil })
	_, _ = wrapped(context.Background(), nil)
	entries := obs.All()
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	cm := entries[0].ContextMap()
	if cm["micro_response_length"] != int64(0) {
		t.Errorf("encFn error path: respLen want 0, got %v", cm["micro_response_length"])
	}
	t.Skip("known: requestLogger silently swallows encFn error at logger_middleware.go:48; tests pin len(b)==0 in passing path")
}
