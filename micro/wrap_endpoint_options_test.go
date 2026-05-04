package micro

import (
	"context"
	"reflect"
	"testing"
	"time"

	natsmicro "github.com/nats-io/nats.go/micro"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// applyAll applies a slice of EndpointOption to a fresh endpointOpts.
func applyAll(t *testing.T, opts []EndpointOption) *endpointOpts {
	t.Helper()
	e := defaultEndpointOptions(nil)
	for i, o := range opts {
		if err := o(e); err != nil {
			t.Fatalf("opt[%d] err: %v", i, err)
		}
	}
	return e
}

// markerMW returns a middleware that records its label when invoked.
func markerMW(label string, sink *[]string) Middleware {
	return func(h Handler) Handler {
		return func(ctx context.Context, r any) (any, error) {
			*sink = append(*sink, label)
			if h != nil {
				return h(ctx, r)
			}
			return label, nil
		}
	}
}

func TestWrapEndpointOptionsApplied(t *testing.T) {
	core, obs := observer.New(zap.InfoLevel)
	logger := zap.New(core).Sugar()

	stubEnc := func(v any) ([]byte, error) { return []byte("E"), nil }
	stubErr := func(error) (string, string, []byte, natsmicro.Headers) { return "", "", nil, nil }

	var sink []string
	mwA := markerMW("A", &sink)
	mwB := markerMW("B", &sink)

	mo := MicroEndpointConfig{
		encFn:        stubEnc,
		errFn:        stubErr,
		logger:       logger,
		loggerFields: []any{"svc", "x"},
		mws:          []Middleware{mwA, mwB},
	}

	user := []EndpointOption{WithTimeout(7 * time.Second)}
	opts := WrapEndpointOptions(mo, "subj", user)
	e := applyAll(t, opts)

	// encFn / errFn identity
	if reflect.ValueOf(e.encFn).Pointer() != reflect.ValueOf(stubEnc).Pointer() {
		t.Errorf("encFn identity mismatch")
	}
	if reflect.ValueOf(e.errFn).Pointer() != reflect.ValueOf(stubErr).Pointer() {
		t.Errorf("errFn identity mismatch")
	}

	// logger should carry "svc=x" — emit and observe
	e.logger.Info("hello")
	entries := obs.All()
	if len(entries) != 1 {
		t.Fatalf("want 1 log entry, got %d", len(entries))
	}
	cm := entries[0].ContextMap()
	if cm["svc"] != "x" {
		t.Errorf("svc field: want x, got %v", cm["svc"])
	}

	// loggerRequestFields ordering
	wantFields := []interface{}{"endpoint", "subj", "svc", "x"}
	if !reflect.DeepEqual(e.loggerRequestFields, wantFields) {
		t.Errorf("loggerRequestFields: want %v, got %v", wantFields, e.loggerRequestFields)
	}

	// middlewares: requestLogger + mwA + mwB = 3
	if len(e.middlewares) != 3 {
		t.Fatalf("want 3 mws, got %d", len(e.middlewares))
	}
	// index 1, 2 should be mwA, mwB (identity via pointer compare)
	if reflect.ValueOf(e.middlewares[1]).Pointer() != reflect.ValueOf(mwA).Pointer() {
		t.Errorf("mw[1] identity != mwA")
	}
	if reflect.ValueOf(e.middlewares[2]).Pointer() != reflect.ValueOf(mwB).Pointer() {
		t.Errorf("mw[2] identity != mwB")
	}
	// index 0 should be requestLogger — invoke through it and verify a log entry with key "d".
	rlMW := e.middlewares[0]
	wrapped := rlMW(func(ctx context.Context, r any) (any, error) { return "ok", nil })
	_, _ = wrapped(context.Background(), nil)
	entries = obs.All()
	// First entry was the "hello" we logged above; subsequent entry is from requestLogger.
	if len(entries) < 2 {
		t.Fatalf("want >=2 log entries, got %d", len(entries))
	}
	last := entries[len(entries)-1]
	if last.Message != "complete" {
		t.Errorf("last log: want message 'complete', got %q", last.Message)
	}
	if _, ok := last.ContextMap()["d"]; !ok {
		t.Errorf("requestLogger entry must include key 'd'")
	}

	// User option appended last — overrides timeout default.
	if e.timeout != 7*time.Second {
		t.Errorf("user option override: timeout want 7s, got %v", e.timeout)
	}
}

func TestWrapEndpointOptionsEmptyMwsAndFields(t *testing.T) {
	logger := zap.NewNop().Sugar()
	stubEnc := func(v any) ([]byte, error) { return nil, nil }
	stubErr := func(error) (string, string, []byte, natsmicro.Headers) { return "", "", nil, nil }
	mo := MicroEndpointConfig{
		encFn:        stubEnc,
		errFn:        stubErr,
		logger:       logger,
		loggerFields: nil,
		mws:          nil,
	}
	opts := WrapEndpointOptions(mo, "subj", nil)
	e := applyAll(t, opts)
	if len(e.middlewares) != 1 {
		t.Errorf("only requestLogger expected, got %d mws", len(e.middlewares))
	}
	want := []interface{}{"endpoint", "subj"}
	if !reflect.DeepEqual(e.loggerRequestFields, want) {
		t.Errorf("loggerRequestFields: want %v, got %v", want, e.loggerRequestFields)
	}
}
