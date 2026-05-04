package micro_test

import (
	"context"
	"errors"
	"testing"

	"github.com/olpie101/fast-forward/micro"
)

func TestWrapTypedEndpointHappyPath(t *testing.T) {
	called := 0
	fn := func(ctx context.Context, s string) (int, error) {
		called++
		return len(s), nil
	}
	h := micro.WrapTypedEndpoint[string, int](fn)
	got, err := h(context.Background(), "hello")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != 5 {
		t.Errorf("want 5, got %v", got)
	}
	if called != 1 {
		t.Errorf("inner fn call count: want 1, got %d", called)
	}
}

func TestWrapTypedEndpointWrongTypeReturnsErrDecodingError(t *testing.T) {
	called := 0
	fn := func(ctx context.Context, s string) (int, error) {
		called++
		return 0, nil
	}
	h := micro.WrapTypedEndpoint[string, int](fn)
	got, err := h(context.Background(), 123)
	if !errors.Is(err, micro.ErrDecodingError) {
		t.Errorf("want ErrDecodingError, got %v", err)
	}
	if got != nil {
		t.Errorf("want nil result on type mismatch, got %v", got)
	}
	if called != 0 {
		t.Errorf("inner fn must not be called on type mismatch, count=%d", called)
	}
}

func TestWrapTypedEndpointInnerErrorPropagated(t *testing.T) {
	errBoom := errors.New("boom")
	fn := func(ctx context.Context, s string) (int, error) { return 0, errBoom }
	h := micro.WrapTypedEndpoint[string, int](fn)
	got, err := h(context.Background(), "hi")
	if !errors.Is(err, errBoom) {
		t.Errorf("want errBoom, got %v", err)
	}
	if got != 0 {
		t.Errorf("want zero int, got %v", got)
	}
}

type wrapFoo struct{ V int }

func TestWrapTypedEndpointTypedNilPointerSucceeds(t *testing.T) {
	called := 0
	fn := func(ctx context.Context, r *wrapFoo) (int, error) {
		called++
		if r == nil {
			return -1, nil
		}
		return r.V, nil
	}
	h := micro.WrapTypedEndpoint[*wrapFoo, int](fn)
	got, err := h(context.Background(), (*wrapFoo)(nil))
	if err != nil {
		t.Fatalf("typed nil should pass type assertion, got err=%v", err)
	}
	if got != -1 {
		t.Errorf("want -1, got %v", got)
	}
	if called != 1 {
		t.Errorf("fn must be called for typed nil, count=%d", called)
	}
}

func TestWrapTypedEndpointAnyAcceptsAnyInput(t *testing.T) {
	fn := func(ctx context.Context, r any) (int, error) { return 1, nil }
	h := micro.WrapTypedEndpoint[any, int](fn)
	if _, err := h(context.Background(), 123); err != nil {
		t.Errorf("any-typed: err=%v", err)
	}
	if _, err := h(context.Background(), "str"); err != nil {
		t.Errorf("any-typed: err=%v", err)
	}
}
