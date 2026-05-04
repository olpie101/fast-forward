package micro

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/nats-io/nats.go"
	natsmicro "github.com/nats-io/nats.go/micro"
	"github.com/olpie101/fast-forward/projection"
	pkgerrors "github.com/pkg/errors"
)

func TestWrappedErrorFnExtractsServiceError(t *testing.T) {
	hdr := natsmicro.Headers{"K": []string{"v"}}
	body := []byte("body")
	err := NewError(io.EOF, "418", body, hdr)

	// errFn must NOT be consulted when err contains a *ServiceError.
	called := false
	errFn := func(error) (string, string, []byte, natsmicro.Headers) {
		called = true
		return "", "", nil, nil
	}
	code, desc, d, h := wrappedErrorFn(err, errFn)
	if called {
		t.Errorf("errFn must not be called for ServiceError input")
	}
	if code != "418" || desc != "service error: EOF" {
		t.Errorf("got (%q, %q), want (418, service error: EOF)", code, desc)
	}
	if !reflect.DeepEqual(d, body) || !reflect.DeepEqual(h, hdr) {
		t.Errorf("body/headers mismatch: %v %v", d, h)
	}
}

func TestWrappedErrorFnUnwrapsThroughPkgErrors(t *testing.T) {
	inner := NewError(io.EOF, "418", nil, nil)
	wrapped := pkgerrors.Wrap(inner, "outer")
	code, _, _, _ := wrappedErrorFn(wrapped, nil)
	if code != "418" {
		t.Errorf("unwrapped code: want 418, got %q", code)
	}
}

func TestWrappedErrorFnUsesCustomErrFnWhenCodeNonEmpty(t *testing.T) {
	errFn := func(error) (string, string, []byte, natsmicro.Headers) {
		return "499", "custom", []byte("c"), nil
	}
	code, desc, d, _ := wrappedErrorFn(errors.New("plain"), errFn)
	if code != "499" || desc != "custom" || string(d) != "c" {
		t.Errorf("got (%q, %q, %q)", code, desc, string(d))
	}
}

func TestWrappedErrorFnFallsBackToDefaultWhenCodeEmpty(t *testing.T) {
	errFn := func(error) (string, string, []byte, natsmicro.Headers) {
		return "", "ignored", nil, nil
	}
	code, desc, _, _ := wrappedErrorFn(errors.New("plain"), errFn)
	if code != "500" || desc != "an unknown error occurred" {
		t.Errorf("got (%q, %q)", code, desc)
	}
}

func TestDefaultErrorFunc(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		code         string
		desc         string
	}{
		{"deadline exceeded", context.DeadlineExceeded, "408", "request timed out"},
		{"canceled", context.Canceled, "408", "request timed out"},
		{"wrapped deadline", fmt.Errorf("wrap: %w", context.DeadlineExceeded), "408", "request timed out"},
		{"nats key not found", nats.ErrKeyNotFound, "404", "not found"},
		{"projection not found", projection.ErrNotFound, "404", "not found"},
		{"decoding error", ErrDecodingError, "400", "invalid request"},
		{"unknown", errors.New("anything else"), "500", "an unknown error occurred"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, desc, d, h := defaultErrorFunc(tc.err)
			if code != tc.code {
				t.Errorf("code: want %q, got %q", tc.code, code)
			}
			if desc != tc.desc {
				t.Errorf("desc: want %q, got %q", tc.desc, desc)
			}
			if d != nil {
				t.Errorf("data must be nil, got %v", d)
			}
			if h != nil {
				t.Errorf("headers must be nil, got %v", h)
			}
		})
	}
}
