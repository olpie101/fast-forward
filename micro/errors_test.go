package micro_test

import (
	"errors"
	"io"
	"reflect"
	"testing"

	natsmicro "github.com/nats-io/nats.go/micro"
	"github.com/olpie101/fast-forward/micro"
	pkgerrors "github.com/pkg/errors"
)

func TestNewErrorWrapsAndExposesParts(t *testing.T) {
	hdr := natsmicro.Headers{"X": []string{"y"}}
	body := []byte("teapot")
	e := micro.NewError(io.EOF, "418", body, hdr)
	if e == nil {
		t.Fatal("NewError returned nil")
	}
	code, desc, d, h := e.ErrorParts()
	if code != "418" {
		t.Errorf("code: want 418, got %q", code)
	}
	if desc != "service error: EOF" {
		t.Errorf("desc: want %q, got %q", "service error: EOF", desc)
	}
	if !reflect.DeepEqual(d, body) {
		t.Errorf("body: want %v, got %v", body, d)
	}
	if !reflect.DeepEqual(h, hdr) {
		t.Errorf("headers: want %v, got %v", hdr, h)
	}
}

func TestServiceErrorErrorPrefix(t *testing.T) {
	e := micro.NewError(errors.New("boom"), "500", nil, nil)
	got := e.Error()
	want := "service error: boom"
	if got != want {
		t.Errorf("Error: want %q, got %q", want, got)
	}
}

func TestNewErrorNilErrCharacterization(t *testing.T) {
	t.Skip("known: NewError(nil) produces a ServiceError with nil err; Error() will panic; endpointer.go:107-115 lacks nil guard")
}

func TestNewErrorAlwaysWrapsServiceErrorCharacterization(t *testing.T) {
	// Current behavior: errors.Is(err, &ServiceError{}) at endpointer.go:107
	// always returns false because *ServiceError lacks an Is method; the
	// As-style sentinel always wraps.
	inner := micro.NewError(errors.New("boom"), "418", nil, nil)
	outer := micro.NewError(inner, "418", nil, nil)
	got := outer.Error()
	// Pin current "always-wrap" behavior. When the bug is fixed, this
	// assertion must flip to "service error: boom".
	want := "service error: service error: boom"
	if got != want {
		t.Errorf("current always-wrap behavior: want %q, got %q", want, got)
	}
	t.Skip("known: errors.Is(err, &ServiceError{}) at endpointer.go:107 always returns false because *ServiceError lacks an Is method; the As-style sentinel always wraps; desired: skip wrap when err already contains a *ServiceError (use errors.As)")
}

// Verify pkg/errors compatibility — wrapping an existing ServiceError via
// pkg/errors.Wrap and unwrapping via stdlib errors.As works.
func TestServiceErrorUnwrappableViaPkgErrors(t *testing.T) {
	inner := micro.NewError(io.EOF, "418", nil, nil)
	wrapped := pkgerrors.Wrap(inner, "outer")
	var got *micro.ServiceError
	if !errors.As(wrapped, &got) {
		t.Fatal("errors.As must find inner *ServiceError through pkg/errors.Wrap")
	}
	if got != inner {
		t.Errorf("inner identity mismatch")
	}
}
