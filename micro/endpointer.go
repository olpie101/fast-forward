package micro

import (
	"context"
	"encoding/json"
	"time"

	"github.com/invopop/jsonschema"
	"github.com/nats-io/nats.go/micro"
	"github.com/pkg/errors"
)

type EndpointAttacher func(context.Context, micro.Group) error
type EndpointAttacherSet []EndpointAttacher

func (e EndpointAttacherSet) Endpoints() []EndpointAttacher {
	return e
}

type Endpointer interface {
	Endpoints() []EndpointAttacher
}

type EndpointOption func(*endpointOpts) error

type DecoderFunc func(b []byte) (any, error)
type EncoderFunc func(v any) ([]byte, error)
type ErrorFunc func(err error) (string, string, []byte, micro.Headers)
type ContextFunc func(context.Context, string, micro.Headers) context.Context

type endpointOpts struct {
	metadata    map[string]string
	timeout     time.Duration
	fn          Handler
	ctx         context.Context
	ctxFn       ContextFunc
	decFn       DecoderFunc
	encFn       EncoderFunc
	errFn       ErrorFunc
	middlewares []Middleware
}

func (o *endpointOpts) Handler() micro.Handler {
	fn := o.fn
	for _, mw := range o.middlewares {
		fn = mw(fn)
	}
	return micro.ContextHandler(o.ctx, func(ctx context.Context, r micro.Request) {
		ctx, cancel := handlerCtx(ctx, o.timeout)
		defer cancel()

		req, err := o.decFn(r.Data())
		if err != nil {
			wrapError(r, err, o.errFn)
			return
		}

		if err != nil {
			wrapError(r, err, o.errFn)
			return
		}

		res, err := fn(ctx, req)
		if err != nil {
			wrapError(r, err, o.errFn)
			return
		}

		b, err := o.encFn(res)

		if err != nil {
			wrapError(r, err, o.errFn)
			return
		}

		err = r.Respond(b)

		if err != nil {
			wrapError(r, err, o.errFn)
			return
		}
	})

}

func handlerCtx(ctx context.Context, t time.Duration) (context.Context, context.CancelFunc) {
	if t < time.Millisecond {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, t)
}

func wrapError(r micro.Request, err error, fn ErrorFunc) {
	code, desc, d, h := fn(err)

	r.Error(code, desc, d, micro.WithHeaders(h))
	// TODO: should take a logger of some kind to log this message
}

func defaultEndpointOptions(fn Handler) *endpointOpts {
	return &endpointOpts{
		metadata: map[string]string{},
		timeout:  10 * time.Second,
		fn:       fn,
		ctx:      context.Background(),
		ctxFn: func(ctx context.Context, s string, h micro.Headers) context.Context {
			return ctx
		},
		encFn: func(v any) ([]byte, error) {
			return json.Marshal(v)
		},
		errFn: func(err error) (string, string, []byte, micro.Headers) {
			return "500", err.Error(), nil, nil
		},
		middlewares: make([]Middleware, 0),
	}
}

func AddEndpoint(subject string, group micro.Group, fn Handler, opts ...EndpointOption) error {
	endPntOpt := defaultEndpointOptions(fn)
	for _, o := range opts {
		err := o(endPntOpt)
		if err != nil {
			return err
		}
	}
	if endPntOpt.decFn == nil {
		return errors.New("decoder function cannot be nil")
	}

	err := group.AddEndpoint(
		subject,
		endPntOpt.Handler(),
		micro.WithEndpointMetadata(endPntOpt.metadata),
	)
	return err
}

func WithJsonSchema(reflector jsonschema.Reflector, req interface{}, res interface{}) EndpointOption {
	return func(e *endpointOpts) error {
		if e.metadata == nil {
			e.metadata = make(map[string]string)
		}
		schema, err := generateJsonSchema(reflector, res, req)
		if err != nil {
			return errors.Wrap(err, "unable to generate schema")
		}
		out, err := json.Marshal(schema)
		if err != nil {
			return errors.Wrap(err, "unable to generate schema json")
		}
		e.metadata["schema"] = string(out)
		return nil
	}
}

func WithTimeout(t time.Duration) EndpointOption {
	return func(e *endpointOpts) error {
		e.timeout = t
		return nil
	}
}

func WithContext(ctx context.Context) EndpointOption {
	return func(e *endpointOpts) error {
		e.ctx = ctx
		return nil
	}
}

func WithContextFn(fn ContextFunc) EndpointOption {
	return func(e *endpointOpts) error {
		if fn == nil {
			return errors.New("nil context func")
		}

		e.ctxFn = fn
		return nil
	}
}

func WithDecoderFn(fn DecoderFunc) EndpointOption {
	return func(e *endpointOpts) error {
		if fn == nil {
			return errors.New("nil decoder")
		}

		e.decFn = fn
		return nil
	}
}

func WithEncoderFn(fn EncoderFunc) EndpointOption {
	return func(e *endpointOpts) error {
		if fn == nil {
			return errors.New("nil encoder")
		}

		e.encFn = fn
		return nil
	}
}

func WithErrFn(fn ErrorFunc) EndpointOption {
	return func(e *endpointOpts) error {
		if fn == nil {
			return errors.New("nil err fn")
		}

		e.errFn = fn
		return nil
	}
}

func WithMiddlewares(mws ...Middleware) EndpointOption {
	return func(e *endpointOpts) error {
		e.middlewares = mws
		return nil
	}
}
