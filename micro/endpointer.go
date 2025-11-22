package micro

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/invopop/jsonschema"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/olpie101/fast-forward/projection"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type AttacherFunc func(ctx context.Context, mcfg MicroEndpointConfig, group micro.Group) error
type AttacherListFunc func() []AttacherFunc
type EndpointAttacher AttacherListFunc

type EndpointOption func(*endpointOpts) error

type DecoderFunc func(b []byte) (any, error)
type EncoderFunc func(v any) ([]byte, error)
type ErrorFunc func(err error) (string, string, []byte, micro.Headers)
type ContextFunc func(context.Context, string, micro.Headers) context.Context

type endpointOpts struct {
	metadata            map[string]string
	timeout             time.Duration
	fn                  Handler
	ctx                 context.Context
	ctxFn               ContextFunc
	decFn               DecoderFunc
	encFn               EncoderFunc
	errFn               ErrorFunc
	logger              *zap.SugaredLogger
	loggerRequestFields []interface{}
	middlewares         []Middleware
}

func (o *endpointOpts) Handler() micro.Handler {
	fn := o.fn
	for _, mw := range o.middlewares {
		fn = mw(fn)
	}
	return micro.ContextHandler(o.ctx, func(ctx context.Context, r micro.Request) {
		loggingFields := append(o.loggerRequestFields, "micro_request_length", len(r.Data()))
		ctx = WithLoggerFields(ctx, loggingFields...)
		ctx = o.ctxFn(ctx, r.Subject(), r.Headers())
		ctx, cancel := handlerCtx(ctx, o.timeout)
		var err error
		var respErr error
		var b []byte
		defer cancel()
		defer func() {
			if err == nil {
				return
			}
			code, desc, d, h := wrappedErrorFn(err, o.errFn)
			fmt.Println("returning error", code, desc, d, h)
			respErr = r.Error(code, desc, d, micro.WithHeaders(h))
			fmt.Println("returned error")
			_ = respErr
		}()

		req, err := o.decFn(r.Data())
		if err != nil {
			fmt.Println("returning error after  dec func")
			return
		}

		res, err := fn(ctx, req)
		if err != nil {
			fmt.Println("returning error after func")

			return
		}

		b, err = o.encFn(res)

		if err != nil {
			fmt.Println("returning error after enc func")
			return
		}

		respErr = r.Respond(b)
	})

}

func handlerCtx(ctx context.Context, t time.Duration) (context.Context, context.CancelFunc) {
	if t < time.Millisecond {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, t)
}

type ServiceError struct {
	err     error
	code    string
	body    []byte
	headers map[string][]string
}

func NewError(err error, code string, body []byte, headers map[string][]string) *ServiceError {
	if ok := errors.Is(err, &ServiceError{}); !ok {
		err = errors.Wrap(err, "service error")
	}
	return &ServiceError{
		err:     err,
		code:    code,
		body:    body,
		headers: headers,
	}
}

func (e *ServiceError) Error() string {
	return e.err.Error()
}

func (e *ServiceError) ErrorParts() (string, string, []byte, micro.Headers) {
	return e.code, e.Error(), e.body, e.headers
}

func wrappedErrorFn(err error, errFn ErrorFunc) (string, string, []byte, micro.Headers) {
	var serr *ServiceError
	ok := errors.As(err, &serr)

	if ok {
		return serr.ErrorParts()
	}
	code, desc, d, h := errFn(err)
	if code == "" {
		return defaultErrorFunc(err)
	}
	return code, desc, d, h
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
		errFn: func(err error) (string, string, []byte, micro.Headers) {
			return "", err.Error(), nil, nil
		},
		logger:      zap.NewNop().Sugar(),
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
		schema, err := generateJsonSchema(reflector, req, res)
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

func defaultErrorFunc(err error) (string, string, []byte, micro.Headers) {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "408", "request timed out", nil, nil
	} else if errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, projection.ErrNotFound) {
		return "404", "not found", nil, nil
	} else if errors.Is(err, ErrDecodingError) {
		return "400", "invalid request", nil, nil
	}
	return "500", "an unknown error occurred", nil, nil
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
		e.middlewares = append(e.middlewares, mws...)
		return nil
	}
}

func WithLogger(logger *zap.SugaredLogger, args ...interface{}) EndpointOption {
	return func(e *endpointOpts) error {
		if len(args)%2 != 0 {
			return errors.New("logger fields has odd number of entries")
		}

		e.logger = logger.With(args...)
		return nil
	}
}

func WithLoggerRequestFields(args ...interface{}) EndpointOption {
	return func(e *endpointOpts) error {
		if len(args)%2 != 0 {
			return errors.New("logger request fields has odd number of entries")
		}

		e.loggerRequestFields = args
		return nil
	}
}

func WrapEndpointOptions(mo MicroEndpointConfig, subject string, baseOptions []EndpointOption) []EndpointOption {
	loggerFields := []any{"endpoint", subject}
	loggerFields = append(loggerFields, mo.loggerFields...)
	mws := append([]Middleware{requestLogger(mo.logger, mo.encFn, mo.errFn)}, mo.mws...)
	options := append(
		[]EndpointOption{
			WithEncoderFn(mo.encFn),
			WithErrFn(mo.errFn),
			WithLogger(mo.logger, mo.loggerFields...),
			WithLoggerRequestFields(loggerFields...),
			WithMiddlewares(mws...),
		},
		baseOptions...,
	)

	return options
}

func WrapTypedEndpoint[R any, T any](fn HandlerTyped[R, T]) Handler {
	return func(ctx context.Context, r any) (any, error) {
		req, ok := r.(R)
		if !ok {
			return nil, ErrDecodingError
		}
		return fn(ctx, req)
	}
}
