package micro

import (
	"context"
	"encoding/json"
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
	maxInFlight         int
}

func (o *endpointOpts) Handler() micro.Handler {
	fn := o.fn
	for _, mw := range o.middlewares {
		fn = mw(fn)
	}

	// execute runs the decode -> handler -> encode pipeline on a per-request
	// context and returns either the encoded success payload or an error. It
	// never responds. On a normal/error return it does not cancel: the returned
	// CancelFunc is non-nil and the caller owns the cancel timing, which
	// deliberately differs between the inline and async paths. The only case
	// where execute cancels is while unwinding a panic raised after the
	// per-request context was created (decoder, handler, or encoder); it then
	// re-panics, and the caller never receives a CancelFunc.
	execute := func(ctx context.Context, r micro.Request) (context.CancelFunc, []byte, error) {
		// Copy the shared logger request fields before appending the
		// request-specific entries. Appending directly to o.loggerRequestFields
		// would alias its backing array across requests, which races once the
		// async path runs multiple requests concurrently.
		loggingFields := make([]any, 0, len(o.loggerRequestFields)+2)
		loggingFields = append(loggingFields, o.loggerRequestFields...)
		loggingFields = append(loggingFields, "micro_request_length", len(r.Data()))
		ctx = WithLoggerFields(ctx, loggingFields...)
		ctx = o.ctxFn(ctx, r.Subject(), r.Headers())
		ctx, cancel := handlerCtx(ctx, o.timeout)

		// On a normal return the caller owns cancel() so it runs after the
		// response (preserving the original inline ordering). A panic in the
		// decoder, handler, or encoder never returns to the caller, so cancel
		// it here during unwinding and re-panic to preserve propagation.
		defer func() {
			if rec := recover(); rec != nil {
				if cancel != nil {
					cancel()
				}
				panic(rec)
			}
		}()

		req, err := o.decFn(r.Data())
		if err != nil {
			return cancel, nil, err
		}

		res, err := fn(ctx, req)
		if err != nil {
			return cancel, nil, err
		}

		b, err := o.encFn(res)
		if err != nil {
			return cancel, nil, err
		}

		return cancel, b, nil
	}

	// runInline preserves the original synchronous lifecycle byte-for-byte
	// (minus the removed debug prints). It runs on the NATS delivery goroutine,
	// so its r.Error write happens-before nats.go's reqHandler reads
	// request.respondError after Handle returns; the error response is therefore
	// race-free and is reported via nats.go's built-in NumErrors/LastError stats.
	runInline := func(ctx context.Context, r micro.Request) {
		cancel, b, err := execute(ctx, r)
		defer cancel()
		if err != nil {
			code, desc, d, h := wrappedErrorFn(err, o.errFn)
			_ = r.Error(code, desc, d, micro.WithHeaders(h))
			return
		}
		_ = r.Respond(b)
	}

	// runAsync mirrors runInline but is safe to invoke from a spawned goroutine.
	// It must never call r.Error: nats.go's request.Error writes respondError
	// unconditionally (request.go:152,155) after Handle has returned, racing
	// reqHandler's read (service.go:700) for every async error. Instead it
	// replicates Error's wire format through Respond, which only writes
	// respondError on publish failure (request.go:113). Successful async error
	// publishes are therefore race-free, but are NOT counted by nats.go's
	// built-in NumErrors/LastError. The empty-arg skip mirrors request.Error's
	// guard at request.go:134-139.
	runAsync := func(ctx context.Context, r micro.Request) {
		cancel, b, err := execute(ctx, r)
		defer cancel()
		if err != nil {
			code, desc, d, h := wrappedErrorFn(err, o.errFn)
			if code == "" || desc == "" {
				return
			}
			_ = r.Respond(d,
				micro.WithHeaders(micro.Headers{
					micro.ErrorHeader:     []string{desc},
					micro.ErrorCodeHeader: []string{code},
				}),
				micro.WithHeaders(h),
			)
			return
		}
		_ = r.Respond(b)
	}

	// maxInFlight <= 1 selects the genuine inline serial path: no goroutine, no
	// semaphore, handler runs on the delivery goroutine exactly as before.
	if o.maxInFlight <= 1 {
		return micro.ContextHandler(o.ctx, runInline)
	}

	// Bounded async path. The semaphore is acquired on the delivery goroutine
	// before spawning, preserving arrival-order backpressure (but not completion
	// ordering) and capping in-flight handlers at maxInFlight. This is
	// semaphore-only by design (Option A): there is no WaitGroup and no drain.
	// Service.Stop() drains the NATS subscription but does NOT wait for these
	// goroutines, so a handler cut off mid-flight may fail its later Respond and
	// the client may time out. This does not corrupt store state because the
	// write path's OCC safeguards (writeLeaseKV lease, versionKV,
	// ErrLeaseLocked, ErrValidationVersionMismatch) make interrupted writes fail
	// safely.
	//
	// Accepted residual race: on a Respond publish failure (response exceeds the
	// server max_payload, or the connection is closing/draining) the async
	// worker writes nats.go's internal request.respondError (request.go:113)
	// after Handle has returned, racing reqHandler's read (service.go:700). The
	// user-visible effect is a dropped reply / request timeout; in a narrow
	// timing tail it can cause a torn-interface read in reqHandler (potential
	// crash) or, far more commonly, a missed NumErrors stat. Store correctness
	// is unaffected (OCC safeguards above). Eliminating this fully would require
	// publishing replies via an injected *nats.Conn (option ii / WithConn),
	// which is deliberately out of scope for this iteration.
	sem := make(chan struct{}, o.maxInFlight)
	return micro.ContextHandler(o.ctx, func(ctx context.Context, r micro.Request) {
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			runAsync(ctx, r)
		}()
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
		maxInFlight: 1,
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

// WithConcurrency selects how an endpoint processes incoming requests.
//
// maxInFlight <= 1 (the default is 1) uses the genuine inline serial path: the
// handler, middlewares, decoders, encoders, error mappers, and context
// functions run on the NATS delivery goroutine, one request at a time, exactly
// as without this option. maxInFlight > 1 enables bounded async processing:
// each request is offloaded to a goroutine with at most maxInFlight in flight
// per endpoint. Negative values are invalid.
//
// In async mode the same middleware-wrapped handler and the decode/encode/error/
// context functions are invoked concurrently and MUST be safe for concurrent
// use; thread-safety is the caller's responsibility. Async mode does not
// preserve per-endpoint request ordering and is not keyed/partitioned.
//
// Async mode also changes nats.go micro stats: built-in ProcessingTime measures
// spawn/gate time (nats.go times the inline Handle return), and async error
// responses are delivered via Respond with error headers rather than r.Error,
// so they are not counted by nats.go's built-in NumErrors/LastError. See the
// Handler documentation for the accepted shutdown and publish-failure
// limitations.
func WithConcurrency(maxInFlight int) EndpointOption {
	return func(e *endpointOpts) error {
		if maxInFlight < 0 {
			return errors.New("concurrency cannot be negative")
		}
		e.maxInFlight = maxInFlight
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
