package micro

import (
	"encoding/json"

	"github.com/nats-io/nats.go/micro"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type MicroOption func(*microOpts) error

type microOpts struct {
	cfg    micro.Config
	s      micro.Service
	encFn  EncoderFunc
	errFn  ErrorFunc
	logger *zap.SugaredLogger
	mws    []Middleware
}

type MicroEndpointConfig struct {
	encFn        EncoderFunc
	errFn        ErrorFunc
	logger       *zap.SugaredLogger
	loggerFields []any
	mws          []Middleware
}

func MicroOptions(cfg micro.Config) *microOpts {
	return &microOpts{
		cfg:    cfg,
		logger: zap.NewNop().Sugar(),
		encFn:  JsonEncoderFunc,
		errFn: func(err error) (string, string, []byte, micro.Headers) {
			return "", "", nil, nil
		},
	}
}

// func (mo *microOpts) EndpointOptions() []EndpointOption {
// 	return []EndpointOption{
// 		WithLogger(mo.logger),
// 	}
// }

func (mo *microOpts) WithService(s micro.Service) {
	mo.s = s
}

func (mo *microOpts) MicroConfig() micro.Config {
	if mo.cfg.ErrorHandler == nil {
		mo.cfg.ErrorHandler = func(s micro.Service, n *micro.NATSError) {
			el := mo.logger.With(mo.loggerFields()...)
			el.Errorw(
				"nats micro error",
				"err", n.Error(),
				"micro_subject", n.Subject,
				"micro_description", n.Description,
			)
		}
	}

	if mo.cfg.DoneHandler == nil {
		mo.cfg.DoneHandler = func(s micro.Service) {
			dl := mo.logger.With(mo.loggerFields()...)
			dl.Infow("nats micro done handler")
		}
	}
	return mo.cfg
}

func (mo *microOpts) EndpointConfig() MicroEndpointConfig {
	return MicroEndpointConfig{
		encFn:        mo.encFn,
		errFn:        mo.errFn,
		logger:       mo.logger,
		loggerFields: mo.loggerFields(),
		mws:          mo.mws,
	}
}

func WithMicroLogger(logger *zap.SugaredLogger) MicroOption {
	return func(mo *microOpts) error {
		if logger == nil {
			return errors.New("logger cannot be nil")
		}
		mo.logger = logger
		return nil
	}
}

func WithMicroMiddlewares(mws ...Middleware) MicroOption {
	return func(mo *microOpts) error {
		mo.mws = mws
		return nil
	}
}

func WithPrependMiddlewares(mws ...Middleware) MicroOption {
	return func(mo *microOpts) error {
		mo.mws = append(mws, mo.mws...)
		return nil
	}
}

func WithEncoderFunc(fn EncoderFunc) MicroOption {
	return func(mo *microOpts) error {
		if fn == nil {
			return errors.New("encoder function cannot be nil")
		}
		mo.encFn = fn
		return nil
	}
}

func WithErrorFunc(fn ErrorFunc) MicroOption {
	return func(mo *microOpts) error {
		if fn == nil {
			return errors.New("error function cannot be nil")
		}
		mo.errFn = fn
		return nil
	}
}

func (mo *microOpts) loggerFields() []any {
	if mo.s == nil {
		return loggerFieldsFromCfg(mo.cfg)
	}
	return loggerFieldsFromService(mo.s)
}

func loggerFieldsFromCfg(cfg micro.Config) []any {
	return []any{
		"micro_name", cfg.Name,
		"micro_base_path", cfg.Metadata["base-path"],
		"micro_version", cfg.Version,
	}
}

func loggerFieldsFromService(s micro.Service) []any {
	info := s.Info()
	return []any{
		"micro_name", info.Name,
		"micro_base_path", info.Metadata["base-path"],
		"micro_id", info.ID,
		"micro_version", info.Version,
	}
}

func JsonEncoderFunc(v any) ([]byte, error) {
	return json.Marshal(v)
}
