package micro

import (
	"github.com/nats-io/nats.go/micro"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type MicroOption func(*microOpts) error

type microOpts struct {
	cfg    micro.Config
	errFn  ErrorFunc
	logger *zap.SugaredLogger
}

func (mo *microOpts) EndpointOptions() []EndpointOption {
	return []EndpointOption{
		WithLogger(mo.logger, "micro_name", mo.cfg.Name, "micro_base_path", mo.cfg.Metadata["base-path"]),
	}
}

func DefaultMicroOptions(cfg micro.Config) *microOpts {
	return &microOpts{
		cfg:    cfg,
		logger: zap.NewNop().Sugar(),
		errFn: func(err error) (string, string, []byte, micro.Headers) {
			return "", "", nil, nil
		},
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

func WithErrorFunc(fn ErrorFunc) MicroOption {
	return func(mo *microOpts) error {
		if fn == nil {
			return errors.New("error function cannot be nil")
		}
		mo.errFn = fn
		return nil
	}
}
