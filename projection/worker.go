package projection

import (
	"context"

	"github.com/modernice/goes/codec"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/projection"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Worker struct {
	logger     *zap.SugaredLogger
	schedules  Schedules
	projectSvc *projection.Service
	projection Projector
	running    bool
}

type WorkerOption func(*Worker)

func New(bus event.Bus, store event.Store, reg *codec.Registry, opts ...WorkerOption) *Worker {
	projector := &Worker{
		logger:     zap.NewNop().Sugar(),
		schedules:  map[string]projection.Schedule{},
		projectSvc: projection.NewService(bus),
		projection: nil,
		running:    false,
	}
	for _, v := range opts {
		v(projector)
	}
	projector.schedules = projector.projection.WithSchedules(bus, store)
	projection.RegisterService(reg)
	return projector
}

func (svc *Worker) Start(ctx context.Context) (<-chan error, error) {
	// svc.projection.Register()
	svc.running = true
	errs := make(chan error)
	for name, s := range svc.schedules {
		svc.projectSvc.Register(name, s)
		perrs, err := s.Subscribe(ctx, func(j projection.Job) error {
			svc.logger.Infow("proj")
			return j.Apply(ctx, svc.projection)
		})
		if err != nil {
			return nil, err
		}

		c, err := svc.projectSvc.Run(ctx)
		if err != nil {
			return nil, err
		}
		go func(errs chan error, triggerErrs <-chan error, projErrs <-chan error) {
			for {
				select {
				case e := <-triggerErrs:
					errs <- errors.WithMessage(e, "trigger error occured")
				case e := <-projErrs:
					errs <- errors.WithMessage(e, "proj error occured")

				}
			}
		}(errs, c, perrs)
	}
	return errs, nil
}
