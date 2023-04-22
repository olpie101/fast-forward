package projection

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/modernice/goes/codec"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/projection"
	"go.uber.org/zap"
)

type ContextKey string

var (
	ContextKeyJobName = ContextKey("name")
	ContextKeyId      = ContextKey("id")
	ContextKeyRunId   = ContextKey("run-id")
)

type Worker struct {
	logger     *zap.SugaredLogger
	schedules  Schedules
	projectSvc *projection.Service
	projection Projector
	running    bool
	onError    func(projection.Job, error)
}

var defaultOnErrorFunc = func(projection.Job, error) {}

type WorkerOption func(*Worker)

func New(bus event.Bus, store event.Store, reg *codec.Registry, opts ...WorkerOption) *Worker {
	projector := &Worker{
		logger:     zap.NewNop().Sugar(),
		schedules:  map[string]projection.Schedule{},
		projectSvc: projection.NewService(bus),
		projection: nil,
		running:    false,
		onError:    defaultOnErrorFunc,
	}
	for _, v := range opts {
		v(projector)
	}
	projector.schedules = projector.projection.WithSchedules(bus, store)
	projection.RegisterService(reg)
	return projector
}

func (svc *Worker) Start(ctx context.Context, opts ...projection.SubscribeOption) (<-chan error, error) {
	// svc.projection.Register()
	svc.running = true
	errs := make(chan error)
	for name, s := range svc.schedules {
		jobId := uuid.New()
		logger := svc.logger.With("job_name", name, "job_id", jobId)
		svc.projectSvc.Register(name, s)
		ctx = context.WithValue(ctx, ContextKeyJobName, name)
		ctx = context.WithValue(ctx, ContextKeyId, jobId)
		perrs, err := s.Subscribe(
			ctx,
			svc.projection.HandleJob,
			opts...,
		)
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
					if err == nil {
						continue
					}
					logger.Errorw("projection trigger err", "err", err)
					errs <- fmt.Errorf("trigger error occurred: %w", e)
				case e := <-projErrs:
					if err == nil {
						continue
					}
					logger.Errorw("projection err", "err", err)
					errs <- fmt.Errorf("proj error occurred: %w", e)
				}
			}
		}(errs, c, perrs)
	}
	return errs, nil
}
