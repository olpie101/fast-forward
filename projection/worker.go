package projection

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/modernice/goes/codec"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/helper/streams"
	"github.com/modernice/goes/projection"
	"go.uber.org/zap"
)

type ContextKey string

var (
	ContextKeyJobName = ContextKey("name")
	ContextKeyId      = ContextKey("id")
	ContextKeyRunId   = ContextKey("run-id")
)

type Starter interface {
	Start(context.Context) error
}

type Worker struct {
	logger     *zap.SugaredLogger
	projectSvc *projection.Service
	projector  Projector
	running    bool
	onError    func(projection.Job, error)
}

var defaultOnErrorFunc = func(projection.Job, error) {}

type WorkerOption func(*Worker)

func New(projector Projector, bus event.Bus, reg *codec.Registry, opts ...WorkerOption) *Worker {
	worker := &Worker{
		logger:     zap.NewNop().Sugar(),
		projectSvc: projection.NewService(bus),
		projector:  projector,
		running:    false,
		onError:    defaultOnErrorFunc,
	}
	for _, v := range opts {
		v(worker)
	}
	// projector.schedules = projector.projection.WithSchedules(bus, store)
	projection.RegisterService(reg)
	return worker
}

func (svc *Worker) Start(ctx context.Context, opts ...projection.SubscribeOption) (<-chan error, error) {
	// svc.projection.Register()
	svc.running = true
	var projErrsArr []<-chan error
	for name, s := range svc.projector.Schedules() {
		jobId := uuid.New()
		logger := svc.logger.With("job_name", name, "job_id", jobId)
		svc.projectSvc.Register(name, s.Schedule)
		ctx = context.WithValue(ctx, ContextKeyJobName, name)
		ctx = context.WithValue(ctx, ContextKeyId, jobId)

		errs, err := s.Subscribe(ctx, svc.projector.HandleJob)
		if err != nil {
			return nil, err
		}

		errs = streams.Map(
			ctx,
			errs,
			func(e error) error {
				logger.Errorw("projection err", "err", err)
				return fmt.Errorf("projection error occurred: %w", e)
			},
		)

		projErrsArr = append(projErrsArr, errs)
	}

	triggerErrs, err := svc.projectSvc.Run(ctx)
	if err != nil {
		return nil, err
	}

	projErrs := streams.FanInContext(ctx, projErrsArr...)

	triggerErrs = streams.Map(
		ctx,
		triggerErrs,
		func(e error) error {
			svc.logger.Errorw("trigger err", "err", err)
			return fmt.Errorf("projection trigger error occurred: %w", e)
		},
	)

	starter, isStarter := svc.projector.(Starter)
	if isStarter {
		err := starter.Start(ctx)
		if err != nil {
			return nil, err
		}
	}

	return streams.FanInContext(ctx, triggerErrs, projErrs), nil
}
