package projection

import (
	"context"

	"github.com/modernice/goes/event"
	"github.com/modernice/goes/projection"
)

type Scheduler struct {
	Schedule       projection.Schedule
	StartupOptions []projection.SubscribeOption
}

func (s *Scheduler) Subscribe(ctx context.Context, handler func(projection.Job) error) (<-chan error, error) {
	return s.Schedule.Subscribe(ctx, handler, s.StartupOptions...)
}

type Schedules = map[string]*Scheduler

type Registerer interface {
	// RegisterEventHandler registers an event handler for the given event name.
	RegisterEventHandler(eventName string, handler func(event.Event) error)
}

type Projector interface {
	Schedules() Schedules
	HandleJob(projection.Job) error
}
