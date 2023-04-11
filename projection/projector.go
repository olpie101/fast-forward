package projection

import (
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/projection"
)

type Schedules = map[string]projection.Schedule

type Registerer interface {
	// RegisterEventHandler registers an event handler for the given event name.
	RegisterEventHandler(eventName string, handler func(event.Event) error)
}

type Projector interface {
	WithSchedules(bus event.Bus, store event.Store) Schedules
	HandleJob(projection.Job) error
}
