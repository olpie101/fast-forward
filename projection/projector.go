package projection

import (
	"fmt"

	"github.com/modernice/goes/event"
	"github.com/modernice/goes/helper/pick"
	"github.com/modernice/goes/projection"
)

type Schedules = map[string]projection.Schedule

type ErrorableTarget[Data any] func(event.Of[Data]) error

type Registerer interface {
	// RegisterEventHandler registers an event handler for the given event name.
	RegisterEventHandler(eventName string, handler func(event.Event) error)
}

type Projector interface {
	projection.ProgressAware
	projection.Target[any]
	WithSchedules(bus event.Bus, store event.Store) Schedules
	// Register()
}

func ApplyWith[Data any](r Registerer, handler ErrorableTarget[Data], eventNames ...string) {
	for _, name := range eventNames {
		RegisterHandler(r, name, handler)
	}
}

func RegisterHandler[Data any](r Registerer, eventName string, handler ErrorableTarget[Data]) {
	r.RegisterEventHandler(eventName, func(evt event.Event) error {
		if casted, ok := event.TryCast[Data](evt); ok {
			return handler(casted)
		}

		aggregateName := "<unknown>"
		if a, ok := r.(pick.AggregateProvider); ok {
			aggregateName = pick.AggregateName(a)
		}
		var zero Data
		// TODO reformat this error
		panic(fmt.Errorf(
			"[goes/event.RegisterHandler] Cannot cast %T to %T. "+
				"You probably provided the wrong event name for this handler. "+
				"[event=%v, aggregate=%v]",
			evt.Data(), zero, eventName, aggregateName,
		))
	})
}
