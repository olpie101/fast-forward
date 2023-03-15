package errorable

import (
	"fmt"

	"github.com/modernice/goes/event"
)

// Base can be embedded into projections to implement event.Handler.
type Base struct {
	appliers      map[string]func(event.Event) error
	maxRetries    uint
	retryableFunc func(error) bool
}

type BaseOption func(*Base)

func WithRetries(maxRetries uint) BaseOption {
	return func(b *Base) {
		b.maxRetries = maxRetries
	}
}

func WithRetryableFunc(fn func(error) bool) BaseOption {
	return func(b *Base) {
		b.retryableFunc = fn
	}
}

// New returns a new base for a projection. Use the RegisterHandler function to add
func New(opts ...BaseOption) *Base {
	b := &Base{
		appliers:   make(map[string]func(event.Event) error),
		maxRetries: 0,
		retryableFunc: func(error) bool {
			return false
		},
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// RegisterEventHandler implements event.Handler.
func (a *Base) RegisterEventHandler(eventName string, handler func(event.Event) error) {
	a.appliers[eventName] = handler
}

// ApplyEvent implements eventApplier.
func (a *Base) ApplyEvent(evt event.Event) {
	fmt.Println("exec handler")
	if handler, ok := a.appliers[evt.Name()]; ok {
		handler(evt)
	}
}
