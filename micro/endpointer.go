package micro

import (
	"context"

	"github.com/invopop/jsonschema"
	"github.com/nats-io/nats.go/micro"
)

type EndpointAttacher func(context.Context, jsonschema.Reflector, micro.Group) error
type EndpointAttacherSet []EndpointAttacher

func (e EndpointAttacherSet) Endpoints() []EndpointAttacher {
	return e
}

type Endpointer interface {
	Endpoints() []EndpointAttacher
}
