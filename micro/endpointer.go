package micro

import (
	"context"

	"github.com/invopop/jsonschema"
	"github.com/nats-io/nats.go/micro"
)

type EndpointAttacher func(context.Context, jsonschema.Reflector, micro.Group) error

type Endpointer interface {
	Endpoints() []EndpointAttacher
}
