package nats

import (
	"context"

	"github.com/google/uuid"
	"github.com/modernice/goes/persistence/model"
	"github.com/nats-io/nats.go"
)

var _ model.Repository[model.Model[uuid.UUID], uuid.UUID] = &ModelRepository[model.Model[uuid.UUID], uuid.UUID]{}

// ModelRepository is a Nats KV backed model repository.
type ModelRepository[Model model.Model[ID], ID model.ID] struct {
	kv nats.KeyValue
}

// Delete implements model.Repository
func (*ModelRepository[Model, ID]) Delete(ctx context.Context, a Model) error {
	panic("unimplemented")
}

// Fetch implements model.Repository
func (*ModelRepository[Model, ID]) Fetch(ctx context.Context, id ID) (Model, error) {
	panic("unimplemented")
}

// Save implements model.Repository
func (*ModelRepository[Model, ID]) Save(ctx context.Context, a Model) error {
	panic("unimplemented")
}

// Use implements model.Repository
func (*ModelRepository[Model, ID]) Use(ctx context.Context, id ID, fn func(Model) error) error {
	panic("unimplemented")
}
