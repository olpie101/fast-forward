package micro

import (
	"context"
)

type Handler func(ctx context.Context, req any) (any, error)
type HandlerTyped[R any, T any] func(ctx context.Context, req R) (T, error)
type Middleware func(Handler) Handler
