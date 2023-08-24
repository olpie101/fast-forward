package micro

import (
	"context"
)

type Handler func(ctx context.Context, req any) (any, error)
type Middleware func(Handler) Handler
