package micro

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/micro"
)

type ErrorableMicroRequest func(ctx context.Context, r micro.Request) error

func WithErrorableRequest(h ErrorableMicroRequest) func(ctx context.Context, r micro.Request) {
	return func(ctx context.Context, r micro.Request) {
		err := h(ctx, r)
		if err != nil {
			r.Error("500", err.Error(), nil)
			return
		}
	}
}

func WithContext(timeout time.Duration, h func(ctx context.Context, r micro.Request)) func(ctx context.Context, r micro.Request) {
	return func(ctx context.Context, r micro.Request) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		h(ctx, r)
	}
}
