package micro

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go/micro"
)

type ContextKey string

var (
	ContextKeyUserId   = ContextKey("userId")
	ContextKeyUserName = ContextKey("name")
	ContextKeyUserOrg  = ContextKey("org")
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

func WithUserID(h func(ctx context.Context, r micro.Request)) func(ctx context.Context, r micro.Request) {
	return func(ctx context.Context, r micro.Request) {
		uid := r.Headers().Get("userId")
		id, _ := uuid.Parse(uid)
		ctx = context.WithValue(ctx, ContextKeyUserId, id)
		h(ctx, r)
	}
}

func WithToken(h ErrorableMicroRequest) ErrorableMicroRequest {
	return func(ctx context.Context, r micro.Request) error {
		token := r.Headers().Get("token")
		claims, err := jwt.DecodeUserClaims(token)
		if err != nil {
			return err
		}
		uid, err := uuid.Parse(claims.Name)
		if err != nil {
			return err
		}
		ctx = context.WithValue(ctx, ContextKeyUserId, uid)
		for _, tag := range claims.Tags {
			if strings.HasPrefix(tag, "name") {
				parts := strings.Split(tag, ":")
				ctx = context.WithValue(ctx, ContextKeyUserName, parts[1])
			}

			if strings.HasPrefix(tag, "org") {
				parts := strings.Split(tag, ":")
				ctx = context.WithValue(ctx, ContextKeyUserOrg, parts[1])
			}
		}
		return h(ctx, r)
	}
}

func UserID(ctx context.Context) uuid.UUID {
	uid, _ := ctx.Value(ContextKeyUserId).(uuid.UUID)
	return uid
}
