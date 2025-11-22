package micro

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// LoggerFieldsProvider is a function that extracts logger fields from context
type LoggerFieldsProvider func(context.Context) []any

// loggerFieldsContextKey is the context key for storing logger fields
type loggerFieldsContextKey struct{}

func WithLoggerFields(ctx context.Context, fields ...any) context.Context {
	existingFields := LoggerFieldsFromContext(ctx)
	existingFields = append(existingFields, fields...)
	return context.WithValue(ctx, loggerFieldsContextKey{}, existingFields)
}

// LoggerFieldsFromContext extracts logger fields from context
func LoggerFieldsFromContext(ctx context.Context) []any {
	if fields, ok := ctx.Value(loggerFieldsContextKey{}).([]any); ok {
		return fields
	}
	return []any{}
}

func requestLogger(logger *zap.SugaredLogger, encFn EncoderFunc, errFn ErrorFunc) Middleware {
	return func(h Handler) Handler {
		return func(ctx context.Context, r any) (any, error) {
			start := time.Now()
			resp, err := h(ctx, r)
			defer func() {
				// Get dynamic fields from context
				dynamicFields := LoggerFieldsFromContext(ctx)
				allFields := dynamicFields

				if err != nil {
					code, desc, d, _ := wrappedErrorFn(err, errFn)
					respLen := len(d)
					logger.With(allFields...).Errorw("complete", "d", time.Since(start), "micro_response_length", respLen, "code", code, "err", err, "micro_error_response", desc)

					return
				}

				b, _ := encFn(resp)
				respLen := len(b)
				logger.With(allFields...).Infow("complete", "d", time.Since(start), "micro_response_length", respLen, "code", 200)
			}()
			return resp, err
		}

	}
}
