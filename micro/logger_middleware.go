package micro

import (
	"context"
)

// LoggerFieldsProvider is a function that extracts logger fields from context
type LoggerFieldsProvider func(context.Context) []any

// loggerFieldsContextKey is the context key for storing logger fields
type loggerFieldsContextKey struct{}

func WithLoggerFields(ctx context.Context, fields ...any) context.Context {
	return context.WithValue(ctx, loggerFieldsContextKey{}, fields)
}

// LoggerFieldsFromContext extracts logger fields from context
func LoggerFieldsFromContext(ctx context.Context) []any {
	if fields, ok := ctx.Value(loggerFieldsContextKey{}).([]any); ok {
		return fields
	}
	return nil
}
