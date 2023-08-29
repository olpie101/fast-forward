package projection

import (
	"github.com/modernice/goes/projection"
	"go.uber.org/zap"
)

func WithLogger(logger *zap.SugaredLogger) WorkerOption {
	return func(w *Worker) {
		w.logger = logger
	}
}

func WithErrorFunc(fn func(projection.Job, error)) WorkerOption {
	return func(w *Worker) {
		w.onError = fn
	}
}
