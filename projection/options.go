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

// JobHandler handles one projection job (Projector.HandleJob).
type JobHandler func(projection.Job) error

// JobMiddleware wraps the job handler of the worker's schedule called name,
// e.g. to trace or log each job: it returns a handler that calls next. The
// job it passes to next may be replaced (e.g. by one carrying a span in its
// context); the projector then sees that job.
type JobMiddleware func(name string, next JobHandler) JobHandler

// WithJobMiddleware wraps every job the worker handles with mw. The first
// middleware is the outermost: WithJobMiddleware(a, b) runs a(b(HandleJob)).
// Repeated options append.
func WithJobMiddleware(mw ...JobMiddleware) WorkerOption {
	return func(w *Worker) {
		w.jobMiddleware = append(w.jobMiddleware, mw...)
	}
}
