package projection

import (
	"go.uber.org/zap"
)

func WithProjector(projector Projector) WorkerOption {
	return func(w *Worker) {
		w.projection = projector
	}
}

func WithLogger(logger *zap.SugaredLogger) WorkerOption {
	return func(w *Worker) {
		w.logger = logger
	}
}

func WithSchedules(schedules Schedules) WorkerOption {
	return func(w *Worker) {
		w.schedules = schedules
	}
}
