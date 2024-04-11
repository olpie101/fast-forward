package projection

import (
	"context"
	"time"

	"github.com/modernice/goes/event/query"
	"github.com/modernice/goes/projection"
)

func PeriodicJobTicker(ctx context.Context, d time.Duration, s projection.Schedule, opts ...query.Option) <-chan error {
	t := time.NewTicker(d)

	mergedOpts := []query.Option{
		query.SortByTime(),
	}

	mergedOpts = append(mergedOpts, opts...)
	errs := make(chan error)
	go func() {
		for {
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
				err := s.Trigger(
					ctx,
					projection.Query(
						query.New(
							mergedOpts...,
						),
					),
				)
				if err != nil {
					errs <- err
				}
			}

		}
	}()

	return errs
}
