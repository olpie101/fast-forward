package projection

import (
	"context"
	"fmt"
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

func PeriodicJobTickerFunc(ctx context.Context, d time.Duration, s projection.Schedule, tickFn func(context.Context) ([]query.Option, error), opts ...query.Option) <-chan error {
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
				extraOpts, err := tickFn(ctx)

				if err != nil {
					errs <- fmt.Errorf("job tick func error: %w", err)
					continue
				}

				allOpts := append(mergedOpts, extraOpts...)

				err = s.Trigger(
					ctx,
					projection.Query(
						query.New(
							allOpts...,
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
