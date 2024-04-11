package projection

import (
	"context"
	"errors"

	"github.com/modernice/goes/projection"
)

var (
	ErrNotFound = errors.New("projection not found")
)

func Use[T ProgressAwareTarget](
	j projection.Job,
	newFn func(context.Context) (T, error),
	getFn func(context.Context, string) (T, error),
	saveFn func(context.Context, T) error,
	useFn func(context.Context, T) error) error {
	n, err := newFn(j)
	if err != nil {
		return err
	}

	v, err := getFn(j, n.Key())
	notFound := errors.Is(err, ErrNotFound)
	if err != nil && !notFound {
		return err
	}

	if notFound {
		v = n
	}

	err = useFn(j, v)
	if err != nil {
		return err
	}

	if !v.Dirty() {
		return nil
	}

	return saveFn(j, v)
}
