package projection

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/modernice/goes/event"
	"github.com/modernice/goes/helper/streams"
	"golang.org/x/exp/maps"
)

func Filter[T ProgressAwareTarget](t T, evts []event.Event) ([]event.Event, error) {
	progress, ids := t.Progress()
	return streams.All(
		streams.Filter(
			streams.New(evts),
			func(evt event.Event) bool {
				if evt.Time().Before(progress) {
					return false
				}

				if evt.Time().After(progress) {
					return true
				}

				return !slices.Contains(ids, evt.ID())
			},
		),
	)
}

type Group[K any, V any] struct {
	Key   K
	Value []V
}

func GroupByFunc[K comparable, V any](ctx context.Context, groupFn func(V) (K, error), in <-chan V, errs ...<-chan error) ([]Group[K, V], error) {
	errChan, stop := streams.FanIn(errs...)
	defer stop()
	m := make(map[K][]V)

	for {
		if in == nil && errChan == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err, ok := <-errChan:
			if ok {
				return nil, err
			}
			errChan = nil
		case el, ok := <-in:
			if !ok {
				in = nil
				break
			}
			k, err := groupFn(el)
			if err != nil {
				return nil, err
			}
			e := m[k]
			e = append(e, el)
			m[k] = e
		}
	}

	groups := make([]Group[K, V], 0, len(m))
	for k, v := range m {
		groups = append(
			groups,
			Group[K, V]{
				Key:   k,
				Value: v,
			},
		)
	}
	return groups, nil
}

func Deduplicate[T comparable](in <-chan T, window time.Duration) <-chan T {
	out := make(chan T, 10)
	go func() {
		tmp := make(map[T]struct{}, 10)
		t := time.NewTimer(window)
		t.Stop()

		push := func() {
			vals := maps.Keys(tmp)
			for _, v := range vals {
				out <- v
			}
			clear(tmp)
			t.Stop()
		}

		defer func() {
			push()
			close(out)
			t.Stop()
		}()

		var v T
		var ok bool
		for {
			select {
			case v, ok = <-in:
				if !ok {
					return
				}
				previousLen := len(tmp)
				tmp[v] = struct{}{}
				// start timer on first value
				if previousLen == 0 {
					t.Reset(window)
				}
			case <-t.C:
				fmt.Println("tick", len(tmp))
				push()
			}
		}
	}()
	return out
}
