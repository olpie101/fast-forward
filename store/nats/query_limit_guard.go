package nats

import (
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/helper/pick"
	"golang.org/x/exp/slices"
)

type limitGuard struct {
	minVersionGuard func(e event.Event) bool
	maxVersionGuard func(e event.Event) bool
	minTimeGuard    func(e event.Event) bool
	maxTimeGuard    func(e event.Event) bool
}

func (g limitGuard) guard(e event.Event) bool {
	return g.minTimeGuard(e) && g.maxTimeGuard(e) && g.minVersionGuard(e) && g.maxVersionGuard(e)
}

func newLimitGuard(q event.Query) limitGuard {
	guard := limitGuard{
		minVersionGuard: func(e event.Event) bool { return true },
		maxVersionGuard: func(e event.Event) bool { return true },
		minTimeGuard:    func(e event.Event) bool { return true },
		maxTimeGuard:    func(e event.Event) bool { return true },
	}

	if q.AggregateVersions() != nil {
		if len(q.AggregateVersions().Min()) > 0 {
			min := slices.Min(q.AggregateVersions().Min())
			guard.minVersionGuard = func(e event.Event) bool {
				return pick.AggregateVersion(e) >= min
			}
		}
		if len(q.AggregateVersions().Max()) > 0 {
			max := slices.Min(q.AggregateVersions().Max())
			guard.maxVersionGuard = func(e event.Event) bool {
				return pick.AggregateVersion(e) <= max
			}
		}
	}

	if q.Times() != nil {
		if !q.Times().Min().IsZero() {
			guard.minTimeGuard = func(e event.Event) bool {
				return e.Time().After(q.Times().Min())
			}
		}
		if !q.Times().Max().IsZero() {
			guard.maxTimeGuard = func(e event.Event) bool {
				return e.Time().Before(q.Times().Max())
			}
		}
	}

	return guard
}
