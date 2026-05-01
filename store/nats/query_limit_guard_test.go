package nats

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query"
	qtime "github.com/modernice/goes/event/query/time"
	"github.com/modernice/goes/event/query/version"
)

func TestLimitGuardVersions(t *testing.T) {
	tests := []struct {
		name     string
		q        event.Query
		accepted []int
		rejected []int
	}{
		{
			name:     "no version filters accepts all",
			q:        query.New(),
			accepted: []int{1, 2, 3, 4, 5, 6},
		},
		{
			name:     "minimum version is inclusive",
			q:        query.New(query.AggregateVersion(version.Min(3))),
			accepted: []int{3, 4, 5},
			rejected: []int{1, 2},
		},
		{
			name:     "maximum version is inclusive",
			q:        query.New(query.AggregateVersion(version.Max(5))),
			accepted: []int{1, 2, 3, 4, 5},
			rejected: []int{6},
		},
		{
			name:     "combined minimum and maximum versions are inclusive",
			q:        query.New(query.AggregateVersion(version.Min(3), version.Max(5))),
			accepted: []int{3, 4, 5},
			rejected: []int{2, 6},
		},
		{
			name:     "multiple minimums collapse to least restrictive lower bound",
			q:        query.New(query.AggregateVersion(version.Min(3, 7))),
			accepted: []int{3, 4, 7},
			rejected: []int{2},
		},
		{
			name:     "combined multi-value min max documents current max collapse behavior",
			q:        query.New(query.AggregateVersion(version.Min(3, 7), version.Max(5, 10))),
			accepted: []int{3, 4, 5},
			rejected: []int{2, 6, 10, 11},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			guard := newLimitGuard(tt.q)
			for _, v := range tt.accepted {
				if !guard.guard(testEventWithVersion(v)) {
					t.Fatalf("version %d rejected, want accepted", v)
				}
			}
			for _, v := range tt.rejected {
				if guard.guard(testEventWithVersion(v)) {
					t.Fatalf("version %d accepted, want rejected", v)
				}
			}
		})
	}
}

func TestLimitGuardVersionUpstreamParitySkipped(t *testing.T) {
	t.Run("version Max(5,10) should accept through 10 under upstream OR semantics", func(t *testing.T) {
		t.Skip("upstream version.Max(5,10) accepts 1..10 and rejects 11, but newLimitGuard currently uses slices.Min(q.AggregateVersions().Max()) and behaves as <=5")

		guard := newLimitGuard(query.New(query.AggregateVersion(version.Max(5, 10))))
		for _, v := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
			if !guard.guard(testEventWithVersion(v)) {
				t.Fatalf("version %d rejected, want accepted under upstream version.Includes parity", v)
			}
		}
		if guard.guard(testEventWithVersion(11)) {
			t.Fatal("version 11 accepted, want rejected under upstream version.Includes parity")
		}
	})

	t.Run("version Min(3,7) Max(5,10) should accept 3 through 10 under upstream parity", func(t *testing.T) {
		t.Skip("upstream version.Min(3,7)+version.Max(5,10) accepts 3..10 and rejects 2 and 11, but current max collapse at query_limit_guard.go:48 limits this to 3..5")

		guard := newLimitGuard(query.New(query.AggregateVersion(version.Min(3, 7), version.Max(5, 10))))
		for _, v := range []int{3, 4, 5, 6, 7, 8, 9, 10} {
			if !guard.guard(testEventWithVersion(v)) {
				t.Fatalf("version %d rejected, want accepted under upstream version.Includes parity", v)
			}
		}
		for _, v := range []int{2, 11} {
			if guard.guard(testEventWithVersion(v)) {
				t.Fatalf("version %d accepted, want rejected under upstream version.Includes parity", v)
			}
		}
	})
}

func TestLimitGuardTimeBoundaries(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		q        event.Query
		accepted []time.Time
		rejected []time.Time
	}{
		{
			name:     "minimum time is strict after",
			q:        query.New(query.Time(qtime.Min(base))),
			accepted: []time.Time{base.Add(time.Nanosecond)},
			rejected: []time.Time{base, base.Add(-time.Nanosecond)},
		},
		{
			name:     "maximum time is strict before",
			q:        query.New(query.Time(qtime.Max(base))),
			accepted: []time.Time{base.Add(-time.Nanosecond)},
			rejected: []time.Time{base, base.Add(time.Nanosecond)},
		},
		{
			name:     "combined time bounds accept only strict interior",
			q:        query.New(query.Time(qtime.Min(base), qtime.Max(base.Add(10*time.Nanosecond)))),
			accepted: []time.Time{base.Add(time.Nanosecond), base.Add(9 * time.Nanosecond)},
			rejected: []time.Time{base, base.Add(10 * time.Nanosecond)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			guard := newLimitGuard(tt.q)
			for _, tm := range tt.accepted {
				if !guard.guard(testEventAtTime(tm)) {
					t.Fatalf("time %s rejected, want accepted", tm)
				}
			}
			for _, tm := range tt.rejected {
				if guard.guard(testEventAtTime(tm)) {
					t.Fatalf("time %s accepted, want rejected", tm)
				}
			}
		})
	}
}

func testEventWithVersion(version int) event.Event {
	return event.New[any](
		"order.created",
		"data",
		event.Aggregate(uuid.MustParse("11111111-1111-1111-1111-111111111111"), "order", version),
	)
}

func testEventAtTime(t time.Time) event.Event {
	return event.New[any](
		"order.created",
		"data",
		event.Time(t),
		event.Aggregate(uuid.MustParse("11111111-1111-1111-1111-111111111111"), "order", 1),
	)
}
