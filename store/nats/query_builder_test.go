package nats

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query"
	"github.com/modernice/goes/event/query/version"
	"github.com/nats-io/nats.go/jetstream"
)

func TestBuildQuery(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	otherID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	tests := []struct {
		name string
		q    event.Query
		want []string
	}{
		{
			name: "no filters",
			q:    query.New(),
			want: []string{"es.*.*.*.*"},
		},
		{
			name: "aggregate name",
			q:    query.New(query.AggregateName("order")),
			want: []string{"es.order.*.*.*"},
		},
		{
			name: "aggregate id without aggregate name",
			q:    query.New(query.AggregateID(id)),
			want: []string{"es.*." + id.String() + ".*.*"},
		},
		{
			name: "aggregate name and id",
			q:    query.New(query.AggregateName("order"), query.AggregateID(id)),
			want: []string{"es.order." + id.String() + ".*.*"},
		},
		{
			name: "aggregates merge names and ids without duplicates",
			q: query.New(
				query.AggregateName("order"),
				query.AggregateID(id),
				query.Aggregates(
					event.AggregateRef{Name: "order", ID: id},
					event.AggregateRef{Name: "invoice", ID: otherID},
				),
			),
			want: []string{
				"es.order." + id.String() + ".*.*",
				"es.order." + otherID.String() + ".*.*",
				"es.invoice." + id.String() + ".*.*",
				"es.invoice." + otherID.String() + ".*.*",
			},
		},
		{
			name: "exact versions sorted and unique",
			q: query.New(
				query.AggregateName("order"),
				query.AggregateID(id),
				query.AggregateVersion(version.Exact(3, 1, 3)),
			),
			want: []string{
				"es.order." + id.String() + ".1.*",
				"es.order." + id.String() + ".3.*",
			},
		},
		{
			name: "version ranges expand inclusively and overlap with exact remains unique",
			q: query.New(
				query.AggregateName("order"),
				query.AggregateID(id),
				query.AggregateVersion(version.Exact(3), version.InRange(version.Range{2, 4})),
			),
			want: []string{
				"es.order." + id.String() + ".2.*",
				"es.order." + id.String() + ".3.*",
				"es.order." + id.String() + ".4.*",
			},
		},
		{
			name: "event names are normalized",
			q: query.New(
				query.AggregateName("order"),
				query.AggregateID(id),
				query.Name("order.created.v2"),
			),
			want: []string{"es.order." + id.String() + ".*.order_created_v2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s Store
			got, err := s.buildQuery(tt.q)
			if err != nil {
				t.Fatalf("buildQuery() error = %v", err)
			}
			assertStringsEqual(t, got, tt.want)
		})
	}
}

func TestNormaliseEventName(t *testing.T) {
	got := normaliseEventName("order.created.v2")
	want := "order_created_v2"
	if got != want {
		t.Fatalf("normaliseEventName() = %q, want %q", got, want)
	}
}

func TestSubjectFunc(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	got, err := subjectFunc("order", id, 7, "order.created.v2")
	if err != nil {
		t.Fatalf("subjectFunc() error = %v", err)
	}
	want := "es.order." + id.String() + ".7.order_created_v2"
	if got != want {
		t.Fatalf("subjectFunc() = %q, want %q", got, want)
	}
}

func assertStringsEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d\ngot:  %#v\nwant: %#v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %q, want %q\ngot:  %#v\nwant: %#v", i, got[i], want[i], got, want)
		}
	}
}

func TestOrderedConsumerConfig(t *testing.T) {
	const streamSubject = "es.order.>"
	const threshold = 30 * time.Second

	t.Run("zero startTime leaves DeliverAll defaults", func(t *testing.T) {
		cfg := orderedConsumerConfig(
			[]string{"es.order." + uuid.New().String() + ".1.*"},
			time.Time{},
			streamSubject,
			threshold,
		)
		if cfg.DeliverPolicy != jetstream.DeliverAllPolicy {
			t.Errorf("DeliverPolicy = %v, want DeliverAllPolicy (zero value)", cfg.DeliverPolicy)
		}
		if cfg.OptStartTime != nil {
			t.Errorf("OptStartTime = %v, want nil", cfg.OptStartTime)
		}
		if len(cfg.FilterSubjects) != 1 {
			t.Fatalf("len(FilterSubjects) = %d, want 1", len(cfg.FilterSubjects))
		}
		if cfg.InactiveThreshold != threshold {
			t.Errorf("InactiveThreshold = %v, want %v", cfg.InactiveThreshold, threshold)
		}
	})

	t.Run("non-zero startTime selects DeliverByStartTimePolicy", func(t *testing.T) {
		start := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
		cfg := orderedConsumerConfig(
			[]string{"es.order.*.*.*"},
			start,
			streamSubject,
			threshold,
		)
		if cfg.DeliverPolicy != jetstream.DeliverByStartTimePolicy {
			t.Errorf("DeliverPolicy = %v, want DeliverByStartTimePolicy", cfg.DeliverPolicy)
		}
		if cfg.OptStartTime == nil {
			t.Fatal("OptStartTime is nil")
		}
		if !cfg.OptStartTime.Equal(start) {
			t.Errorf("OptStartTime = %v, want %v", *cfg.OptStartTime, start)
		}
		if cfg.InactiveThreshold != threshold {
			t.Errorf("InactiveThreshold = %v, want %v", cfg.InactiveThreshold, threshold)
		}
	})

	t.Run("FilterSubjects normalised against stream subject", func(t *testing.T) {
		// normaliseSubject substitutes the stream-subject's aggregate-name
		// segment when the query subject differs. Pass a query subject with
		// a different aggregate-name segment to exercise the substitution.
		id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
		querySubjects := []string{
			"es.invoice." + id.String() + ".*.*",
			"es.invoice." + id.String() + ".5.*",
		}
		cfg := orderedConsumerConfig(querySubjects, time.Time{}, "es.order.>", threshold)
		want := []string{
			"es.order." + id.String() + ".*.*",
			"es.order." + id.String() + ".5.*",
		}
		assertStringsEqual(t, cfg.FilterSubjects, want)
	})
}
