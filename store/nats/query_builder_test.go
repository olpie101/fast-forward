package nats

import (
	"testing"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query"
	"github.com/modernice/goes/event/query/version"
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
