package nats

import (
	"testing"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query"
)

func TestCanStreamReplay(t *testing.T) {
	aggID := uuid.New().String()
	concreteSubject := "es.order." + aggID + ".*.*"
	versionAscSort := query.New(query.SortBy(event.SortAggregateVersion, event.SortAsc))

	tests := []struct {
		name     string
		q        event.Query
		subjects []string
		groups   map[string][]string
		want     bool
	}{
		{
			name:     "single concrete aggregate version asc",
			q:        versionAscSort,
			subjects: []string{concreteSubject},
			groups:   map[string][]string{"es_order": {concreteSubject}},
			want:     true,
		},
		{
			name:     "multiple concrete versions same aggregate",
			q:        versionAscSort,
			subjects: []string{"es.order." + aggID + ".1.*", "es.order." + aggID + ".2.*"},
			groups:   map[string][]string{"es_order": {"es.order." + aggID + ".1.*", "es.order." + aggID + ".2.*"}},
			want:     true,
		},
		{
			name:     "no sort falls back",
			q:        query.New(),
			subjects: []string{concreteSubject},
			groups:   map[string][]string{"es_order": {concreteSubject}},
			want:     false,
		},
		{
			name:     "descending sort falls back",
			q:        query.New(query.SortBy(event.SortAggregateVersion, event.SortDesc)),
			subjects: []string{concreteSubject},
			groups:   map[string][]string{"es_order": {concreteSubject}},
			want:     false,
		},
		{
			name:     "sort by time falls back",
			q:        query.New(query.SortBy(event.SortTime, event.SortAsc)),
			subjects: []string{concreteSubject},
			groups:   map[string][]string{"es_order": {concreteSubject}},
			want:     false,
		},
		{
			name: "multi-key sort falls back",
			q: query.New(query.SortByMulti(
				event.SortOptions{Sort: event.SortAggregateVersion, Dir: event.SortAsc},
				event.SortOptions{Sort: event.SortTime, Dir: event.SortAsc},
			)),
			subjects: []string{concreteSubject},
			groups:   map[string][]string{"es_order": {concreteSubject}},
			want:     false,
		},
		{
			name:     "wildcard aggregate name falls back",
			q:        versionAscSort,
			subjects: []string{"es.*." + aggID + ".*.order_created"},
			groups:   map[string][]string{"es_order": {"es.*." + aggID + ".*.order_created"}},
			want:     false,
		},
		{
			name:     "wildcard aggregate id falls back",
			q:        versionAscSort,
			subjects: []string{"es.order.*.*.*"},
			groups:   map[string][]string{"es_order": {"es.order.*.*.*"}},
			want:     false,
		},
		{
			name:     "multiple aggregate ids falls back",
			q:        versionAscSort,
			subjects: []string{"es.order." + aggID + ".*.*", "es.order." + uuid.New().String() + ".*.*"},
			groups:   map[string][]string{"es_order": {"es.order." + aggID + ".*.*", "es.order." + uuid.New().String() + ".*.*"}},
			want:     false,
		},
		{
			name:     "multiple aggregate names falls back",
			q:        versionAscSort,
			subjects: []string{"es.order." + aggID + ".*.*", "es.invoice." + aggID + ".*.*"},
			groups:   map[string][]string{"es_order": {"es.order." + aggID + ".*.*", "es.invoice." + aggID + ".*.*"}},
			want:     false,
		},
		{
			name:     "two groups falls back",
			q:        versionAscSort,
			subjects: []string{concreteSubject},
			groups:   map[string][]string{"es_order": {concreteSubject}, "es_other": {concreteSubject}},
			want:     false,
		},
		{
			name:     "wrong subject prefix falls back",
			q:        versionAscSort,
			subjects: []string{"xx.order." + aggID + ".*.*"},
			groups:   map[string][]string{"es_order": {"xx.order." + aggID + ".*.*"}},
			want:     false,
		},
		{
			name:     "empty subjects falls back",
			q:        versionAscSort,
			subjects: nil,
			groups:   map[string][]string{"es_order": {}},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := canStreamReplay(tt.q, tt.subjects, tt.groups)
			if got != tt.want {
				t.Fatalf("canStreamReplay() = %v, want %v", got, tt.want)
			}
		})
	}
}
