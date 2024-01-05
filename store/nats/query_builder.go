package nats

import (
	"fmt"

	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query/version"
	"golang.org/x/exp/slices"
)

func (s *Store) buildQuery(q event.Query) ([]string, error) {
	subjects := buildEventNameQuery(
		buildAggregateVersionsQuery(
			buildAggregateIdsQuery(
				buildAggregatesQuery(q),
				q,
			),
			q.AggregateVersions(),
		),
		q.Names(),
	)
	return subjects, nil
}

func buildAggregatesQuery(q event.Query) []string {
	names := q.AggregateNames()

	for _, r := range q.Aggregates() {
		if !slices.Contains(names, r.Name) {
			names = append(names, r.Name)
		}
	}

	if len(names) == 0 {
		return []string{"es.*"}
	}

	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, fmt.Sprintf("es.%s", name))
	}
	return out
}

func buildAggregateIdsQuery(aggQueries []string, q event.Query) []string {
	ids := q.AggregateIDs()
	if len(ids) == 0 {
		for i := 0; i < len(aggQueries); i++ {
			aggQueries[i] = fmt.Sprintf("%s.*", aggQueries[i])
		}
		return aggQueries
	}

	out := make([]string, 0, len(aggQueries)*len(ids))
	for _, aggName := range aggQueries {
		for _, id := range ids {
			out = append(out, fmt.Sprintf("%s.%s", aggName, id))
		}
	}

	return out
}

func buildAggregateVersionsQuery(idQueries []string, versions version.Constraints) []string {
	vers := versions.Exact()
	ranges := versions.Ranges()
	rangeNums := make([]int, 0, len(ranges)*2)
	for _, v := range ranges {
		start, end := v[0], v[1]
		for i := start; i < end+1; i++ {
			rangeNums = append(rangeNums, i)
		}
	}
	vers = append(vers, rangeNums...)
	vers = unique(vers)
	slices.Sort(vers)
	if len(vers) == 0 {
		for i := 0; i < len(idQueries); i++ {
			idQueries[i] = fmt.Sprintf("%s.*", idQueries[i])
		}
		return idQueries
	}

	out := make([]string, 0, len(idQueries)*len(vers))
	for _, id := range idQueries {
		for _, v := range vers {
			out = append(out, fmt.Sprintf("%s.%d", id, v))
		}
	}

	return out
}

func buildEventNameQuery(versionQueries []string, names []string) []string {

	if len(names) == 0 {
		for i := 0; i < len(versionQueries); i++ {
			versionQueries[i] = fmt.Sprintf("%s.*", versionQueries[i])
		}
		return versionQueries
	}

	out := make([]string, 0, len(versionQueries)*len(names))
	for _, aggName := range versionQueries {
		for _, name := range names {
			out = append(out, fmt.Sprintf("%s.%s", aggName, normaliseEventName(name)))
		}
	}

	return out
}

func unique[T comparable](s []T) []T {
	inResult := make(map[T]struct{})
	var result []T
	for _, str := range s {
		if _, ok := inResult[str]; !ok {
			inResult[str] = struct{}{}
			result = append(result, str)
		}
	}
	return result
}
