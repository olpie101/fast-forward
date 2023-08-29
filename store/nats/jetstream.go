package nats

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/codec"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query/version"
	"github.com/modernice/goes/helper/pick"
	"github.com/modernice/goes/helper/streams"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

type ContextKey string

var (
	ContextKeyAggregates = ContextKey("aggregates")
)

var (
	ErrEndOfMsgs = errors.New("end of message stream")
)

type EncodingRegisterer interface {
	codec.Encoding
	Map() map[string]func() any
}

type StreamMapper struct {
	AggregateName string
	EventName     string
	StreamName    string
}

type Store struct {
	js              jetstream.JetStream
	enc             EncodingRegisterer
	logger          *zap.SugaredLogger
	encMap          map[string]string
	aggStreamMapper map[string]string
	evtStreamMapper map[string][]string
}

func New(js jetstream.JetStream, enc EncodingRegisterer, logger *zap.SugaredLogger) *Store {
	return &Store{
		js:              js,
		enc:             enc,
		logger:          logger,
		encMap:          generateEncodingMap(enc),
		aggStreamMapper: map[string]string{},
		evtStreamMapper: map[string][]string{},
	}
}

func generateEncodingMap(enc EncodingRegisterer) map[string]string {
	out := make(map[string]string, len(enc.Map()))
	for k := range enc.Map() {
		out[normaliseEventName(k)] = k
	}
	return out
}

// Insert inserts events into the store.
func (s *Store) Insert(ctx context.Context, evts ...event.Event) error {
	s.logger.Debugw("inserting events")
	for _, evt := range evts {
		u, name, version := evt.Aggregate()
		b, err := s.enc.Marshal(evt.Data())
		if err != nil {
			return err
		}
		headers := make(nats.Header)
		headers.Add("event-name", evt.Name())
		headers.Add("event-time", evt.Time().Format(time.RFC3339Nano))
		headers.Add("aggregate-name", pick.AggregateName(evt))
		headers.Add("aggregate-id", pick.AggregateID(evt).String())
		headers.Add("aggregate-version", fmt.Sprint(pick.AggregateVersion(evt)))
		sub, err := subjectFunc(name, u, version, evt.Name())
		if err != nil {
			return err
		}

		msg := &nats.Msg{
			Subject: sub,
			Header:  headers,
			Data:    b,
		}

		opts := []jetstream.PublishOpt{
			jetstream.WithMsgID(evt.ID().String()),
			jetstream.WithExpectLastSequencePerSubject(0),
		}

		_, err = s.js.PublishMsg(
			ctx,
			msg,
			opts...,
		)

		if err != nil {
			s.logger.Errorw("err inserting events", "error", err, "subject", sub)
			return err
		}
	}
	s.logger.Debug("inserted events")
	return nil
}

// Find fetches the given event from the store.
func (s *Store) Find(_ context.Context, _ uuid.UUID) (event.Event, error) {
	panic("not implemented") // TODO: Implement
}

// Query queries the store for events and returns two channels – one for the
// returned events and one for any asynchronous errors that occur during the
// query.
func (s *Store) Query(ctx context.Context, q event.Query) (<-chan event.Event, <-chan error, error) {
	s.logger.Debugw("query",
		"ids", q.AggregateIDs(),
		"names", q.AggregateNames(),
		"aggregates", len(q.Aggregates()),
		"event_name", q.Names(),
		"version min", q.AggregateVersions().Min(),
		"version max", q.AggregateVersions().Max(),
	)

	// streams, err := s.identifyStreams(q.AggregateNames(), q.Names())

	subjects, err := s.buildQuery(q)
	if err != nil {
		return nil, nil, err
	}

	evts, errs, err := s.query(ctx, q, subjects)
	if err != nil {
		return nil, nil, err
	}
	return evts, errs, nil
}

func (s *Store) IdentifyStreams(ctx context.Context, aggregateNames, evtNames []string) error {
	err := s.identifyStreams(ctx, aggregateNames, evtNames)
	return err
}

func (s *Store) identifyStreams(ctx context.Context, aggregateNames, evtNames []string) error {
	for _, a := range aggregateNames {
		if _, ok := s.aggStreamMapper[a]; ok {
			//already identified
			continue
		}

		subject := fmt.Sprintf("es.%s.*.*.*", a)
		snl := s.js.StreamNames(ctx, jetstream.WithStreamListSubject(subject))
		if snl.Err() != nil {
			return snl.Err()
		}
		names, err := streams.Drain(ctx, snl.Name())
		if snl.Err() != nil {
			return err
		}
		if len(names) == 0 {
			return fmt.Errorf("no stream for aggregate (%s)", a)
		}
		s.aggStreamMapper[a] = names[0]
	}

	for _, e := range evtNames {
		normalisedEvt := normaliseEventName(e)
		if _, ok := s.evtStreamMapper[normalisedEvt]; ok {
			//already identified
			continue
		}
		subject := fmt.Sprintf("es.*.*.*.%s", normalisedEvt)
		snl := s.js.StreamNames(ctx, jetstream.WithStreamListSubject(subject))
		if snl.Err() != nil {
			return snl.Err()
		}
		names, err := streams.Drain(ctx, snl.Name())
		if snl.Err() != nil {
			return err
		}
		if len(names) == 0 {
			return fmt.Errorf("no stream for event (%s)", e)
		}
		s.evtStreamMapper[normalisedEvt] = names
	}

	return nil
}

func (s *Store) buildQuery(q event.Query) ([]string, error) {
	names := q.Names()
	subjects := buildEventNameQuery(
		buildAggregateVersionsQuery(
			buildAggregateIdsQuery(
				buildAggregatesQuery(q.AggregateNames()),
				q.AggregateIDs(),
			),
			q.AggregateVersions(),
		),
		names,
	)
	return subjects, nil
}

func (s *Store) query(ctx context.Context, q event.Query, subjects []string) (<-chan event.Event, <-chan error, error) {
	subCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()

	err := s.identifyStreams(ctx, q.AggregateNames(), q.Names())
	if err != nil {
		return nil, nil, err
	}

	opts := natsOpts(ctx, q)
	guard := newLimitGuard(q)

	s.logger.Debugw("subject list", "subs", subjects)

	opErrs := make(chan error, 1)
	defer close(opErrs)
	subErrs := make(chan error, 1)

	errs := streams.FanInContext(ctx, opErrs, subErrs)

	groups := make(map[string][]string)
	for _, subject := range subjects {
		streams, err := s.streamFunc(subject)
		if err != nil {
			return nil, nil, err
		}

		for _, name := range streams {
			groups[name] = append(groups[name], subject)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(groups))

	cmsgs := make(chan jetstream.Msg, 200)
	push := streams.ConcurrentContext(subCtx, cmsgs)
	go func() {
		wg.Wait()
		close(cmsgs)
		close(subErrs)
	}()

	for stream, subjects := range groups {
		go s.subscribe(subCtx, &wg, stream, subjects, push, subErrs, opts...)
	}

	msgs, err := s.collect(ctx, cmsgs, subErrs, guard)
	if err != nil {
		opErrs <- err
	}

	select {
	case <-subCtx.Done():
		return nil, errs, nil
	default:
		s.logger.Debugw("ctx not done")
		break
	}

	applySortings(msgs, q.Sortings())

	s.logger.Debugw("total msg c", "total", len(msgs), "duration", time.Since(start))
	return streams.New(msgs), errs, nil
}

func (s *Store) subscribe(ctx context.Context, wg *sync.WaitGroup, stream string, subjects []string, push func(...jetstream.Msg) error, errs chan<- error, opts ...nats.SubOpt) {
	defer wg.Done()

	var subWg sync.WaitGroup
	subWg.Add(len(subjects))

	// TODO: simplify this using `FilterSubjects` when nats 2.10 is out
	for _, sub := range subjects {
		go func(sub string, wg *sync.WaitGroup) {
			defer wg.Done()
			c, err := s.js.CreateOrUpdateConsumer(
				ctx,
				stream,
				jetstream.ConsumerConfig{
					DeliverPolicy:     jetstream.DeliverAllPolicy,
					AckPolicy:         jetstream.AckExplicitPolicy,
					FilterSubject:     sub,
					ReplayPolicy:      jetstream.ReplayInstantPolicy,
					InactiveThreshold: 200 * time.Millisecond,
					MemoryStorage:     true,
					// FilterSubjects:     subjects,
				},
			)

			if err != nil {
				s.logger.Errorw("consumer error", "err", err)
				errs <- err
				return
			}

			expected := c.CachedInfo().NumPending
			if expected == 0 {
				return
			}

			if err != nil {
				s.logger.Errorw("consumer error", "err", err)
				errs <- err
				return
			}

			it, err := c.Messages()
			defer func() {
				go it.Stop()
			}()
			if err != nil {
				log.Fatal(err)
			}
			var count uint64 = 0
			for {
				msg, err := it.Next()
				if err != nil {
					s.logger.Errorw("err getting next msg", "err", err)
					errs <- err
					break
				}
				err = push(msg)
				if err != nil {
					s.logger.Errorw("err pushing next msg", "err", err)
					errs <- err
					break
				}
				err = msg.Ack()
				if err != nil {
					s.logger.Errorw("err acking msg", "err", err)
					errs <- err
					break
				}
				count++
				if count >= expected {
					break
				}
			}
		}(sub, &subWg)
	}
	subWg.Wait()
}

func (s *Store) collect(ctx context.Context, msgs <-chan jetstream.Msg, errs chan error, g limitGuard) ([]event.Event, error) {
	return streams.All(
		streams.Filter(
			streams.Map(
				ctx,
				msgs,
				func(m jetstream.Msg) event.Event {
					e, err := s.processQueryMsg(m)
					if err != nil {
						errs <- err
					}
					return e
				}),
			func(e event.Event) bool { return e != nil },
			g.guard,
		),
		errs,
	)
}

func (s *Store) streamFunc(subject string) ([]string, error) {
	parts := strings.Split(subject, ".")
	hasAggregate := parts[1] != "*"
	hasEvent := parts[4] != "*"
	if hasAggregate {
		return []string{s.aggStreamMapper[parts[1]]}, nil
	}

	if hasEvent {
		return s.evtStreamMapper[parts[4]], nil
	}

	return nil, fmt.Errorf("subject is not supported (%s)", subject)
}

func applySortings(evts []event.Event, sortings []event.SortOptions) {
	slices.SortFunc(
		evts,
		func(a event.Of[any], b event.Of[any]) bool {
			less := false
			for _, sorter := range sortings {
				switch sorter.Sort {
				case event.SortTime:
					less = a.Time().Before(b.Time())
				case event.SortAggregateVersion:
					if pick.AggregateName(a) != pick.AggregateName(b) {
						less = pick.AggregateName(a) < pick.AggregateName(b)
						continue
					}
					if pick.AggregateID(a) != pick.AggregateID(b) {
						less = pick.AggregateID(a).String() < pick.AggregateID(b).String()
						continue
					}
					less = pick.AggregateVersion(a) < pick.AggregateVersion(b)
				case event.SortAggregateName:
					less = pick.AggregateName(a) < pick.AggregateName(b)
				case event.SortAggregateID:
					less = pick.AggregateID(a).String() < pick.AggregateID(b).String()
				}
				if sorter.Dir == event.SortDesc {
					less = !less
				}
			}
			return false
		},
	)
}

func natsOpts(ctx context.Context, q event.Query) []nats.SubOpt {
	opts := []nats.SubOpt{
		nats.OrderedConsumer(),
		nats.ReplayInstant(),
		nats.Context(ctx),
	}

	// TODO: this is incorrect. The sequences will not match
	if q.AggregateVersions() != nil && len(q.AggregateVersions().Min()) > 0 {
		min := min(q.AggregateVersions().Min())
		opts = append(opts, nats.StartSequence(uint64(min)))
	}

	if q.Times() != nil && (q.Times().Min() != time.Time{}) {
		opts = append(opts, nats.StartTime(q.Times().Min()))
	}
	return opts
}

type limitGuard struct {
	versionGuard func(e event.Event) bool
	maxTimeGuard func(e event.Event) bool
}

func (g limitGuard) guard(e event.Event) bool {
	return g.maxTimeGuard(e) && g.versionGuard(e)
}

func newLimitGuard(q event.Query) limitGuard {
	guard := limitGuard{
		versionGuard: func(e event.Event) bool { return true },
		maxTimeGuard: func(e event.Event) bool { return true },
	}

	if q.AggregateVersions() != nil && len(q.AggregateVersions().Max()) > 0 {
		guard.versionGuard = func(e event.Event) bool {
			return pick.AggregateVersion(e) <= max(q.AggregateVersions().Max())
		}
	}

	if q.Times() != nil && (q.Times().Max() != time.Time{}) {
		guard.maxTimeGuard = func(e event.Event) bool {
			return e.Time().Before(q.Times().Max().Add(time.Microsecond))
		}
	}

	return guard
}

func buildAggregatesQuery(names []string) []string {
	if len(names) == 0 {
		return []string{"es.*"}
	}

	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, fmt.Sprintf("es.%s", name))
	}
	return out
}

func buildAggregateIdsQuery(aggQueries []string, ids []uuid.UUID) []string {
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

// Delete deletes events from the store.
func (s *Store) Delete(_ context.Context, _ ...event.Event) error {
	panic("not implemented") // TODO: Implement
}

func parseEventValues(h nats.Header) (uuid.UUID, string, time.Time, error) {
	evtName := h.Get("event-name")
	evtTime := h.Get("event-time")
	evtId := h.Get("Nats-Msg-Id")

	id, err := uuid.Parse(evtId)
	if err != nil {
		return uuid.Nil, "", time.Time{}, err
	}

	t, err := time.Parse(time.RFC3339Nano, evtTime)
	if err != nil {
		return uuid.Nil, "", time.Time{}, err
	}

	return id, evtName, t, nil
}

func subjectFunc(aggregateName string, id uuid.UUID, version int, eventName string) (string, error) {
	eventName = normaliseEventName(eventName)

	return fmt.Sprintf("es.%s.%s.%d.%s", aggregateName, id.String(), version, eventName), nil
}

func subjectToValues(sub string) (aggregateName string, id uuid.UUID, version int, err error) {
	parts := strings.Split(sub, ".")
	if len(parts) != 5 {
		return "", uuid.Nil, 0, errors.New("incorrect subject format")
	}
	parts = parts[1:]
	id, err = uuid.Parse(parts[1])
	if err != nil {
		return
	}

	version, err = strconv.Atoi(parts[2])
	if err != nil {
		return
	}
	return parts[0], id, version, nil
}

func normaliseEventName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

type eventMetadata struct {
	evtId            uuid.UUID
	evtName          string
	evtTime          time.Time
	aggregateName    string
	aggregateId      uuid.UUID
	aggregateVersion int
}

func getMetadata(h nats.Header, sub string) (eventMetadata, error) {
	evtId, evtName, evtTime, err := parseEventValues(h)
	if err != nil {
		return eventMetadata{}, err
	}

	aggName, aggId, aggVersion, err := subjectToValues(sub)
	if err != nil {
		return eventMetadata{}, err
	}

	return eventMetadata{
		evtId:            evtId,
		evtName:          evtName,
		evtTime:          evtTime,
		aggregateName:    aggName,
		aggregateId:      aggId,
		aggregateVersion: aggVersion,
	}, nil
}

func (s *Store) processQueryMsg(msg jetstream.Msg) (event.Event, error) {
	metadata, err := getMetadata(msg.Headers(), msg.Subject())
	if err != nil {
		return nil, err
	}

	if streams, ok := s.evtStreamMapper[eventFromSubject(msg.Subject())]; !ok || (ok && len(streams) > 1) {
		md, err := msg.Metadata()
		if err != nil {
			return nil, err
		}
		s.evtStreamMapper[eventFromSubject(msg.Subject())] = []string{md.Stream}
	}

	data, err := s.enc.Unmarshal(msg.Data(), metadata.evtName)
	if err != nil {
		return nil, err
	}

	e := event.New(
		metadata.evtName,
		data,
		event.ID(metadata.evtId),
		event.Time(metadata.evtTime),
		event.Aggregate(metadata.aggregateId, metadata.aggregateName, metadata.aggregateVersion),
	)
	return e, nil
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

func min[T constraints.Ordered](s []T) T {
	if len(s) == 0 {
		var zero T
		return zero
	}
	m := s[0]
	for _, v := range s {
		if m > v {
			m = v
		}
	}
	return m
}

func max[T constraints.Ordered](s []T) T {
	if len(s) == 0 {
		var zero T
		return zero
	}
	m := s[0]
	for _, v := range s {
		if m < v {
			m = v
		}
	}
	return m
}

func eventFromSubject(subject string) string {
	return strings.Split(subject, ".")[4]
}
