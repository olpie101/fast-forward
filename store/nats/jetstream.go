package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/codec"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/event/query/version"
	"github.com/modernice/goes/helper/pick"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

var (
	ErrEndOfMsgs = errors.New("end of message stream")
)

type EncodingRegisterer interface {
	codec.Encoding
	Map() map[string]func() any
}

type Store struct {
	js     nats.JetStreamContext
	enc    EncodingRegisterer
	logger *zap.SugaredLogger
}

func New(js nats.JetStreamContext, enc EncodingRegisterer, logger *zap.SugaredLogger) *Store {
	return &Store{
		js:     js,
		enc:    enc,
		logger: logger,
	}
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

		opts := []nats.PubOpt{
			nats.MsgId(evt.ID().String()),
			nats.ExpectLastSequencePerSubject(0),
		}

		_, err = s.js.PublishMsg(
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
	s.logger.Debug("query",
		"ids", q.AggregateIDs(),
		"names", q.AggregateNames(),
		"aggregates", len(q.Aggregates()),
		"event_name", q.Names(),
	)

	evtChan := make(chan event.Of[any])
	errChan := make(chan error)

	subjects, err := s.buildQuery(q)
	if err != nil {
		return nil, nil, err
	}

	go s.query(ctx, q, subjects, evtChan, errChan)
	return evtChan, errChan, nil
}

func (s *Store) buildQuery(q event.Query) ([]string, error) {
	names := q.Names()
	for i := 0; i < len(names); i++ {
		name, err := eventSubFromEvent(names[i])
		if err != nil {
			return nil, err
		}
		names[i] = name
	}
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

type evtTest func(m eventMetadata) bool

var defaultEvtTest = func(m eventMetadata) bool { return true }

func (s *Store) query(ctx context.Context, q event.Query, subjects []string, evts chan<- event.Event, errs chan<- error) {
	defer close(evts)
	defer close(errs)
	opts := []nats.SubOpt{
		nats.OrderedConsumer(),
		nats.ReplayInstant(),
		nats.Context(ctx),
	}

	if q.AggregateVersions() != nil && len(q.AggregateVersions().Min()) > 0 {
		min := min(q.AggregateVersions().Min())
		opts = append(opts, nats.StartSequence(uint64(min)))
	}

	if q.Times() != nil && (q.Times().Min() != time.Time{}) {
		opts = append(opts, nats.StartTime(q.Times().Min()))
	}

	maxVerTest := defaultEvtTest
	maxTimeTest := defaultEvtTest

	if q.AggregateVersions() != nil && len(q.AggregateVersions().Max()) > 0 {
		maxVerTest = func(m eventMetadata) bool {
			return m.aggregateVersion <= max(q.AggregateVersions().Max())
		}
	}

	if q.Times() != nil && (q.Times().Max() != time.Time{}) {
		maxTimeTest = func(m eventMetadata) bool {
			return m.evtTime.Before(q.Times().Max().Add(time.Microsecond))
		}
	}

	msgs := make(chan *nats.Msg, len(subjects))
	for _, subject := range subjects {
		_, err := s.js.ChanSubscribe(subject, msgs, opts...)
		if err != nil {
			s.logger.Errorw("err sub", "err", err)
			errs <- err
			return
		}
	}

	exp := 10 * time.Millisecond
	t := time.NewTimer(exp)
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-msgs:
			test := func(md eventMetadata) bool {
				return maxVerTest(md) && maxTimeTest(md)
			}
			evt, ok, err := s.processQueryMsg(m, test)
			if err != nil {
				errs <- err
			}
			if !ok {
				continue
			}

			evts <- evt
			t.Reset(exp)
		case <-t.C:
			return
		}
	}
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
			return aggQueries
		}
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
			out = append(out, fmt.Sprintf("%s.%s", aggName, name))
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

func parseAggregateValues(h nats.Header) (uuid.UUID, string, int, error) {
	aggName := h.Get("aggregate-name")
	aggId := h.Get("aggregate-id")
	aggVersion := h.Get("aggregate-version")

	id, err := uuid.Parse(aggId)
	if err != nil {
		return uuid.Nil, "", 0, err
	}

	ver, err := strconv.Atoi(aggVersion)
	if err != nil {
		return uuid.Nil, "", 0, err
	}

	return id, aggName, ver, nil
}

func subjectFunc(aggregateName string, id uuid.UUID, version int, eventName string) (string, error) {
	eventName, err := eventSubFromEvent(eventName)
	if err != nil {
		return "", err
	}

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

func eventSubFromEvent(evtName string) (string, error) {
	parts := strings.Split(evtName, ".")
	if len(parts) != 4 {
		return "", errors.New("incorrect event name format")
	}
	return parts[3], nil
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

func (s *Store) processQueryMsg(msg *nats.Msg, t evtTest) (event.Event, bool, error) {
	metadata, err := getMetadata(msg.Header, msg.Subject)
	if err != nil {
		return nil, false, err
	}

	ok := t(metadata)
	if !ok {
		return nil, false, nil
	}

	data, err := s.enc.Unmarshal(msg.Data, metadata.evtName)
	if err != nil {
		return nil, false, err
	}

	e := event.New(
		metadata.evtName,
		data,
		event.ID(metadata.evtId),
		event.Time(metadata.evtTime),
		event.Aggregate(metadata.aggregateId, metadata.aggregateName, metadata.aggregateVersion),
	)
	return e, true, nil
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
