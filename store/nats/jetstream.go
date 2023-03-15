package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	js  nats.JetStreamContext
	enc EncodingRegisterer
	// stream string
	logger *zap.SugaredLogger
}

func New(js nats.JetStreamContext, enc EncodingRegisterer, logger *zap.SugaredLogger) *Store {
	return &Store{
		js:  js,
		enc: enc,
		// stream: stream,
		logger: logger,
	}
}

// func (s *Store) defaultBus() event.Bus {

// }

// Insert inserts events into the store.
func (s *Store) Insert(ctx context.Context, evts ...event.Event) error {
	s.logger.Infow("inserting events")
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
			// Subject: strings.ReplaceAll(evt.Name(), ".", "_"),
			Subject: sub,
			Header:  headers,
			Data:    b,
		}

		opts := []nats.PubOpt{
			nats.MsgId(evt.ID().String()),
		}
		// if pick.AggregateVersion(evt) > 1 {
		opts = append(opts, nats.ExpectLastSequencePerSubject(0))
		// }

		_, err = s.js.PublishMsg(
			msg,
			opts...,
		)

		if err != nil {
			s.logger.Errorw("err inserting events", "error", err)
			return err
		}
	}
	s.logger.Infow("inserted events")
	return nil
}

// Find fetches the given event from the store.
func (s *Store) Find(_ context.Context, _ uuid.UUID) (event.Event, error) {
	panic("not implemented") // TODO: Implement
}

// Query queries the store for events and returns two channels – one for the
// returned events and one for any asynchronous errors that occur during the
// query.
//
//	var store event.Store
//	events, errs, err := store.Query(context.TODO(), query.New(...))
//	// handle err
//	err := streams.Walk(context.TODO(), func(evt event.Event) {
//		log.Println(fmt.Sprintf("Queried event: %s", evt.Name()))
//	}, events, errs)
//	// handle err
func (s *Store) Query(ctx context.Context, q event.Query) (<-chan event.Event, <-chan error, error) {
	fmt.Printf("query %+v\n", q.AggregateIDs())
	fmt.Printf("query %+v\n", q.AggregateNames())
	fmt.Printf("query %d\n", len(q.Aggregates()))

	fmt.Printf("query %+v\n", q.Names())
	// ctx, cancel := context.WithTimeout(ctx, time.Second)
	// defer cancel()
	evtChan := make(chan event.Of[any])
	errChan := make(chan error)
	sig := make(chan error)

	// err := s.bus.Subscribe(ctx, complaint.ComplaintEvents[0:]...)
	subjects, err := s.buildQuery(q)
	if err != nil {
		return nil, nil, err
	}
	err = s.query(ctx, q, subjects, evtChan, errChan, sig)
	if err != nil {
		return nil, nil, err
	}

	// sub, err := s.js.Subscribe(
	// 	fmt.Sprintf("es.%s.%s.*.*", q.AggregateNames()[0], q.AggregateIDs()[0]),
	// 	func(msg *nats.Msg) {
	// 		evtId, _, evtTime, err := parseEventValues(msg)
	// 		if err != nil {
	// 			errChan <- err
	// 			return
	// 		}

	// 		aggName, aggId, aggVersion, eventName, err := subjectToValues(msg.Subject)
	// 		// aggId, aggName, aggVersion, err := parseAggregateValues(msg)
	// 		if err != nil {
	// 			errChan <- err
	// 			return
	// 		}

	// 		data, err := s.enc.Unmarshal(msg.Data, eventName)
	// 		if err != nil {
	// 			errChan <- err
	// 			return
	// 		}

	// 		e := event.New(
	// 			eventName,
	// 			data,
	// 			event.ID(evtId),
	// 			event.Time(evtTime),
	// 			event.Aggregate(aggId, aggName, aggVersion),
	// 		)
	// 		evtChan <- e

	// 		i, _, err := msg.Sub.Pending()
	// 		if err != nil {
	// 			errChan <- err
	// 		}
	// 		if i == 1 {
	// 			sig <- ErrEndOfMsgs
	// 		}
	// 	},
	// 	nats.OrderedConsumer(),
	// 	nats.ReplayInstant(),
	// 	nats.BindStream(s.stream),
	// 	nats.Context(ctx),
	// )

	// if err != nil {
	// 	return nil, nil, err
	// }

	go func() {
	L:
		for {
			select {
			case <-ctx.Done():
				fmt.Println("DONE>>>>")
				errChan <- context.DeadlineExceeded
			case err = <-sig:
				fmt.Println("SIG>>>>")
				if !errors.Is(err, nats.ErrBadSubscription) && !errors.Is(err, ErrEndOfMsgs) {
					fmt.Println("ERR>>>>")
					errChan <- err
				}
				// close(evtChan)
				break L
			}
		}
		// err = sub.Unsubscribe()
		// if err != nil {
		// 	errChan <- err
		// }
		// close(evtChan)
		close(errChan)
		close(sig)
	}()

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

func (s *Store) query(ctx context.Context, q event.Query, subjects []string, evtChan chan<- event.Event, errChan chan<- error, sig chan<- error) error {
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

	hasMaxVersion := false
	hasMaxTime := false
	var maxVersion uint64
	var maxTime time.Time

	if q.AggregateVersions() != nil && len(q.AggregateVersions().Max()) > 0 {
		maxVersion = uint64(max(q.AggregateVersions().Max()))
	}

	if q.Times() != nil && (q.Times().Max() != time.Time{}) {
		maxTime = q.Times().Max()
	}

	var wg sync.WaitGroup
	for _, subject := range subjects {
		wg.Add(1)
		// TODO: keep track of all subscriptions
		_, err := s.js.Subscribe(
			subject,
			s.processQueryMsg(&wg, errChan, evtChan, sig, hasMaxVersion, maxVersion, hasMaxTime, maxTime),
			opts...,
		)

		if err != nil {
			return err
		}
	}
	wg.Wait()
	return nil
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

func parseEventValues(msg *nats.Msg) (uuid.UUID, string, time.Time, error) {
	evtName := msg.Header.Get("event-name")
	evtTime := msg.Header.Get("event-time")
	evtId := msg.Header.Get("Nats-Msg-Id")

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

func parseAggregateValues(msg *nats.Msg) (uuid.UUID, string, int, error) {
	aggName := msg.Header.Get("aggregate-name")
	aggId := msg.Header.Get("aggregate-id")
	aggVersion := msg.Header.Get("aggregate-version")
	fmt.Println(">>> testing")

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

func (s *Store) processQueryMsg(wg *sync.WaitGroup, errChan chan<- error, evtChan chan<- event.Event, sig chan<- error, hasMaxVersion bool, maxVersion uint64, hasMaxTime bool, maxTime time.Time) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		fmt.Println("message received")
		evtId, evtName, evtTime, err := parseEventValues(msg)
		if err != nil {
			errChan <- err
			return
		}

		aggName, aggId, aggVersion, err := subjectToValues(msg.Subject)
		// aggId, aggName, aggVersion, err := parseAggregateValues(msg)
		if err != nil {
			errChan <- err
			return
		}

		if hasMaxVersion && aggVersion > int(maxVersion) {
			return
		}

		if hasMaxTime && evtTime.After(maxTime) {
			return
		}

		data, err := s.enc.Unmarshal(msg.Data, evtName)
		if err != nil {
			errChan <- err
			return
		}

		e := event.New(
			evtName,
			data,
			event.ID(evtId),
			event.Time(evtTime),
			event.Aggregate(aggId, aggName, aggVersion),
		)
		evtChan <- e

		i, _, err := msg.Sub.Pending()
		if err != nil {
			errChan <- err
			return
		}
		if i == 1 {
			wg.Done()
			// sig <- ErrEndOfMsgs
		}
	}
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
