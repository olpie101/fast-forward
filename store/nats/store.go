//go:generate go-options -new=false -output=store_options.go -option=StoreOption -prefix=With -imports=go.uber.org/zap,time Store

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
	"github.com/modernice/goes/helper/pick"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type Store struct {
	js              jetstream.JetStream `options:"-"`
	enc             codec.Encoding      `options:"-"`
	logger          *zap.SugaredLogger
	aggStreamMapper map[string]string   `options:"-"`
	evtStreamMapper map[string][]string `options:"-"`
	subFn           subFn               `options:"-"`
	retryCount      uint
	pullExpiry      time.Duration
}

type subFn func(ctx context.Context, wg *sync.WaitGroup, stream string, subjects []string, push func(...jetstream.Msg) error, errs chan<- error)

func defaultStoreOptions() []StoreOption {
	return []StoreOption{
		WithLogger(zap.NewNop().Sugar()),
		WithRetryCount(3),
		WithPullExpiry(time.Second),
	}
}

func New(nc *nats.Conn, enc codec.Encoding, opts ...StoreOption) (*Store, error) {
	options := defaultStoreOptions()
	options = append(options, opts...)
	legacy, err := isLegacy(nc.ConnectedServerVersion())
	if err != nil {
		return nil, errors.New("unable to determine server version")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	s := &Store{
		js:              js,
		enc:             enc,
		aggStreamMapper: map[string]string{},
		evtStreamMapper: map[string][]string{},
	}

	s.subFn = s.subscribe

	if legacy {
		s.subFn = s.subscribeLegacy
	}

	err = applyStoreOptions(s, options...)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// Insert inserts events into the store.
func (s *Store) Insert(ctx context.Context, evts ...event.Event) error {
	if len(evts) > 0 {
		s.logger.Debugw("inserting events", "count", len(evts))
	}
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
	if len(evts) > 0 {
		s.logger.Debug("inserted events")
	}
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

	subjects, err := s.buildQuery(q)
	if err != nil {
		return nil, nil, err
	}

	var evts <-chan event.Event = nil
	var errs <-chan error = nil

	for i := 0; i < int(s.retryCount); i++ {
		evts, errs, err = s.query(ctx, q, subjects)
		if err != nil {
			// retry if possible
			if errors.Is(err, jetstream.ErrNoHeartbeat) && i < int(s.retryCount)-1 {
				continue
			} else {
				return nil, nil, err
			}
		}
		break
	}

	return evts, errs, nil
}

// Delete deletes events from the store.
func (s *Store) Delete(_ context.Context, _ ...event.Event) error {
	panic("not implemented") // TODO: Implement
}

func subjectFunc(aggregateName string, id uuid.UUID, version int, eventName string) (string, error) {
	eventName = normaliseEventName(eventName)

	return fmt.Sprintf("es.%s.%s.%d.%s", aggregateName, id.String(), version, eventName), nil
}

func normaliseEventName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

func isLegacy(version string) (bool, error) {
	parts := strings.Split(version, ".")

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return false, err
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return false, err
	}

	if major < 2 || (major == 2 && minor < 10) {
		return true, nil
	}

	return false, nil
}
