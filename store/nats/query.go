package nats

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/modernice/goes/event"
	"github.com/modernice/goes/helper/pick"
	"github.com/modernice/goes/helper/streams"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/exp/slices"
)

func (s *Store) query(ctx context.Context, q event.Query, subjects []string) (<-chan event.Event, <-chan error, error) {
	subCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()

	err := s.identifyStreams(ctx, q.AggregateNames(), q.Names())
	if err != nil {
		return nil, nil, err
	}

	guard := newLimitGuard(q)

	s.logger.Debugw("subject list", "subs", subjects)

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
	subErrs := make(chan error, 1)

	for stream, subjects := range groups {
		go s.subFn(subCtx, &wg, stream, subjects, push, subErrs)
	}

	go func() {
		wg.Wait()
		close(cmsgs)
		close(subErrs)
	}()

	err = <-subErrs
	if err != nil {
		return nil, nil, err
	}

	opErrs := make(chan error, 1)
	defer close(opErrs)

	// TODO: this collect is sync
	msgs, err := s.collect(ctx, cmsgs, opErrs, guard)
	if err != nil {
		opErrs <- err
	}

	select {
	case <-subCtx.Done():
		return nil, opErrs, nil
	default:
		s.logger.Debugw("ctx not done")
		break
	}

	applySortings(msgs, q.Sortings())

	s.logger.Debugw("total msg c", "total", len(msgs), "duration", time.Since(start))
	return streams.New(msgs), opErrs, nil
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

func (s *Store) subscribe(ctx context.Context, wg *sync.WaitGroup, stream string, subjects []string, push func(...jetstream.Msg) error, errs chan<- error) {
	defer wg.Done()

	str, err := s.js.Stream(ctx, stream)

	if err != nil {
		errs <- err
		return
	}

	i := str.CachedInfo()
	conCfg := consumerConfig(subjects, i.Config)

	c, err := s.js.CreateOrUpdateConsumer(ctx, stream, conCfg)

	if err != nil {
		s.logger.Errorw("create consumer error", "err", err, "stream", stream, "subjects", conCfg.FilterSubjects)
		errs <- err
		return
	}

	err = consumeMessages(c, push, s.pullExpiry)
	if err != nil {
		s.logger.Errorw("err consuming messages", "err", err, "stream", stream)
		errs <- err
	}
}

func normaliseSubject(streamSubject, subject string) string {
	strParts := strings.Split(streamSubject, ".")
	subParts := strings.Split(subject, ".")
	if strParts[1] != subParts[1] {
		subParts[1] = strParts[1]
	}
	return strings.Join(subParts, ".")
}

func eventFromSubject(subject string) string {
	return strings.Split(subject, ".")[4]
}

func defaultConsumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		InactiveThreshold: 10 * time.Second,
		MemoryStorage:     true,
	}
}

func consumerConfig(subjects []string, sc jetstream.StreamConfig) jetstream.ConsumerConfig {
	filterSubjects := make([]string, 0, len(subjects))
	for _, s := range subjects {
		filterSubjects = append(filterSubjects, normaliseSubject(sc.Subjects[0], s))
	}

	cfg := defaultConsumerConfig()
	cfg.FilterSubjects = filterSubjects
	return cfg
}

func consumeMessages(c jetstream.Consumer, push func(...jetstream.Msg) error, expTime time.Duration) error {
	expected := c.CachedInfo().NumPending
	if expected == 0 {
		return nil
	}

	it, err := c.Messages(jetstream.PullExpiry(expTime))
	if err != nil {
		return err
	}
	defer it.Stop()

	var count uint64 = 0
	for {
		msg, err := it.Next()
		if err != nil {
			return err
		}

		err = push(msg)
		if err != nil {
			return err
		}

		err = msg.Ack()
		if err != nil {
			return err
		}

		count++
		if count >= expected {
			break
		}
	}
	return nil
}

func applySortings(evts []event.Event, sortings []event.SortOptions) {
	slices.SortFunc(
		evts,
		func(a event.Of[any], b event.Of[any]) int {
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
			if less {
				return -1
			} else {
				return 1
			}
		},
	)
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
	)
}

func (s *Store) processQueryMsg(msg jetstream.Msg) (event.Event, error) {
	metadata, err := getMetadata(msg.Headers(), msg.Subject())
	if err != nil {
		return nil, err
	}

	//TODO: get event from metadata and normalise to replace eventFromSubject
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
