package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/modernice/goes/event"
	"github.com/modernice/goes/helper/streams"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/exp/slices"
)

func (s *Store) query(ctx context.Context, q event.Query, subjects []string) (<-chan event.Event, <-chan error, error) {
	var subCtx context.Context
	var cancel context.CancelFunc
	_, ok := ctx.Deadline()
	if !ok {
		subCtx, cancel = context.WithTimeout(ctx, 120*time.Second)
	} else {
		subCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	start := time.Now()

	err := s.identifyStreams(ctx, q)
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
	minTime := q.Times().Min()

	if len(subjects) == 1 && len(q.AggregateIDs()) == 1 && guard.hasMinVersion && guard.minVersion > 1 {
		st := time.Now()
		for stream, subjects := range groups {
			t, err := s.fetchVersionMinTime(ctx, stream, subjects[0], guard.minVersion-1)
			if err != nil {
				s.logger.Errorw("unable to get aggregate min time", "subject", subjects[0], "error", err)
				break
			}
			minTime = t
		}
		s.logger.Infow("aggreagte fetch version metadata", "d", time.Since(st))
	}

	for stream, subjects := range groups {
		go s.subFn(subCtx, &wg, stream, subjects, minTime, push, subErrs)
	}

	go func() {
		wg.Wait()
		close(cmsgs)
		close(subErrs)
	}()

	opErrs := make(chan error, 1)
	defer close(opErrs)

	// TODO: this collect is sync
	evts, err := s.collect(ctx, cmsgs, opErrs, guard)
	if err != nil {
		opErrs <- err
	}

	err = <-subErrs
	if err != nil {
		return nil, nil, err
	}

	select {
	case <-subCtx.Done():
		return nil, opErrs, nil
	default:
		break
	}

	sortings := q.Sortings()
	if len(q.Sortings()) == 0 {
		sortings = append(
			sortings,
			event.SortOptions{
				Sort: event.SortTime,
				Dir:  event.SortAsc,
			},
		)
	}

	evts = event.SortMulti(evts, sortings...)

	s.logger.Debugw("total evts c", "total", len(evts), "duration", time.Since(start))
	return streams.New(evts), opErrs, nil
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

func (s *Store) identifyStreams(ctx context.Context, q event.Query) error {
	aggregateNames := q.AggregateNames()
	for _, r := range q.Aggregates() {
		if !slices.Contains(aggregateNames, r.Name) {
			aggregateNames = append(aggregateNames, r.Name)
		}
	}
	evtNames := q.Names()

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

func (s *Store) fetchVersionMinTime(ctx context.Context, stream, subject string, version int) (time.Time, error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 5 && parts[3] != "*" {
		return time.Time{}, errors.New("unknown subject format to identify version")
	}

	subject = strings.Replace(subject, "*", fmt.Sprintf("%d", version), 1)
	s.logger.Debugw("fetching aggregate version meta", "subject", subject)
	c, err := s.js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckNonePolicy,
		FilterSubject: subject,
		HeadersOnly:   true,
		Replicas:      1,
		MemoryStorage: true,
	})

	if err != nil {
		return time.Time{}, err
	}

	msg, err := c.Next(jetstream.FetchMaxWait(10 * time.Second))
	if err != nil {
		return time.Time{}, err
	}
	_, _, t, err := parseEventValues(msg.Headers())
	return t, err
}

func (s *Store) subscribe(ctx context.Context, wg *sync.WaitGroup, stream string, subjects []string, startTime time.Time, push func(...jetstream.Msg) error, errs chan<- error) {
	defer wg.Done()

	str, err := s.js.Stream(ctx, stream)

	if err != nil {
		errs <- err
		return
	}

	i := str.CachedInfo()
	conCfg := consumerConfig(subjects, startTime, i.Config)

	c, err := s.js.CreateOrUpdateConsumer(ctx, stream, conCfg)

	if err != nil {
		errs <- fmt.Errorf("create consumer error (stream: %s): %w", stream, err)
		return
	}

	err = consumeMessages(c, push, s.pullExpiry)
	if err != nil {
		errs <- fmt.Errorf("err consuming messages (stream: %s): %w", stream, err)
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

func defaultConsumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		InactiveThreshold: 10 * time.Second,
		Replicas:          1,
		MemoryStorage:     true,
	}
}

func consumerConfig(subjects []string, startTime time.Time, sc jetstream.StreamConfig) jetstream.ConsumerConfig {
	filterSubjects := make([]string, 0, len(subjects))
	for _, s := range subjects {
		filterSubjects = append(filterSubjects, normaliseSubject(sc.Subjects[0], s))
	}

	cfg := defaultConsumerConfig()
	cfg.FilterSubjects = filterSubjects

	if !startTime.IsZero() {
		cfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		cfg.OptStartTime = &startTime
	}
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

	if streams, ok := s.evtStreamMapper[metadata.normalisedEventName()]; !ok || (ok && len(streams) > 1) {
		md, err := msg.Metadata()
		if err != nil {
			return nil, err
		}
		s.evtStreamMapper[metadata.normalisedEventName()] = []string{md.Stream}
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
