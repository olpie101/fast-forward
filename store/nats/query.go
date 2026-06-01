package nats

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	// The streaming fast path transfers cancel ownership to its coordinator
	// goroutine, so only cancel here when we keep the materialized path.
	streaming := false
	defer func() {
		if !streaming {
			cancel()
		}
	}()
	start := time.Now()

	err := s.identifyStreams(ctx, q)
	if err != nil {
		return nil, nil, err
	}

	guard := newLimitGuard(q)

	s.logger.Debugw("subject list", "subs", subjects)

	groups := make(map[string][]string)
	for _, subject := range subjects {
		streamNames, err := s.streamFunc(subject)
		if err != nil {
			return nil, nil, err
		}

		for _, name := range streamNames {
			groups[name] = append(groups[name], subject)
		}
	}

	minTime := q.Times().Min()
	if len(subjects) == 1 && len(q.AggregateIDs()) == 1 && guard.hasMinVersion && guard.minVersion > 1 {
		st := time.Now()
		for stream, gsubjects := range groups {
			t, err := s.fetchVersionMinTime(ctx, stream, gsubjects[0], guard.minVersion-1)
			if err != nil {
				s.logger.Errorw("unable to get aggregate min time", "subject", gsubjects[0], "error", err)
				break
			}
			minTime = t
		}
		s.logger.Infow("aggreagte fetch version metadata", "d", time.Since(st))
	}

	// SortAggregateVersion/Asc fast path relies on Store.Insert preserving and
	// appending single-aggregate events in aggregate-version order (enforced by
	// validateBatchVersionContiguity before lease acquisition), so an ordered
	// consumer delivers them already sorted and no full materialize +
	// event.SortMulti is required.
	if canStreamReplay(q, subjects, groups) {
		cmsgs, subErrs := s.spawnSubscriptions(subCtx, groups, minTime)
		out, errs := s.streamReplay(subCtx, cancel, groups, minTime, guard, cmsgs, subErrs)
		streaming = true
		return out, errs, nil
	}

	cmsgs, subErrs := s.spawnSubscriptions(subCtx, groups, minTime)

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

// spawnSubscriptions starts one subscribe goroutine per stream group and a
// waiter that closes both returned channels once every subscriber has
// finished. It is shared by the materialized and streaming query paths and may
// be called again by the streaming coordinator to re-establish a subscription
// on a retryable error.
func (s *Store) spawnSubscriptions(subCtx context.Context, groups map[string][]string, minTime time.Time) (<-chan jetstream.Msg, <-chan error) {
	var wg sync.WaitGroup
	wg.Add(len(groups))

	cmsgs := make(chan jetstream.Msg, 200)
	push := streams.ConcurrentContext(subCtx, cmsgs)
	subErrs := make(chan error, 1)

	for stream, subjects := range groups {
		go s.subscribe(subCtx, &wg, stream, subjects, minTime, push, subErrs)
	}

	go func() {
		wg.Wait()
		close(cmsgs)
		close(subErrs)
	}()

	return cmsgs, subErrs
}

// canStreamReplay reports whether a query is eligible for the streaming
// fast path: it must resolve to a single stream, target exactly one concrete
// (aggregateName, aggregateID) pair across all built subjects, and request
// exactly one explicit ascending SortAggregateVersion ordering.
func canStreamReplay(q event.Query, subjects []string, groups map[string][]string) bool {
	if len(groups) != 1 || len(subjects) == 0 {
		return false
	}
	if !isSingleAggregateVersionAscSort(q) {
		return false
	}

	var name, id string
	for i, subject := range subjects {
		parts := strings.Split(subject, ".")
		if len(parts) != 5 || parts[0] != "es" {
			return false
		}
		if parts[1] == "*" || parts[2] == "*" {
			return false
		}
		if i == 0 {
			name, id = parts[1], parts[2]
			continue
		}
		if parts[1] != name || parts[2] != id {
			return false
		}
	}
	return true
}

func isSingleAggregateVersionAscSort(q event.Query) bool {
	sortings := q.Sortings()
	return len(sortings) == 1 &&
		sortings[0].Sort == event.SortAggregateVersion &&
		sortings[0].Dir == event.SortAsc
}

// streamReplay owns the returned out/errs channels and the subCtx cancel for
// the streaming fast path. A single coordinator goroutine is the sole sender to
// errs and the sole closer of both channels.
func (s *Store) streamReplay(
	subCtx context.Context,
	cancel context.CancelFunc,
	groups map[string][]string,
	minTime time.Time,
	guard limitGuard,
	cmsgs <-chan jetstream.Msg,
	subErrs <-chan error,
) (<-chan event.Event, <-chan error) {
	out := make(chan event.Event)
	errs := make(chan error, 1)

	go func() {
		defer cancel()
		defer close(errs)
		defer close(out)

		// Mirror Store.Query's retry budget (store.go:158-173). Retry is only
		// safe before the first event is emitted: an ordered-consumer restart
		// re-delivers from the start and would double-emit already-sent events.
		// The materialized fallback never emits before completing, so pre-emit
		// retry preserves equivalent ErrNoHeartbeat recovery without that hazard.
		emitted := false
		for i := 0; ; i++ {
			termErr := s.streamSubscription(subCtx, cmsgs, subErrs, guard, out, errs, &emitted)
			if termErr == nil {
				return
			}
			if errors.Is(termErr, jetstream.ErrNoHeartbeat) && !emitted && i < int(s.retryCount)-1 {
				s.logger.Errorw("stream replay retrying", "count", i+1, "err", termErr)
				backoff := time.Duration(2 * math.Pow(2, float64(i)))
				select {
				case <-subCtx.Done():
					return
				case <-time.After(backoff * time.Second):
				}
				cmsgs, subErrs = s.spawnSubscriptions(subCtx, groups, minTime)
				continue
			}
			sendStreamErr(subCtx, errs, termErr)
			return
		}
	}()

	return out, errs
}

// streamSubscription drains one subscription, emitting guard-passing events to
// out and forwarding decode/metadata errors to errs. It returns the terminal
// error for the pass: the subscription error (nil if the subscription finished
// cleanly) or a context error if subCtx was cancelled mid-send.
func (s *Store) streamSubscription(
	subCtx context.Context,
	cmsgs <-chan jetstream.Msg,
	subErrs <-chan error,
	guard limitGuard,
	out chan<- event.Event,
	errs chan<- error,
	emitted *bool,
) error {
	for m := range cmsgs {
		e, err := s.processQueryMsg(m)
		if err != nil {
			if !sendStreamErr(subCtx, errs, err) {
				return subCtx.Err()
			}
			continue
		}
		if e == nil || !guard.guard(e) {
			continue
		}
		select {
		case out <- e:
			*emitted = true
		case <-subCtx.Done():
			return subCtx.Err()
		}
	}

	return <-subErrs
}

// sendStreamErr sends err to errs unless subCtx is cancelled first, returning
// whether the send succeeded. It keeps every error send context-aware so the
// coordinator never blocks once the caller cancels.
func sendStreamErr(subCtx context.Context, errs chan<- error, err error) bool {
	select {
	case errs <- err:
		return true
	case <-subCtx.Done():
		return false
	}
}

func (s *Store) streamFunc(subject string) ([]string, error) {
	parts := strings.Split(subject, ".")
	hasAggregate := parts[1] != "*"
	hasEvent := parts[4] != "*"
	if hasAggregate {
		s.streamMapperMu.RLock()
		defer s.streamMapperMu.RUnlock()
		return []string{s.aggStreamMapper[parts[1]]}, nil
	}

	if hasEvent {
		s.streamMapperMu.RLock()
		defer s.streamMapperMu.RUnlock()
		return s.evtStreamMapper[parts[4]], nil
	}

	return nil, fmt.Errorf("subject is not supported (%s)", subject)
}

func (s *Store) hasAggregateStreamMapping(aggregate string) bool {
	s.streamMapperMu.RLock()
	defer s.streamMapperMu.RUnlock()
	_, ok := s.aggStreamMapper[aggregate]
	return ok
}

func (s *Store) cacheAggregateStreamMapping(aggregate, stream string) {
	s.streamMapperMu.Lock()
	defer s.streamMapperMu.Unlock()
	if _, ok := s.aggStreamMapper[aggregate]; !ok {
		s.aggStreamMapper[aggregate] = stream
	}
}

func (s *Store) hasEventStreamMapping(event string) bool {
	s.streamMapperMu.RLock()
	defer s.streamMapperMu.RUnlock()
	_, ok := s.evtStreamMapper[event]
	return ok
}

func (s *Store) cacheEventStreamMapping(event string, streamNames []string) {
	s.streamMapperMu.Lock()
	defer s.streamMapperMu.Unlock()
	if _, ok := s.evtStreamMapper[event]; !ok {
		s.evtStreamMapper[event] = streamNames
	}
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
		if s.hasAggregateStreamMapping(a) {
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
		s.cacheAggregateStreamMapping(a, names[0])
	}

	for _, e := range evtNames {
		normalisedEvt := normaliseEventName(e)
		if s.hasEventStreamMapping(normalisedEvt) {
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
		s.cacheEventStreamMapping(normalisedEvt, names)
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

// streamSubject returns the stream's configured first subject, caching it so
// subscribe avoids a per-query Stream() round trip. The cache value is static
// for the stream lifetime, so concurrent misses both perform the lookup and
// write the same constant (last-writer-wins); the JetStream call is made
// outside any lock.
func (s *Store) streamSubject(ctx context.Context, stream string) (string, error) {
	var subject string
	var ok bool
	func() {
		s.streamSubjectMu.RLock()
		defer s.streamSubjectMu.RUnlock()
		subject, ok = s.streamSubjectMapper[stream]
	}()
	if ok {
		return subject, nil
	}

	str, err := s.js.Stream(ctx, stream)
	if err != nil {
		return "", fmt.Errorf("stream info (stream: %s): %w", stream, err)
	}
	info := str.CachedInfo()
	if len(info.Config.Subjects) == 0 {
		return "", fmt.Errorf("stream has no subjects (stream: %s)", stream)
	}
	subject = info.Config.Subjects[0]

	func() {
		s.streamSubjectMu.Lock()
		defer s.streamSubjectMu.Unlock()
		s.streamSubjectMapper[stream] = subject
	}()

	return subject, nil
}

func (s *Store) subscribe(ctx context.Context, wg *sync.WaitGroup, stream string, subjects []string, startTime time.Time, push func(...jetstream.Msg) error, errs chan<- error) {
	defer wg.Done()

	streamSubject, err := s.streamSubject(ctx, stream)
	if err != nil {
		errs <- err
		return
	}

	threshold := max(orderedConsumerInactiveThreshold, 2*s.pullExpiry)
	if deadline, ok := ctx.Deadline(); ok {
		threshold = max(threshold, time.Until(deadline)+s.pullExpiry)
	}

	conCfg := orderedConsumerConfig(subjects, startTime, streamSubject, threshold)

	c, err := s.js.OrderedConsumer(ctx, stream, conCfg)
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

// orderedConsumerInactiveThreshold is the floor for a per-query ordered
// consumer's InactiveThreshold. nats.go's orderedSubscription.Stop only stops
// the current subscription and does not delete the final consumer (it deletes
// previous consumers only on reset), so InactiveThreshold is the primary
// server-side cleanup lever for the consumer left behind after consumeMessages
// defers it.Stop.
const orderedConsumerInactiveThreshold = 5 * time.Second

func orderedConsumerConfig(subjects []string, startTime time.Time, streamSubject string, inactiveThreshold time.Duration) jetstream.OrderedConsumerConfig {
	filterSubjects := make([]string, 0, len(subjects))
	for _, s := range subjects {
		filterSubjects = append(filterSubjects, normaliseSubject(streamSubject, s))
	}

	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:    filterSubjects,
		InactiveThreshold: inactiveThreshold,
	}

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
	// it.Stop only stops the current subscription; the final ordered consumer is
	// reaped server-side via its InactiveThreshold, not by this Stop.
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

	normalisedEvt := metadata.normalisedEventName()

	needsStreamMetadata := func() bool {
		s.streamMapperMu.RLock()
		defer s.streamMapperMu.RUnlock()
		streamNames, ok := s.evtStreamMapper[normalisedEvt]
		return !ok || len(streamNames) > 1
	}()

	if needsStreamMetadata {
		md, err := msg.Metadata()
		if err != nil {
			return nil, err
		}
		func() {
			s.streamMapperMu.Lock()
			defer s.streamMapperMu.Unlock()
			if streamNames, ok := s.evtStreamMapper[normalisedEvt]; !ok || len(streamNames) > 1 {
				s.evtStreamMapper[normalisedEvt] = []string{md.Stream}
			}
		}()
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
