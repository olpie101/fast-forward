//go:generate go-options -new=false -output=store_options.go -option=StoreOption -prefix=With -imports=github.com/olpie101/fast-forward/kv,go.uber.org/zap,time Store

package nats

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/codec"
	"github.com/modernice/goes/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/olpie101/fast-forward/kv"
	"go.uber.org/zap"
)

// Error
var (
	ErrLeaseLocked               = errors.New("lease is currently locked")
	ErrValidationVersionMismatch = errors.New("validation version mismatch")
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
	writeLeaseKV    kv.KeyValuer[*kv.NilValue]
	versionKV       kv.KeyValuer[*kv.UInt64Value]
}

type versionInfo struct {
	current      uint64
	next         uint64
	rev          uint64
	finalVersion uint64
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
		return nil, errors.New("store: unable to determine server version")
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

	err = validateStore(s)
	if err != nil {
		return nil, fmt.Errorf("store init: %w", err)
	}

	return s, nil
}

// Insert inserts events into the store.
func (s *Store) Insert(ctx context.Context, evts ...event.Event) error {
	leases, err := s.obtainLeases(ctx, evts)
	if err != nil {
		return err
	}
	defer func() {
		rErr := s.releaseLeases(ctx, leases, true)
		if rErr != nil {
			s.logger.Errorw("unable to release leases")
		}
	}()

	msgs, err := s.genPublishMsgs(evts, leases)
	if err != nil {
		return err
	}
	if len(evts) > 0 {
		s.logger.Debugw("inserting events", "count", len(evts))
	}
	for _, m := range msgs {
		opts := []jetstream.PublishOpt{
			jetstream.WithExpectLastSequencePerSubject(0),
		}

		_, err = s.js.PublishMsg(
			ctx,
			m,
			opts...,
		)

		if err != nil {
			s.logger.Errorw("err inserting events", "error", err, "subject", m.Subject)
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
				s.logger.Errorw("error consuming messages retrying...", "count", i+1)
				backoff := time.Duration(2 * math.Pow(2, float64(i)))
				<-time.After(backoff * time.Second)
				continue
			} else {
				s.logger.Errorw("error executing query", "err", err)
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

func (s *Store) genPublishMsgs(evts []event.Event, leases map[string]versionInfo) ([]*nats.Msg, error) {
	out := make([]*nats.Msg, 0, len(evts))
	processed := make(map[string]struct{}, len(leases))
	for _, evt := range evts {
		u, name, version := evt.Aggregate()

		lKey := writeLeaseKey(name, u)
		if _, ok := processed[lKey]; !ok {
			if lease := leases[lKey]; lease.next-1 != lease.current {
				return nil, ErrValidationVersionMismatch
			}
			processed[lKey] = struct{}{}
		}

		b, err := s.enc.Marshal(evt.Data())
		if err != nil {
			return nil, err
		}
		header := genNatsHeader(evt)

		sub, err := subjectFunc(name, u, version, evt.Name())
		if err != nil {
			return nil, err
		}

		msg := &nats.Msg{
			Subject: sub,
			Header:  header,
			Data:    b,
		}
		out = append(out, msg)
	}
	return out, nil
}

func (s *Store) obtainLeases(ctx context.Context, evts []event.Event) (obtained map[string]versionInfo, err error) {
	found := make(map[string]versionInfo)
	for _, evt := range evts {
		id, name, ver := evt.Aggregate()

		key := writeLeaseKey(name, id)
		if vi, ok := found[key]; ok {
			// Update final version
			_, _, ver := evt.Aggregate()
			vi.finalVersion = uint64(ver)
			found[key] = vi
			continue
		}

		found[key] = versionInfo{
			next:         uint64(ver),
			finalVersion: uint64(ver),
		}
	}

	// keys := maps.Keys(found)
	obtained = make(map[string]versionInfo, len(found))

	//release any leases previously obtained
	defer func() {
		if err == nil {
			return
		}

		rErr := s.releaseLeases(ctx, obtained, false)
		if rErr != nil {
			s.logger.Errorw("unable to release leases after failed obtained")
		}
	}()

	for k, v := range found {
		_, err := s.writeLeaseKV.Create(ctx, k, &kv.NilValue{})
		if err != nil {
			natsErr := &nats.APIError{}
			if ok := errors.As(err, &natsErr); ok && natsErr.ErrorCode == nats.JSErrCodeStreamWrongLastSequence {
				return nil, ErrLeaseLocked
			}
			return nil, err
		}
		ver, rev, err := s.getOrCreateCurrentVersion(ctx, k)
		if err != nil {
			return nil, err
		}
		v.current = ver
		if ver == 0 {
			v.current = v.next - 1
		}
		v.rev = rev
		obtained[k] = v
	}

	return obtained, nil
}

func (s *Store) releaseLeases(ctx context.Context, leases map[string]versionInfo, updateVersion bool) error {
	for k, v := range leases {
		if updateVersion {
			_, err := s.versionKV.Update(ctx, k, &kv.UInt64Value{Value: v.finalVersion}, v.rev)
			if err != nil {
				return fmt.Errorf("unable to update aggregate version: %w", err)
			}
		}
		err := s.writeLeaseKV.Delete(ctx, k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) getOrCreateCurrentVersion(ctx context.Context, key string) (uint64, uint64, error) {
	v, rev, err := s.versionKV.Get(ctx, key)
	notFound := errors.Is(err, nats.ErrKeyNotFound)
	if err != nil && !notFound {
		return 0, 0, err
	}
	if notFound {
		rev, err = s.versionKV.Create(ctx, key, &kv.UInt64Value{Value: 0})
		if err != nil && !notFound {
			return 0, 0, err
		}
	}
	return v.Value, rev, nil
}

func validateStore(s *Store) error {
	if s.js == nil {
		return errors.New("jetstream cannot be nil")
	}

	if s.enc == nil {
		return errors.New("encoding cannot be nil")
	}

	if s.writeLeaseKV == nil {
		return errors.New("write lease kv cannot be nil")
	}

	if s.versionKV == nil {
		return errors.New("aggregate version kv cannot be nil")
	}
	return nil
}

func writeLeaseKey(aggregateName string, id uuid.UUID) string {
	return fmt.Sprintf("%s.%s", aggregateName, id)
}
