package nats

// This file contains methods related to creating consumers on
// nats servers pre v 2.10.0

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

func consumerConfigLegacy(subject string, sc jetstream.StreamConfig) jetstream.ConsumerConfig {
	cfg := defaultConsumerConfig()
	cfg.FilterSubject = normaliseSubject(sc.Subjects[0], subject)
	return cfg
}

func (s *Store) subscribeLegacy(ctx context.Context, wg *sync.WaitGroup, stream string, subjects []string, push func(...jetstream.Msg) error, errs chan<- error) {
	defer wg.Done()

	var subWg sync.WaitGroup
	subWg.Add(len(subjects))

	str, err := s.js.Stream(ctx, stream)

	if err != nil {
		errs <- err
		return
	}

	i := str.CachedInfo()

	for _, sub := range subjects {
		go func(sub string, wg *sync.WaitGroup) {
			defer wg.Done()

			conCfg := consumerConfigLegacy(sub, i.Config)

			c, err := s.js.CreateOrUpdateConsumer(ctx, stream, conCfg)

			if err != nil {
				s.logger.Errorw("create consumer error", "err", err, "stream", stream, "subject", sub)
				errs <- err
				return
			}

			err = consumeMessages(c, push, s.pullExpiry)
			if err != nil {
				s.logger.Errorw("err consuming messages", "err", err, "stream", stream, "subject", sub)
				errs <- err
			}

		}(sub, &subWg)
	}
	subWg.Wait()
}
