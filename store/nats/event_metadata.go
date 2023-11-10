package nats

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Event metadata keys
const (
	MetadataKeyEventName             = "event-name"
	MetadataKeyEventTime             = "event-time"
	MetadataKeyEventAggregateName    = "aggregate-name"
	MetadataKeyEventAggregateId      = "aggregate-id"
	MetadataKeyEventAggregateVersion = "aggregate-version"
)

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

func parseEventValues(h nats.Header) (uuid.UUID, string, time.Time, error) {
	evtName := h.Get(MetadataKeyEventName)
	evtTime := h.Get(MetadataKeyEventTime)
	evtId := h.Get(jetstream.MsgIDHeader)

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
