package nats

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestGenNatsHeader(t *testing.T) {
	eventID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	aggregateID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	eventTime := time.Date(2026, 5, 1, 10, 11, 12, 123456789, time.UTC)
	evt := event.New[any](
		"order.created",
		map[string]string{"id": "123"},
		event.ID(eventID),
		event.Time(eventTime),
		event.Aggregate(aggregateID, "order", 3),
	)

	h := genNatsHeader(evt)

	assertHeaderValue(t, h, MetadataKeyEventName, "order.created")
	assertHeaderValue(t, h, MetadataKeyEventTime, eventTime.Format(time.RFC3339Nano))
	assertHeaderValue(t, h, MetadataKeyEventAggregateName, "order")
	assertHeaderValue(t, h, MetadataKeyEventAggregateId, aggregateID.String())
	assertHeaderValue(t, h, MetadataKeyEventAggregateVersion, "3")
	assertHeaderValue(t, h, jetstream.MsgIDHeader, eventID.String())
}

func TestParseEventValues(t *testing.T) {
	eventID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	eventTime := time.Date(2026, 5, 1, 10, 11, 12, 123456789, time.UTC)
	h := nats.Header{}
	h.Set(jetstream.MsgIDHeader, eventID.String())
	h.Set(MetadataKeyEventName, "order.created")
	h.Set(MetadataKeyEventTime, eventTime.Format(time.RFC3339Nano))

	gotID, gotName, gotTime, err := parseEventValues(h)
	if err != nil {
		t.Fatalf("parseEventValues() error = %v", err)
	}
	if gotID != eventID {
		t.Fatalf("id = %s, want %s", gotID, eventID)
	}
	if gotName != "order.created" {
		t.Fatalf("name = %q, want %q", gotName, "order.created")
	}
	if !gotTime.Equal(eventTime) {
		t.Fatalf("time = %s, want %s", gotTime, eventTime)
	}
}

func TestParseEventValuesInvalidHeaders(t *testing.T) {
	eventID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	eventTime := time.Date(2026, 5, 1, 10, 11, 12, 123456789, time.UTC)

	tests := []struct {
		name    string
		headers nats.Header
	}{
		{
			name: "missing message id",
			headers: nats.Header{
				MetadataKeyEventName: []string{"order.created"},
				MetadataKeyEventTime: []string{eventTime.Format(time.RFC3339Nano)},
			},
		},
		{
			name: "malformed message id",
			headers: nats.Header{
				jetstream.MsgIDHeader: []string{"not-a-uuid"},
				MetadataKeyEventName:  []string{"order.created"},
				MetadataKeyEventTime:  []string{eventTime.Format(time.RFC3339Nano)},
			},
		},
		{
			name: "missing event time",
			headers: nats.Header{
				jetstream.MsgIDHeader: []string{eventID.String()},
				MetadataKeyEventName:  []string{"order.created"},
			},
		},
		{
			name: "invalid event time",
			headers: nats.Header{
				jetstream.MsgIDHeader: []string{eventID.String()},
				MetadataKeyEventName:  []string{"order.created"},
				MetadataKeyEventTime:  []string{"not-a-time"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := parseEventValues(tt.headers)
			if err == nil {
				t.Fatal("parseEventValues() error = nil, want error")
			}
		})
	}
}

func TestSubjectToValues(t *testing.T) {
	aggregateID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	subject := "es.order." + aggregateID.String() + ".3.order_created"

	gotName, gotID, gotVersion, err := subjectToValues(subject)
	if err != nil {
		t.Fatalf("subjectToValues() error = %v", err)
	}
	if gotName != "order" {
		t.Fatalf("aggregate name = %q, want %q", gotName, "order")
	}
	if gotID != aggregateID {
		t.Fatalf("aggregate id = %s, want %s", gotID, aggregateID)
	}
	if gotVersion != 3 {
		t.Fatalf("version = %d, want 3", gotVersion)
	}
}

func TestSubjectToValuesInvalidSubjects(t *testing.T) {
	aggregateID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	tests := []struct {
		name    string
		subject string
		wantErr string
	}{
		{
			name:    "too few tokens",
			subject: "es.order." + aggregateID.String() + ".3",
			wantErr: "incorrect subject format",
		},
		{
			name:    "too many tokens from dotted event name",
			subject: "es.order." + aggregateID.String() + ".3.order.created",
			wantErr: "incorrect subject format",
		},
		{
			name:    "invalid aggregate uuid",
			subject: "es.order.not-a-uuid.3.order_created",
		},
		{
			name:    "invalid aggregate version",
			subject: "es.order." + aggregateID.String() + ".not-a-version.order_created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := subjectToValues(tt.subject)
			if err == nil {
				t.Fatal("subjectToValues() error = nil, want error")
			}
			if tt.wantErr != "" && err.Error() != tt.wantErr {
				t.Fatalf("error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestGetMetadata(t *testing.T) {
	eventID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	aggregateID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	eventTime := time.Date(2026, 5, 1, 10, 11, 12, 123456789, time.UTC)
	h := nats.Header{}
	h.Set(jetstream.MsgIDHeader, eventID.String())
	h.Set(MetadataKeyEventName, "order.created")
	h.Set(MetadataKeyEventTime, eventTime.Format(time.RFC3339Nano))
	subject := "es.order." + aggregateID.String() + ".3.order_created"

	got, err := getMetadata(h, subject)
	if err != nil {
		t.Fatalf("getMetadata() error = %v", err)
	}
	if got.evtId != eventID {
		t.Fatalf("event id = %s, want %s", got.evtId, eventID)
	}
	if got.evtName != "order.created" {
		t.Fatalf("event name = %q, want %q", got.evtName, "order.created")
	}
	if !got.evtTime.Equal(eventTime) {
		t.Fatalf("event time = %s, want %s", got.evtTime, eventTime)
	}
	if got.aggregateName != "order" {
		t.Fatalf("aggregate name = %q, want %q", got.aggregateName, "order")
	}
	if got.aggregateId != aggregateID {
		t.Fatalf("aggregate id = %s, want %s", got.aggregateId, aggregateID)
	}
	if got.aggregateVersion != 3 {
		t.Fatalf("aggregate version = %d, want 3", got.aggregateVersion)
	}
}

func TestGetMetadataPropagatesParseErrors(t *testing.T) {
	aggregateID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	eventTime := time.Date(2026, 5, 1, 10, 11, 12, 123456789, time.UTC)
	validHeaders := nats.Header{}
	validHeaders.Set(jetstream.MsgIDHeader, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	validHeaders.Set(MetadataKeyEventName, "order.created")
	validHeaders.Set(MetadataKeyEventTime, eventTime.Format(time.RFC3339Nano))

	invalidHeaders := nats.Header{}
	invalidHeaders.Set(jetstream.MsgIDHeader, "not-a-uuid")
	invalidHeaders.Set(MetadataKeyEventName, "order.created")
	invalidHeaders.Set(MetadataKeyEventTime, eventTime.Format(time.RFC3339Nano))

	_, err := getMetadata(invalidHeaders, "bad.subject")
	if err == nil {
		t.Fatal("getMetadata() header error = nil, want error")
	}
	if err.Error() == "incorrect subject format" {
		t.Fatal("getMetadata() parsed subject before returning header error")
	}

	_, err = getMetadata(validHeaders, "es.order."+aggregateID.String()+".3.order.created")
	if err == nil {
		t.Fatal("getMetadata() subject error = nil, want error")
	}
	if err.Error() != "incorrect subject format" {
		t.Fatalf("error = %q, want incorrect subject format", err.Error())
	}
}

func TestEventMetadataNormalisedEventName(t *testing.T) {
	m := eventMetadata{evtName: "order.created.v2"}
	if got, want := m.normalisedEventName(), "order_created_v2"; got != want {
		t.Fatalf("normalisedEventName() = %q, want %q", got, want)
	}
}

func assertHeaderValue(t *testing.T, h nats.Header, key, want string) {
	t.Helper()
	if got := h.Get(key); got != want {
		t.Fatalf("header %q = %q, want %q", key, got, want)
	}
}
