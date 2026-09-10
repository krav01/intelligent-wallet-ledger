package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	"github.com/krav01/intelligent-wallet-ledger/internal/outbox"
)

func TestNewPublisherValidatesDependenciesAndConfiguration(t *testing.T) {
	t.Parallel()
	valid := validPublisherConfig()
	tests := []struct {
		name     string
		store    outbox.Store
		producer outbox.Producer
		logger   *slog.Logger
		config   outbox.PublisherConfig
	}{
		{name: "missing store", producer: &producerStub{}, logger: testLogger(), config: valid},
		{name: "missing producer", store: &storeStub{}, logger: testLogger(), config: valid},
		{name: "missing logger", store: &storeStub{}, producer: &producerStub{}, config: valid},
		{
			name:     "invalid batch size",
			store:    &storeStub{},
			producer: &producerStub{},
			logger:   testLogger(),
			config:   outbox.PublisherConfig{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := outbox.NewPublisher(
				test.store,
				test.producer,
				test.logger,
				test.config,
			); err == nil {
				t.Fatal("NewPublisher() error = nil, want validation error")
			}
		})
	}
}

func TestPublisherPublishBatchMarksOutcomes(t *testing.T) {
	t.Parallel()
	first := claimedEvent(t, "11111111-1111-4111-8111-111111111111", 0)
	second := claimedEvent(t, "22222222-2222-4222-8222-222222222222", 2)
	store := &storeStub{claimed: []outbox.ClaimedEvent{first, second}}
	producer := &producerStub{errors: []error{nil, errors.New("broker unavailable")}}
	publisher := mustPublisher(t, store, producer)

	result, err := publisher.PublishBatch(t.Context())
	if err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if result != (outbox.BatchResult{Claimed: 2, Published: 1, Failed: 1}) {
		t.Errorf("PublishBatch() result = %+v, want 2 claimed, 1 published, 1 failed", result)
	}
	if len(store.published) != 1 || store.published[0].eventID != first.Envelope.EventID() ||
		store.published[0].claimToken != first.ClaimToken {
		t.Errorf("published completions = %+v, want first event", store.published)
	}
	if len(store.failed) != 1 || store.failed[0].eventID != second.Envelope.EventID() ||
		store.failed[0].claimToken != second.ClaimToken || store.failed[0].retryAfter != 4*time.Second ||
		store.failed[0].diagnostic != "broker unavailable" {
		t.Errorf("failed completions = %+v, want second event with 4s retry", store.failed)
	}
	if len(producer.messages) != 2 {
		t.Fatalf("published messages = %d, want 2", len(producer.messages))
	}
	if producer.messages[0].Key != "transfer:"+first.Envelope.AggregateID() {
		t.Errorf("message key = %q, want transfer aggregate key", producer.messages[0].Key)
	}
	var envelopeFields map[string]json.RawMessage
	if err := json.Unmarshal(producer.messages[0].Value, &envelopeFields); err != nil {
		t.Fatalf("decoding published envelope: %v", err)
	}
	if string(envelopeFields["event_id"]) != `"`+first.Envelope.EventID()+`"` {
		t.Errorf("published event_id = %s, want %s", envelopeFields["event_id"], first.Envelope.EventID())
	}
}

func TestPublisherPublishBatchReturnsCompletionError(t *testing.T) {
	t.Parallel()
	claimed := claimedEvent(t, "11111111-1111-4111-8111-111111111111", 0)
	store := &storeStub{claimed: []outbox.ClaimedEvent{claimed}, markPublishedErr: errors.New("database unavailable")}
	publisher := mustPublisher(t, store, &producerStub{})

	result, err := publisher.PublishBatch(t.Context())
	if err == nil {
		t.Fatal("PublishBatch() error = nil, want completion error")
	}
	if result != (outbox.BatchResult{Claimed: 1}) {
		t.Errorf("PublishBatch() result = %+v, want one claimed event", result)
	}
}

func TestPublisherPublishBatchWrapsBrokerAndFailureRecordingErrors(t *testing.T) {
	t.Parallel()
	claimed := claimedEvent(t, "11111111-1111-4111-8111-111111111111", 0)
	brokerErr := errors.New("broker unavailable")
	recordingErr := errors.New("database unavailable")
	store := &storeStub{
		claimed:       []outbox.ClaimedEvent{claimed},
		markFailedErr: recordingErr,
	}
	publisher := mustPublisher(t, store, &producerStub{errors: []error{brokerErr}})

	result, err := publisher.PublishBatch(t.Context())
	if !errors.Is(err, brokerErr) || !errors.Is(err, recordingErr) {
		t.Fatalf("PublishBatch() error = %v, want wrapped broker and recording errors", err)
	}
	if result != (outbox.BatchResult{Claimed: 1}) {
		t.Errorf("PublishBatch() result = %+v, want one claimed event", result)
	}
}

func TestPublisherRunStopsOnCancellation(t *testing.T) {
	t.Parallel()
	store := &storeStub{}
	publisher := mustPublisher(t, store, &producerStub{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := publisher.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if store.claimCalls != 0 {
		t.Errorf("Claim() calls = %d, want 0", store.claimCalls)
	}
}

func TestPublisherRetryDelayCaps(t *testing.T) {
	t.Parallel()
	claimed := claimedEvent(t, "11111111-1111-4111-8111-111111111111", 62)
	store := &storeStub{claimed: []outbox.ClaimedEvent{claimed}}
	producer := &producerStub{errors: []error{errors.New("broker unavailable")}}
	publisher := mustPublisher(t, store, producer)

	if _, err := publisher.PublishBatch(t.Context()); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if len(store.failed) != 1 || store.failed[0].retryAfter != 8*time.Second {
		t.Errorf("retry = %+v, want capped 8s delay", store.failed)
	}
}

type storeStub struct {
	claimed          []outbox.ClaimedEvent
	claimErr         error
	markPublishedErr error
	markFailedErr    error
	claimCalls       int
	published        []completion
	failed           []failure
}

func (s *storeStub) Claim(
	_ context.Context,
	_ int,
	_ time.Duration,
) ([]outbox.ClaimedEvent, error) {
	s.claimCalls++
	return s.claimed, s.claimErr
}

func (s *storeStub) MarkPublished(_ context.Context, eventID, claimToken string) error {
	s.published = append(s.published, completion{eventID: eventID, claimToken: claimToken})
	return s.markPublishedErr
}

func (s *storeStub) MarkFailed(
	_ context.Context,
	eventID, claimToken string,
	retryAfter time.Duration,
	diagnostic string,
) error {
	s.failed = append(s.failed, failure{
		eventID: eventID, claimToken: claimToken, retryAfter: retryAfter, diagnostic: diagnostic,
	})
	return s.markFailedErr
}

type producerStub struct {
	messages []outbox.Message
	errors   []error
}

func (p *producerStub) Publish(_ context.Context, message outbox.Message) error {
	p.messages = append(p.messages, message)
	if len(p.errors) < len(p.messages) {
		return nil
	}
	return p.errors[len(p.messages)-1]
}

type completion struct {
	eventID    string
	claimToken string
}

type failure struct {
	eventID    string
	claimToken string
	retryAfter time.Duration
	diagnostic string
}

func mustPublisher(t testing.TB, store outbox.Store, producer outbox.Producer) *outbox.Publisher {
	t.Helper()
	publisher, err := outbox.NewPublisher(store, producer, testLogger(), validPublisherConfig())
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	return publisher
}

func validPublisherConfig() outbox.PublisherConfig {
	return outbox.PublisherConfig{
		BatchSize:      10,
		ClaimLease:     time.Minute,
		PollInterval:   time.Second,
		BaseRetryDelay: time.Second,
		MaxRetryDelay:  8 * time.Second,
	}
}

func claimedEvent(t testing.TB, eventID string, attempts int32) outbox.ClaimedEvent {
	t.Helper()
	aggregateID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	draft, err := event.NewDraft(event.DraftParams{
		EventType:        "transfer.completed",
		EventVersion:     1,
		AggregateType:    "transfer",
		AggregateID:      aggregateID,
		AggregateVersion: 1,
		CorrelationID:    aggregateID,
		OccurredAt:       time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC),
		Payload:          json.RawMessage(`{"transfer_id":"` + aggregateID + `"}`),
	})
	if err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
	envelope, err := event.NewEnvelope(eventID, draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	return outbox.ClaimedEvent{
		Envelope: envelope, PublishAttempts: attempts, ClaimToken: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
