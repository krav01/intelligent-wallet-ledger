package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	maxBatchSize  = 100
	maxClaimLease = 15 * time.Minute
	maxRetryDelay = 24 * time.Hour
)

// PublisherConfig controls bounded claiming, polling, and retry behavior.
type PublisherConfig struct {
	BatchSize      int
	ClaimLease     time.Duration
	PollInterval   time.Duration
	BaseRetryDelay time.Duration
	MaxRetryDelay  time.Duration
}

// BatchResult describes one bounded publication attempt.
type BatchResult struct {
	Claimed   int
	Published int
	Failed    int
}

// Publisher moves committed outbox events to a broker.
type Publisher struct {
	store    Store
	producer Producer
	logger   *slog.Logger
	config   PublisherConfig
}

// NewPublisher validates dependencies and creates a publisher.
func NewPublisher(
	store Store,
	producer Producer,
	logger *slog.Logger,
	config PublisherConfig,
) (*Publisher, error) {
	if store == nil {
		return nil, errors.New("outbox store is required")
	}
	if producer == nil {
		return nil, errors.New("outbox producer is required")
	}
	if logger == nil {
		return nil, errors.New("logger is required")
	}
	if err := validatePublisherConfig(config); err != nil {
		return nil, err
	}

	return &Publisher{store: store, producer: producer, logger: logger, config: config}, nil
}

// Run publishes batches until cancellation or an infrastructure error.
func (p *Publisher) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	<-timer.C
	defer timer.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		result, err := p.PublishBatch(ctx)
		if err != nil {
			return err
		}
		if result.Claimed > 0 {
			continue
		}

		timer.Reset(p.config.PollInterval)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
	}
}

// PublishBatch processes at most one configured claim batch.
func (p *Publisher) PublishBatch(ctx context.Context) (BatchResult, error) {
	claimed, err := p.store.Claim(ctx, p.config.BatchSize, p.config.ClaimLease)
	if err != nil {
		return BatchResult{}, fmt.Errorf("claiming outbox batch: %w", err)
	}
	result := BatchResult{Claimed: len(claimed)}
	for _, stored := range claimed {
		encoded, err := json.Marshal(stored.Envelope)
		if err != nil {
			return result, fmt.Errorf("encoding outbox event %s: %w", stored.Envelope.EventID(), err)
		}
		message := Message{
			Key:   stored.Envelope.AggregateType() + ":" + stored.Envelope.AggregateID(),
			Value: encoded,
		}
		if err := p.producer.Publish(ctx, message); err != nil {
			retryAfter := p.retryDelay(stored.PublishAttempts)
			if markErr := p.store.MarkFailed(
				ctx,
				stored.Envelope.EventID(),
				stored.ClaimToken,
				retryAfter,
				err.Error(),
			); markErr != nil {
				return result, fmt.Errorf(
					"publishing outbox event %s: %w; recording failure: %w",
					stored.Envelope.EventID(),
					err,
					markErr,
				)
			}
			result.Failed++
			p.logger.Warn(
				"outbox event publication scheduled for retry",
				"event_id", stored.Envelope.EventID(),
				"event_type", stored.Envelope.EventType(),
				"retry_after", retryAfter,
				"error", err,
			)
			continue
		}
		if err := p.store.MarkPublished(
			ctx,
			stored.Envelope.EventID(),
			stored.ClaimToken,
		); err != nil {
			return result, fmt.Errorf("marking outbox event %s published: %w", stored.Envelope.EventID(), err)
		}
		result.Published++
	}

	return result, nil
}

func validatePublisherConfig(config PublisherConfig) error {
	switch {
	case config.BatchSize <= 0 || config.BatchSize > maxBatchSize:
		return fmt.Errorf("outbox batch size must be between 1 and %d", maxBatchSize)
	case config.ClaimLease < time.Second || config.ClaimLease > maxClaimLease || config.ClaimLease%time.Second != 0:
		return fmt.Errorf("outbox claim lease must be whole seconds between 1s and %s", maxClaimLease)
	case config.PollInterval <= 0:
		return errors.New("outbox poll interval must be positive")
	case config.BaseRetryDelay < time.Second || config.BaseRetryDelay > maxRetryDelay || config.BaseRetryDelay%time.Second != 0:
		return fmt.Errorf("outbox base retry delay must be whole seconds between 1s and %s", maxRetryDelay)
	case config.MaxRetryDelay < config.BaseRetryDelay || config.MaxRetryDelay > maxRetryDelay || config.MaxRetryDelay%time.Second != 0:
		return fmt.Errorf("outbox maximum retry delay must be whole seconds between the base delay and %s", maxRetryDelay)
	default:
		return nil
	}
}

func (p *Publisher) retryDelay(previousFailures int32) time.Duration {
	delay := p.config.BaseRetryDelay
	for range min(previousFailures, 62) {
		if delay >= p.config.MaxRetryDelay/2 {
			return p.config.MaxRetryDelay
		}
		delay *= 2
	}
	return min(delay, p.config.MaxRetryDelay)
}
