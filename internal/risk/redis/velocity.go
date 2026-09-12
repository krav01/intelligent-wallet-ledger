// Package redis provides Redis-backed risk infrastructure adapters.
package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

const velocityKeyPrefix = "risk:velocity:source-account:"

var (
	// ErrInvalidArgument indicates invalid Redis velocity observer configuration or input.
	ErrInvalidArgument = errors.New("risk redis: invalid argument")
	velocityScript     = `
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
redis.call("ZADD", KEYS[1], "NX", ARGV[2], ARGV[3])
redis.call("PEXPIRE", KEYS[1], ARGV[4])
return redis.call("ZCARD", KEYS[1])`
)

type commandClient interface {
	Eval(context.Context, string, []string, ...any) *redisclient.Cmd
	Close() error
}

// Observer captures a source-account transfer count in a Redis sliding window.
type Observer struct {
	client commandClient
	window time.Duration
}

// NewObserver creates a Redis velocity observer with bounded Redis command timeouts.
func NewObserver(address string, window time.Duration) (*Observer, error) {
	address = strings.TrimSpace(address)
	if address == "" || window < time.Millisecond {
		return nil, fmt.Errorf("%w: address and millisecond window are required", ErrInvalidArgument)
	}
	client := redisclient.NewClient(&redisclient.Options{
		Addr:         address,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		MaxRetries:   0,
	})
	return newObserver(client, window)
}

func newObserver(client commandClient, window time.Duration) (*Observer, error) {
	if client == nil || window < time.Millisecond {
		return nil, fmt.Errorf("%w: client and millisecond window are required", ErrInvalidArgument)
	}
	return &Observer{client: client, window: window}, nil
}

// Close releases the Redis client resources.
func (o *Observer) Close() error {
	if o == nil || o.client == nil {
		return nil
	}
	return o.client.Close()
}

// Observe adds a transfer once and returns the count in its source-account sliding window.
func (o *Observer) Observe(
	ctx context.Context,
	sourceAccountID string,
	transferID string,
	observedAt time.Time,
) (int, error) {
	if o == nil || o.client == nil || strings.TrimSpace(sourceAccountID) == "" ||
		strings.TrimSpace(transferID) == "" || observedAt.IsZero() {
		return 0, fmt.Errorf("%w: observation", ErrInvalidArgument)
	}
	observedAt = observedAt.UTC()
	count, err := o.client.Eval(
		ctx,
		velocityScript,
		[]string{velocityKeyPrefix + sourceAccountID},
		observedAt.Add(-o.window).UnixMilli(),
		observedAt.UnixMilli(),
		transferID,
		o.window.Milliseconds(),
	).Int()
	if err != nil {
		return 0, fmt.Errorf("capturing velocity observation: %w", err)
	}
	if count < 1 {
		return 0, fmt.Errorf("%w: non-positive velocity count", ErrInvalidArgument)
	}
	return count, nil
}
