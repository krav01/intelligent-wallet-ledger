// Package outbox coordinates durable event publication without owning infrastructure.
package outbox

import (
	"context"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
)

// ClaimedEvent contains an envelope, its failure count, and its fencing token.
type ClaimedEvent struct {
	Envelope        event.Envelope
	Sequence        int64
	PublishAttempts int32
	ClaimToken      string
}

// Message is the broker-independent representation of one integration event.
type Message struct {
	Key   string
	Value []byte
}

// Store leases and completes durable publication work.
type Store interface {
	Claim(ctx context.Context, limit int, lease time.Duration) ([]ClaimedEvent, error)
	MarkPublished(ctx context.Context, eventID, claimToken string) error
	MarkFailed(
		ctx context.Context,
		eventID, claimToken string,
		retryAfter time.Duration,
		diagnostic string,
	) error
}

// Producer publishes one message and returns only after its broker outcome is known.
type Producer interface {
	Publish(ctx context.Context, message Message) error
}
