// Package postgres persists Kafka poison-record quarantine entries in PostgreSQL.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxConsumerNameBytes = 128
	maxTopicBytes        = 249
	maxValueExcerptBytes = 65536

	// ReasonInvalidEnvelope identifies a malformed envelope or owned event payload.
	ReasonInvalidEnvelope = "invalid_envelope"
	// ReasonUnsupportedEventType identifies an event type outside the known contract.
	ReasonUnsupportedEventType = "unsupported_event_type"
)

const insertQuarantineQuery = `
INSERT INTO consumer_quarantine (
    consumer_name,
    topic,
    partition,
    record_offset,
    reason_code,
    value_excerpt,
    value_sha256,
    value_size,
    value_truncated
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (consumer_name, topic, partition, record_offset) DO NOTHING`

// ErrInvalidArgument indicates malformed quarantine input.
var ErrInvalidArgument = errors.New("quarantine repository: invalid argument")

// Record identifies a Kafka record and the reason it must be quarantined.
type Record struct {
	ConsumerName string
	Topic        string
	Partition    int32
	Offset       int64
	ReasonCode   string
	Value        []byte
}

// Repository stores poison records independently from financial transactions.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a PostgreSQL quarantine repository.
func NewRepository(pool *pgxpool.Pool) (*Repository, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: pool is required", ErrInvalidArgument)
	}
	return &Repository{pool: pool}, nil
}

// Store saves a bounded, idempotent quarantine entry before its Kafka offset is committed.
func (r *Repository) Store(ctx context.Context, record Record) error {
	if err := validateRecord(record); err != nil {
		return err
	}

	valueExcerpt := bytes.Clone(record.Value[:min(len(record.Value), maxValueExcerptBytes)])
	valueHash := sha256.Sum256(record.Value)
	_, err := r.pool.Exec(
		ctx,
		insertQuarantineQuery,
		record.ConsumerName,
		record.Topic,
		record.Partition,
		record.Offset,
		record.ReasonCode,
		valueExcerpt,
		valueHash[:],
		int64(len(record.Value)),
		len(record.Value) > len(valueExcerpt),
	)
	if err != nil {
		return fmt.Errorf("inserting consumer quarantine record: %w", err)
	}
	return nil
}

func validateRecord(record Record) error {
	if !validConsumerName(record.ConsumerName) {
		return fmt.Errorf("%w: consumer name", ErrInvalidArgument)
	}
	if record.Topic == "" || len(record.Topic) > maxTopicBytes {
		return fmt.Errorf("%w: topic", ErrInvalidArgument)
	}
	if record.Partition < 0 || record.Offset < 0 {
		return fmt.Errorf("%w: Kafka position", ErrInvalidArgument)
	}
	if record.ReasonCode != ReasonInvalidEnvelope && record.ReasonCode != ReasonUnsupportedEventType {
		return fmt.Errorf("%w: reason code", ErrInvalidArgument)
	}
	return nil
}

func validConsumerName(value string) bool {
	if value == "" || len(value) > maxConsumerNameBytes {
		return false
	}
	for index, character := range []byte(value) {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			continue
		}
		if index > 0 && (character == '.' || character == '_' || character == '-') {
			continue
		}
		return false
	}
	return true
}
