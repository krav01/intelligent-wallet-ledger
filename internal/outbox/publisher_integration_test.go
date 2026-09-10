//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	inboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/inbox/postgres"
	"github.com/krav01/intelligent-wallet-ledger/internal/outbox"
	outboxkafka "github.com/krav01/intelligent-wallet-ledger/internal/outbox/kafka"
	outboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/outbox/postgres"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPublisherRedeliveryKeepsEventIdentityAndConsumerEffectOnce(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for integration tests")
	}
	brokers := integrationKafkaBrokers(t)
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)

	transferID := databaseUUID(t, pool)
	ownerID := databaseUUID(t, pool)
	draft, err := event.NewDraft(event.DraftParams{
		EventType:        "transfer.completed",
		EventVersion:     1,
		AggregateType:    "transfer",
		AggregateID:      transferID,
		AggregateVersion: 1,
		CorrelationID:    transferID,
		OccurredAt:       time.Now().UTC().Truncate(time.Microsecond),
		Payload:          json.RawMessage(`{"transfer_id":"` + transferID + `"}`),
	})
	if err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	envelope, err := outboxpostgres.AddTx(t.Context(), tx, draft)
	if err != nil {
		if rollbackErr := tx.Rollback(t.Context()); rollbackErr != nil {
			t.Errorf("Rollback() after AddTx error = %v", rollbackErr)
		}
		t.Fatalf("AddTx() error = %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM consumer_inbox WHERE event_id = $1`, envelope.EventID()); err != nil {
			t.Errorf("cleaning consumer inbox: %v", err)
		}
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM wallets WHERE owner_id = $1`, ownerID); err != nil {
			t.Errorf("cleaning consumer side effect: %v", err)
		}
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM outbox_events WHERE event_id = $1`, envelope.EventID()); err != nil {
			t.Errorf("cleaning outbox event: %v", err)
		}
	})

	topic := fmt.Sprintf("wallet.events.redelivery.%d", time.Now().UnixNano())
	producer, err := outboxkafka.NewProducer(outboxkafka.ProducerConfig{
		Brokers:                brokers,
		Topic:                  topic,
		ClientID:               "outbox-redelivery-integration",
		DeliveryTimeout:        10 * time.Second,
		MaxBufferedRecords:     10,
		AllowAutoTopicCreation: true,
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := producer.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	repository, err := outboxpostgres.NewRepository(pool)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	store := &loseFirstPublishedMarkStore{Repository: repository}
	publisher, err := outbox.NewPublisher(
		store,
		producer,
		slog.New(slog.DiscardHandler),
		outbox.PublisherConfig{
			BatchSize:      1,
			ClaimLease:     time.Minute,
			PollInterval:   time.Second,
			BaseRetryDelay: time.Second,
			MaxRetryDelay:  time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	if result, err := publisher.PublishBatch(ctx); err == nil || result.Claimed != 1 {
		t.Fatalf("first PublishBatch() = (%+v, %v), want one claim and lost completion", result, err)
	}
	if _, err := pool.Exec(
		ctx,
		`UPDATE outbox_events SET claimed_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE event_id = $1`,
		envelope.EventID(),
	); err != nil {
		t.Fatalf("expiring simulated crashed claim: %v", err)
	}
	if result, err := publisher.PublishBatch(ctx); err != nil || result.Published != 1 {
		t.Fatalf("second PublishBatch() = (%+v, %v), want one published event", result, err)
	}

	records := consumeRecords(t, ctx, brokers, topic, 2)
	for index, record := range records {
		if string(record.Key) != "transfer:"+transferID {
			t.Errorf("record %d key = %q, want transfer key", index, record.Key)
		}
		decoded, err := event.ParseEnvelope(record.Value)
		if err != nil {
			t.Fatalf("ParseEnvelope(record %d) error = %v", index, err)
		}
		if decoded.EventID() != envelope.EventID() {
			t.Errorf("record %d event ID = %s, want %s", index, decoded.EventID(), envelope.EventID())
		}
		processConsumerRecord(t, ctx, pool, ownerID, decoded)
	}
	assertRowCount(t, pool, `SELECT count(*) FROM consumer_inbox WHERE event_id = $1`, envelope.EventID(), 1)
	assertRowCount(t, pool, `SELECT count(*) FROM wallets WHERE owner_id = $1`, ownerID, 1)
	assertRowCount(t, pool, `SELECT count(*) FROM outbox_events WHERE event_id = $1 AND published_at IS NOT NULL`, envelope.EventID(), 1)
}

type loseFirstPublishedMarkStore struct {
	*outboxpostgres.Repository
	lost bool
}

func (s *loseFirstPublishedMarkStore) MarkPublished(ctx context.Context, eventID, claimToken string) error {
	if !s.lost {
		s.lost = true
		return errors.New("simulated crash after Kafka acknowledgement")
	}
	return s.Repository.MarkPublished(ctx, eventID, claimToken)
}

func integrationKafkaBrokers(t testing.TB) []string {
	t.Helper()
	value := os.Getenv("KAFKA_BROKERS")
	if value == "" {
		t.Skip("KAFKA_BROKERS is required for Kafka integration tests")
	}
	var brokers []string
	for broker := range strings.SplitSeq(value, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		t.Fatal("KAFKA_BROKERS contains no broker addresses")
	}
	return brokers
}

func databaseUUID(t testing.TB, pool *pgxpool.Pool) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(context.Background(), `SELECT gen_random_uuid()::text`).Scan(&id); err != nil {
		t.Fatalf("generating UUID: %v", err)
	}
	return id
}

func consumeRecords(
	t testing.TB,
	ctx context.Context,
	brokers []string,
	topic string,
	want int,
) []*kgo.Record {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("creating Kafka consumer: %v", err)
	}
	defer consumer.Close()
	records := make([]*kgo.Record, 0, want)
	for len(records) < want {
		fetches := consumer.PollFetches(ctx)
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			t.Fatalf("polling Kafka: %v", fetchErrors)
		}
		records = append(records, fetches.Records()...)
		if err := ctx.Err(); err != nil {
			t.Fatalf("waiting for Kafka records: %v", err)
		}
	}
	return records[:want]
}

func processConsumerRecord(
	t testing.TB,
	ctx context.Context,
	pool *pgxpool.Pool,
	ownerID string,
	envelope event.Envelope,
) {
	t.Helper()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("beginning consumer transaction: %v", err)
	}
	reserved, err := inboxpostgres.ReserveTx(
		ctx,
		tx,
		"redelivery-proof.v1",
		envelope,
	)
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			t.Errorf("rolling back consumer transaction: %v", rollbackErr)
		}
		t.Fatalf("ReserveTx() error = %v", err)
	}
	if reserved {
		if _, err := tx.Exec(ctx, `INSERT INTO wallets (owner_id) VALUES ($1)`, ownerID); err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				t.Errorf("rolling back consumer side effect: %v", rollbackErr)
			}
			t.Fatalf("inserting consumer side effect: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing consumer transaction: %v", err)
	}
}

func assertRowCount(
	t testing.TB,
	pool *pgxpool.Pool,
	query string,
	argument string,
	want int,
) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), query, argument).Scan(&got); err != nil {
		t.Fatalf("counting integration rows: %v", err)
	}
	if got != want {
		t.Errorf("row count = %d, want %d", got, want)
	}
}
