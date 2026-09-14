//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	quarantinepostgres "github.com/krav01/intelligent-wallet-ledger/internal/quarantine/postgres"
)

func TestRepositoryStoreDeduplicatesAndBoundsValue(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for integration tests")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)

	repository, err := quarantinepostgres.NewRepository(pool)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	record := quarantinepostgres.Record{
		ConsumerName: "quarantine-test.v1",
		Topic:        "wallet.quarantine.test",
		Partition:    0,
		Offset:       1,
		ReasonCode:   quarantinepostgres.ReasonInvalidEnvelope,
		Value:        bytes.Repeat([]byte("x"), 65537),
	}
	if _, err := pool.Exec(t.Context(), `DELETE FROM consumer_quarantine WHERE consumer_name = $1 AND topic = $2`, record.ConsumerName, record.Topic); err != nil {
		t.Fatalf("clearing consumer quarantine: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM consumer_quarantine WHERE consumer_name = $1 AND topic = $2`, record.ConsumerName, record.Topic); err != nil {
			t.Errorf("clearing consumer quarantine: %v", err)
		}
	})

	if err := repository.Store(t.Context(), record); err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	if err := repository.Store(t.Context(), record); err != nil {
		t.Fatalf("Store(duplicate) error = %v", err)
	}

	var (
		excerpt   []byte
		digest    []byte
		valueSize int64
		truncated bool
		count     int64
	)
	err = pool.QueryRow(t.Context(), `
SELECT value_excerpt, value_sha256, value_size, value_truncated,
       COUNT(*) OVER ()
FROM consumer_quarantine
WHERE consumer_name = $1 AND topic = $2 AND partition = $3 AND record_offset = $4`,
		record.ConsumerName,
		record.Topic,
		record.Partition,
		record.Offset,
	).Scan(&excerpt, &digest, &valueSize, &truncated, &count)
	if err != nil {
		t.Fatalf("querying consumer quarantine: %v", err)
	}
	wantDigest := sha256.Sum256(record.Value)
	if len(excerpt) != 65536 || !bytes.Equal(digest, wantDigest[:]) || valueSize != int64(len(record.Value)) || !truncated || count != 1 {
		t.Errorf("stored quarantine = excerpt:%d digest:%t size:%d truncated:%t count:%d, want 65536:true:%d:true:1", len(excerpt), bytes.Equal(digest, wantDigest[:]), valueSize, truncated, count, len(record.Value))
	}
}
