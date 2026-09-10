// Command outbox-publisher publishes committed integration events to Kafka.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/krav01/intelligent-wallet-ledger/internal/app/outboxpublisher"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	config, err := outboxpublisher.ConfigFromEnv()
	if err != nil {
		logger.Error("outbox publisher configuration failed", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = outboxpublisher.Run(ctx, config, logger)
	stop()
	if err != nil {
		logger.Error("outbox publisher stopped", "error", err)
		os.Exit(1)
	}
}
