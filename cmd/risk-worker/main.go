// Command risk-worker consumes transfer requests and persists deterministic risk decisions.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/krav01/intelligent-wallet-ledger/internal/app/riskworker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	config, err := riskworker.ConfigFromEnv()
	if err != nil {
		logger.Error("risk worker configuration failed", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = riskworker.Run(ctx, config, logger)
	stop()
	if err != nil {
		logger.Error("risk worker stopped", "error", err)
		os.Exit(1)
	}
}
