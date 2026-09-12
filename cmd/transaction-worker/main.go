// Command transaction-worker posts risk-approved transfers to the ledger.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/krav01/intelligent-wallet-ledger/internal/app/transactionworker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := transactionworker.Run(ctx, transactionworker.ConfigFromEnv(), logger)
	stop()
	if err != nil {
		logger.Error("transaction worker stopped", "error", err)
		os.Exit(1)
	}
}
