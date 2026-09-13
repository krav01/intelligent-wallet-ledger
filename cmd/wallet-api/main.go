// Command wallet-api serves the public HTTP API.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/krav01/intelligent-wallet-ledger/internal/app/walletapi"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	config, err := walletapi.ConfigFromEnv()
	if err == nil {
		err = walletapi.Run(ctx, config, logger)
	}
	stop()
	if err != nil {
		logger.Error("wallet API stopped", "error", err)
		os.Exit(1)
	}
}
