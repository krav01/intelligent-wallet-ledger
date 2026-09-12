// Command risk-worker consumes transfer requests and persists deterministic risk decisions.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/krav01/intelligent-wallet-ledger/internal/app/riskworker"
	riskredis "github.com/krav01/intelligent-wallet-ledger/internal/risk/redis"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	config, err := riskworker.ConfigFromEnv()
	if err != nil {
		logger.Error("risk worker configuration failed", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var observer *riskredis.Observer
	var velocity riskworker.VelocityObserver
	if config.Velocity.Enabled() {
		var observerErr error
		observer, observerErr = riskredis.NewObserver(config.Velocity.RedisAddress, config.Velocity.Window)
		if observerErr != nil {
			logger.Error("velocity observer configuration failed", "error", observerErr)
			os.Exit(1)
		}
		velocity = observer
	}
	err = riskworker.Run(ctx, config, velocity, logger)
	stop()
	if observer != nil {
		if closeErr := observer.Close(); closeErr != nil {
			logger.Warn("closing velocity observer", "error", closeErr)
		}
	}
	if err != nil {
		logger.Error("risk worker stopped", "error", err)
		os.Exit(1)
	}
}
