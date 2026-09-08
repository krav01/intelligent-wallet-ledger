// Package walletapi owns the wallet API process lifecycle and composition root.
package walletapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/platform/httpserver"
)

const (
	defaultAddress  = ":8080"
	shutdownTimeout = 10 * time.Second
)

// Config contains process-level configuration for the wallet API.
type Config struct {
	Address string
}

// ConfigFromEnv loads 12-factor process configuration from environment variables.
func ConfigFromEnv() Config {
	address := os.Getenv("HTTP_ADDRESS")
	if address == "" {
		address = defaultAddress
	}

	return Config{Address: address}
}

// Run serves requests until the context is cancelled or the server fails.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	if logger == nil {
		return errors.New("logger is required")
	}
	if cfg.Address == "" {
		return errors.New("HTTP address is required")
	}

	server := httpserver.New(cfg.Address, logger)
	errCh := make(chan error, 1)

	go func() {
		logger.Info("wallet API listening", "address", cfg.Address)
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}

		return nil
	}
}
