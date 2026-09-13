// Package walletapi owns the wallet API process lifecycle and composition root.
package walletapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	reviewdecisionhttp "github.com/krav01/intelligent-wallet-ledger/internal/adapter/http/reviewdecision"
	"github.com/krav01/intelligent-wallet-ledger/internal/app/reviewcase"
	"github.com/krav01/intelligent-wallet-ledger/internal/identity/oidc"
	"github.com/krav01/intelligent-wallet-ledger/internal/platform/httpserver"
)

const (
	defaultAddress  = ":8080"
	shutdownTimeout = 10 * time.Second
)

// Config contains process-level configuration for the wallet API.
type Config struct {
	Address     string
	DatabaseURL string
	OIDC        OIDCConfig
}

// OIDCConfig enables analyst command authentication when it is complete.
type OIDCConfig struct {
	Issuer    string
	Audience  string
	RoleClaim string
}

// Enabled reports whether every OIDC setting is present.
func (c OIDCConfig) Enabled() bool {
	return c.Issuer != "" && c.Audience != "" && c.RoleClaim != ""
}

// ConfigFromEnv loads 12-factor process configuration from environment variables.
func ConfigFromEnv() (Config, error) {
	address := strings.TrimSpace(os.Getenv("HTTP_ADDRESS"))
	if address == "" {
		address = defaultAddress
	}
	config := Config{
		Address:     address,
		DatabaseURL: strings.TrimSpace(os.Getenv("DATABASE_URL")),
		OIDC: OIDCConfig{
			Issuer:    strings.TrimSpace(os.Getenv("OIDC_ISSUER")),
			Audience:  strings.TrimSpace(os.Getenv("OIDC_AUDIENCE")),
			RoleClaim: strings.TrimSpace(os.Getenv("OIDC_ROLE_CLAIM")),
		},
	}
	if err := validateOIDCConfig(config); err != nil {
		return Config{}, err
	}

	return config, nil
}

// Run serves requests until the context is cancelled or the server fails.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	if logger == nil {
		return errors.New("logger is required")
	}
	if cfg.Address == "" {
		return errors.New("HTTP address is required")
	}
	if err := validateOIDCConfig(cfg); err != nil {
		return err
	}
	routes, closePool, err := reviewDecisionRoutes(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer closePool()

	server := httpserver.New(cfg.Address, logger, routes)
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

func validateOIDCConfig(config Config) error {
	if config.OIDC.Enabled() {
		if config.DatabaseURL == "" {
			return errors.New("database URL is required when OIDC is configured")
		}
		return nil
	}
	if config.OIDC.Issuer != "" || config.OIDC.Audience != "" || config.OIDC.RoleClaim != "" {
		return errors.New("oidc issuer, audience, and role claim must be configured together")
	}
	return nil
}

func reviewDecisionRoutes(ctx context.Context, cfg Config, logger *slog.Logger) (httpserver.RouteRegistrar, func(), error) {
	if !cfg.OIDC.Enabled() {
		return nil, func() {}, nil
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("creating review decision database pool: %w", err)
	}
	closePool := func() { pool.Close() }
	authenticator, err := oidc.New(ctx, oidc.Config(cfg.OIDC))
	if err != nil {
		closePool()
		return nil, nil, fmt.Errorf("creating OIDC authenticator: %w", err)
	}
	decider, err := reviewcase.NewHandler(pool, time.Now)
	if err != nil {
		closePool()
		return nil, nil, fmt.Errorf("creating review decision handler: %w", err)
	}
	routes, err := reviewdecisionhttp.New(authenticator, decider, logger)
	if err != nil {
		closePool()
		return nil, nil, fmt.Errorf("creating review decision HTTP handler: %w", err)
	}
	return routes, closePool, nil
}
