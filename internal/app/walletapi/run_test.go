package walletapi

import (
	"log/slog"
	"testing"
)

func TestConfigFromEnv(t *testing.T) {
	t.Run("uses default address", func(t *testing.T) {
		t.Setenv("HTTP_ADDRESS", "")

		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Address != defaultAddress {
			t.Fatalf("Address = %q, want %q", cfg.Address, defaultAddress)
		}
	})

	t.Run("uses configured address", func(t *testing.T) {
		t.Setenv("HTTP_ADDRESS", "127.0.0.1:9090")

		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Address != "127.0.0.1:9090" {
			t.Fatalf("Address = %q, want %q", cfg.Address, "127.0.0.1:9090")
		}
	})
}

func TestConfigFromEnvLoadsOIDC(t *testing.T) {
	t.Setenv("DATABASE_URL", " postgres://wallet.example/db ")
	t.Setenv("OIDC_ISSUER", " https://issuer.example ")
	t.Setenv("OIDC_AUDIENCE", " wallet-api ")
	t.Setenv("OIDC_ROLE_CLAIM", " roles ")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://wallet.example/db" || !cfg.OIDC.Enabled() ||
		cfg.OIDC.Issuer != "https://issuer.example" || cfg.OIDC.Audience != "wallet-api" || cfg.OIDC.RoleClaim != "roles" ||
		cfg.ReviewDecisionRateLimitPerMinute != defaultReviewDecisionRateLimitPerMinute {
		t.Errorf("ConfigFromEnv() = %+v, want normalized OIDC configuration", cfg)
	}
}

func TestConfigFromEnvLoadsReviewDecisionRateLimit(t *testing.T) {
	t.Setenv("REVIEW_DECISION_RATE_LIMIT_PER_MINUTE", " 24 ")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReviewDecisionRateLimitPerMinute != 24 {
		t.Errorf("ReviewDecisionRateLimitPerMinute = %d, want 24", cfg.ReviewDecisionRateLimitPerMinute)
	}
}

func TestConfigFromEnvRejectsInvalidReviewDecisionRateLimit(t *testing.T) {
	t.Setenv("REVIEW_DECISION_RATE_LIMIT_PER_MINUTE", "0")
	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("ConfigFromEnv() error = nil, want rate-limit configuration error")
	}
}

func TestConfigFromEnvRejectsPartialOIDC(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://issuer.example")
	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("ConfigFromEnv() error = nil, want OIDC configuration error")
	}
}

func TestRunValidatesConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cfg    Config
		logger *slog.Logger
	}{
		{name: "missing logger", cfg: Config{Address: defaultAddress}},
		{name: "missing address", logger: slog.New(slog.DiscardHandler)},
		{
			name:   "partial OIDC configuration",
			cfg:    Config{Address: defaultAddress, OIDC: OIDCConfig{Issuer: "https://issuer.example"}},
			logger: slog.New(slog.DiscardHandler),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := Run(t.Context(), tt.cfg, tt.logger); err == nil {
				t.Fatal("Run() error = nil, want validation error")
			}
		})
	}
}
