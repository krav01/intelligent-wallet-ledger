package walletapi

import (
	"log/slog"
	"testing"
)

func TestConfigFromEnv(t *testing.T) {
	t.Run("uses default address", func(t *testing.T) {
		t.Setenv("HTTP_ADDRESS", "")

		cfg := ConfigFromEnv()
		if cfg.Address != defaultAddress {
			t.Fatalf("Address = %q, want %q", cfg.Address, defaultAddress)
		}
	})

	t.Run("uses configured address", func(t *testing.T) {
		t.Setenv("HTTP_ADDRESS", "127.0.0.1:9090")

		cfg := ConfigFromEnv()
		if cfg.Address != "127.0.0.1:9090" {
			t.Fatalf("Address = %q, want %q", cfg.Address, "127.0.0.1:9090")
		}
	})
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
