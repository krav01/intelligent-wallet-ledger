// Package httpserver provides the shared HTTP transport for deployable applications.
package httpserver

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

const (
	readHeaderTimeout = 2 * time.Second
	readTimeout       = 5 * time.Second
	writeTimeout      = 10 * time.Second
	idleTimeout       = 60 * time.Second
	maxHeaderBytes    = 64 << 10
)

// RouteRegistrar adds application-specific routes to the shared server mux.
type RouteRegistrar interface {
	RegisterRoutes(*http.ServeMux)
}

// New creates a hardened HTTP server with operational endpoints.
func New(address string, logger *slog.Logger, routes RouteRegistrar) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           Handler(logger, routes),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
}

// Handler returns the wallet API HTTP routes.
func Handler(logger *slog.Logger, routes RouteRegistrar) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health)
	mux.HandleFunc("GET /readyz", health)
	if routes != nil {
		routes.RegisterRoutes(mux)
	}

	return requestLogger(logger, mux)
}

func health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.InfoContext(r.Context(), "HTTP request",
			"method", r.Method,
			"path", r.URL.Path,
			"duration", time.Since(started),
		)
	})
}
