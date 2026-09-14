// Package httpserver provides the shared HTTP transport for deployable applications.
package httpserver

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
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
	return handlerWithRegistry(logger, routes, prometheus.NewRegistry())
}

func handlerWithRegistry(logger *slog.Logger, routes RouteRegistrar, registry *prometheus.Registry) http.Handler {
	metrics := newHTTPMetrics(registry)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health)
	mux.HandleFunc("GET /readyz", health)
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	if routes != nil {
		routes.RegisterRoutes(mux)
	}

	traced := traceContext(metrics.instrument(mux))
	return requestLogger(logger, traced)
}

func traceContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("wallet-api/http").Start(ctx, "wallet-api.http", trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
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
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		logger.InfoContext(r.Context(), "HTTP request",
			"method", r.Method,
			"route", route,
			"duration", time.Since(started),
		)
	})
}
