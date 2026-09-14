package httpserver

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNewConfiguresHeaderLimit(t *testing.T) {
	t.Parallel()

	server := New("127.0.0.1:8080", slog.New(slog.DiscardHandler), nil)
	if server.MaxHeaderBytes != maxHeaderBytes {
		t.Errorf("MaxHeaderBytes = %d, want %d", server.MaxHeaderBytes, maxHeaderBytes)
	}
}

func TestHandlerLogsStaticRoute(t *testing.T) {
	var output bytes.Buffer
	handler := Handler(slog.New(slog.NewTextHandler(&output, nil)), nil)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/unknown%0Aforged=true", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if !strings.Contains(output.String(), "route=unmatched") {
		t.Errorf("log output = %q, want unmatched route", output.String())
	}
	if strings.Contains(output.String(), "forged=true") {
		t.Errorf("log output includes client path: %q", output.String())
	}
}

func TestHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		statusCode int
		body       string
	}{
		{name: "health", path: "/healthz", statusCode: http.StatusOK, body: "{\"status\":\"ok\"}\n"},
		{name: "readiness", path: "/readyz", statusCode: http.StatusOK, body: "{\"status\":\"ok\"}\n"},
		{name: "unknown route", path: "/unknown", statusCode: http.StatusNotFound, body: "404 page not found\n"},
	}

	handler := Handler(slog.New(slog.DiscardHandler), nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != tt.statusCode {
				t.Fatalf("status = %d, want %d", response.Code, tt.statusCode)
			}
			if response.Body.String() != tt.body {
				t.Fatalf("body = %q, want %q", response.Body.String(), tt.body)
			}
		})
	}
}

func TestHandlerExposesBoundedRequestMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler := handlerWithRegistry(slog.New(slog.DiscardHandler), nil, registry)

	for _, path := range []string{"/healthz", "/unknown%0Aforged=true"} {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
	}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", response.Code, http.StatusOK)
	}
	output := response.Body.String()
	if !strings.Contains(output, `wallet_http_requests_total{method="GET",route="GET /healthz",status="200"} 1`) {
		t.Errorf("metrics = %q, want health request counter", output)
	}
	if !strings.Contains(output, `wallet_http_request_duration_seconds_count{method="GET",route="GET /healthz",status="200"} 1`) {
		t.Errorf("metrics = %q, want health duration count", output)
	}
	if !strings.Contains(output, `wallet_http_requests_total{method="GET",route="unmatched",status="404"} 1`) {
		t.Errorf("metrics = %q, want unmatched route counter", output)
	}
	if strings.Contains(output, "forged=true") {
		t.Errorf("metrics include client-controlled path: %q", output)
	}
}

func TestHandlerPropagatesTraceParent(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(t.Context())
		otel.SetTracerProvider(previous)
	})

	handler := Handler(slog.New(slog.DiscardHandler), nil)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil)
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	if spans[0].Name() != "wallet-api.http" {
		t.Errorf("span name = %q, want wallet-api.http", spans[0].Name())
	}
	if got, want := spans[0].SpanContext().TraceID().String(), "4bf92f3577b34da6a3ce929d0e0e4736"; got != want {
		t.Errorf("trace ID = %q, want %q", got, want)
	}
	if got := spans[0].Attributes(); len(got) != 0 {
		t.Errorf("span attributes = %v, want none", got)
	}
}
