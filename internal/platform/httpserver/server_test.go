package httpserver

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

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

	handler := Handler(slog.New(slog.DiscardHandler))
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
