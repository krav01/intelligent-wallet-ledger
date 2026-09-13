package httpserver

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type httpMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newHTTPMetrics(registry *prometheus.Registry) *httpMetrics {
	metrics := &httpMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wallet",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total HTTP requests served by method, route, and status.",
		}, []string{"method", "route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "wallet",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request latency in seconds by method, route, and status.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route", "status"}),
	}
	registry.MustRegister(metrics.requests, metrics.duration)
	return metrics
}

func (m *httpMetrics) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		response := &statusResponseWriter{ResponseWriter: writer}
		next.ServeHTTP(response, request)

		route := request.Pattern
		if route == "" {
			route = "unmatched"
		}
		labels := prometheus.Labels{
			"method": request.Method,
			"route":  route,
			"status": strconv.Itoa(response.status()),
		}
		m.requests.With(labels).Inc()
		m.duration.With(labels).Observe(time.Since(started).Seconds())
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (w *statusResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.statusCode = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusResponseWriter) status() int {
	if !w.wroteHeader {
		return http.StatusOK
	}
	return w.statusCode
}
