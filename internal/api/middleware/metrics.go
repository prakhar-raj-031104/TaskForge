package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/metrics"
)

// RouteResolver returns the route PATTERN that will handle a request, e.g.
// "GET /api/v1/jobs/{id}". It returns "" when nothing matches.
type RouteResolver func(*http.Request) string

// Metrics records request counts, latency and concurrency.
//
// # Why the route label needs a resolver
//
// The label must be the route PATTERN, never the request path. That is not a
// stylistic choice: /api/v1/jobs/{id} is one time series, whereas labelling
// with the raw path would create one series per job id ever requested and grow
// without bound until Prometheus falls over. Unbounded label cardinality is the
// single most common way to break a monitoring system.
//
// The obvious way to get the pattern is http.Request.Pattern, which Go
// populates on the matched route. The catch is that ServeMux sets it on the
// CLONE of the request it passes inward, so any middleware wrapped around the
// mux still holds the original and sees an empty string. Reading r.Pattern here
// silently labels every single request "unmatched" - a bug that produces
// plausible-looking metrics that are entirely useless.
//
// So the router hands us a resolver that asks the mux directly. It costs a
// second routing lookup per request, which is a tree walk against an in-memory
// structure and immeasurable next to a database round trip.
func Metrics(m *metrics.Metrics, resolveRoute RouteResolver) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			m.HTTPInFlight.Inc()
			defer m.HTTPInFlight.Dec()

			route := "unmatched"
			if resolveRoute != nil {
				if pattern := resolveRoute(r); pattern != "" {
					route = pattern
				}
			}

			start := time.Now()
			rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			m.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
			m.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		})
	}
}
