package middleware

import (
	"net/http"
	"slices"
	"strconv"
	"time"
)

// preflightMaxAge is how long a browser may cache a preflight response before
// asking again. An hour is generous enough to avoid a preflight round trip
// before every POST while the allowed origins are effectively static.
const preflightMaxAge = time.Hour

// CORS allows requests from an explicit set of origins.
//
// Closed by default: an empty allowlist means every cross-origin request is
// refused, which is the correct default for an API with no authentication
// layer in front of it (see the README's "Accepted limitations"). The
// frontend's origin is added explicitly via configuration, never inferred
// or wildcarded, so enabling this for a browser demo cannot accidentally
// open the API to every site on the internet.
func CORS(allowedOrigins []string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Every response that varies by Origin must say so, even when the
			// origin does not match — otherwise a shared cache (or the browser
			// itself) can serve one origin's CORS-enabled response to another.
			w.Header().Add("Vary", "Origin")

			if origin == "" || !slices.Contains(allowedOrigins, origin) {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "false")

			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				// The preflight itself: answer it here and never reach the mux, so
				// an OPTIONS request does not need a route of its own and cannot
				// be counted as a 405 in the metrics or access log.
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
				w.Header().Set("Access-Control-Max-Age", strconv.Itoa(int(preflightMaxAge.Seconds())))
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
