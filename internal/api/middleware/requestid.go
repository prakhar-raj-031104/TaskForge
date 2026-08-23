package middleware

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
)

// HeaderRequestID is the header used to receive and echo a request id.
const HeaderRequestID = "X-Request-ID"

// maxInboundRequestIDLength bounds a client-supplied id.
//
// The value ends up in every log line for the request. Without a cap, a client
// could send a megabyte header and have it copied into your log pipeline
// thousands of times — cheap for them, expensive for you.
const maxInboundRequestIDLength = 64

// RequestID assigns every request a unique identifier, stores it in the
// context, and echoes it in the response.
//
// It honours an inbound X-Request-ID so a trace started by an upstream gateway
// survives the hop. That is also why the value is validated rather than
// trusted: it is attacker-controlled input that will be written to logs.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := sanitizeRequestID(r.Header.Get(HeaderRequestID))
			if id == "" {
				id = uuid.NewString()
			}

			// Set on the response before calling next: headers written after a
			// handler has sent its status are silently discarded.
			w.Header().Set(HeaderRequestID, id)

			ctx := httpx.WithRequestID(r.Context(), id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// sanitizeRequestID keeps only safe, printable characters and truncates.
//
// Newlines are the important exclusion: an id containing "\n" lets a caller
// inject fabricated lines into a text log, which is log forging.
func sanitizeRequestID(raw string) string {
	if raw == "" {
		return ""
	}
	if len(raw) > maxInboundRequestIDLength {
		raw = raw[:maxInboundRequestIDLength]
	}

	out := make([]byte, 0, len(raw))
	for i := range len(raw) {
		c := raw[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
			out = append(out, c)
		}
	}
	return string(out)
}
