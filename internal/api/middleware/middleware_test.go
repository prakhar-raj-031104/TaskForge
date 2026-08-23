package middleware

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestChainOrdering pins down the contract that the first argument is the
// outermost wrapper. Getting this backwards silently breaks Recover (it would
// no longer wrap the handler) with no compile error.
func TestChainOrdering(t *testing.T) {
	t.Parallel()

	var order []string

	mark := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name+":in")
				next.ServeHTTP(w, r)
				order = append(order, name+":out")
			})
		}
	}

	h := Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		order = append(order, "handler")
	}), mark("a"), mark("b"))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	want := "a:in b:in handler b:out a:out"
	if got := strings.Join(order, " "); got != want {
		t.Errorf("order = %q, want %q", got, want)
	}
}

func TestRequestIDGeneratesWhenAbsent(t *testing.T) {
	t.Parallel()

	var seen string
	h := RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpx.RequestID(r.Context())
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if seen == "" {
		t.Fatal("no request id was placed in the context")
	}
	if got := rec.Header().Get(HeaderRequestID); got != seen {
		t.Errorf("response header = %q, context = %q; they must match", got, seen)
	}
}

func TestRequestIDHonoursInboundHeader(t *testing.T) {
	t.Parallel()

	var seen string
	h := RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpx.RequestID(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(HeaderRequestID, "upstream-trace-42")

	h.ServeHTTP(httptest.NewRecorder(), req)

	if seen != "upstream-trace-42" {
		t.Errorf("request id = %q, want the inbound value preserved", seen)
	}
}

// TestRequestIDSanitizesHostileInput covers log forging: a client-supplied id
// containing a newline must not be able to inject fake log lines.
func TestRequestIDSanitizesHostileInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, in, want string
	}{
		// '=' is not in the allowlist either, so the forged key=value pairs are
		// dismantled as well as the newline.
		{"newline injection", "abc\nlevel=ERROR msg=fake", "abclevelERRORmsgfake"},
		{"carriage return", "abc\r\ndef", "abcdef"},
		{"quotes and spaces", `a" b'c`, "abc"},
		{"only illegal characters", "!@#$%^&*()", ""},
		{"legal characters kept", "trace-id_1.2", "trace-id_1.2"},
		{"over-long is truncated", strings.Repeat("a", 200), strings.Repeat("a", maxInboundRequestIDLength)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var seen string
			h := RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen = httpx.RequestID(r.Context())
			}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set(HeaderRequestID, tt.in)
			h.ServeHTTP(httptest.NewRecorder(), req)

			// An input that sanitizes to nothing falls back to a generated id.
			if tt.want == "" {
				if seen == "" {
					t.Error("expected a generated id when the inbound one was entirely illegal")
				}
				return
			}
			if seen != tt.want {
				t.Errorf("id = %q, want %q", seen, tt.want)
			}
			if strings.ContainsAny(seen, "\r\n") {
				t.Error("sanitized id still contains a line break")
			}
		})
	}
}

func TestRecoverTurnsPanicIntoJSON500(t *testing.T) {
	t.Parallel()

	h := Chain(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("handler exploded")
		}),
		RequestID(),
		Logger(discardLogger()),
		Recover(),
	)

	rec := httptest.NewRecorder()
	// If Recover failed, this call panics and the test fails loudly.
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content type = %q, want JSON", ct)
	}
	// The panic message must not reach the client.
	if strings.Contains(rec.Body.String(), "handler exploded") {
		t.Errorf("the panic message leaked to the client: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "internal_error") {
		t.Errorf("body = %s, want the internal_error envelope", rec.Body.String())
	}
}

func TestRecoverLeavesAWrittenResponseAlone(t *testing.T) {
	t.Parallel()

	h := Chain(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"ok":true}`))
			panic("after the response was sent")
		}),
		Logger(discardLogger()),
		Recover(),
	)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	// The status was already committed; recovery must not try to change it.
	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want the original 201", rec.Code)
	}
}

func TestTimeoutSetsADeadline(t *testing.T) {
	t.Parallel()

	var hasDeadline bool
	h := Timeout(50 * time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hasDeadline = r.Context().Deadline()
	}))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	if !hasDeadline {
		t.Error("the handler's context carries no deadline")
	}
}

func TestJSONErrorsRewritesStdlibNotFound(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /known", func(w http.ResponseWriter, r *http.Request) {})

	h := Chain(mux, RequestID(), JSONErrors())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/unknown", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content type = %q, want JSON", ct)
	}
	if strings.Contains(rec.Body.String(), "page not found") {
		t.Errorf("the stdlib plain-text body survived: %s", rec.Body.String())
	}
}

// TestJSONErrorsPreserves405 is the regression test for the catch-all route
// bug: registering mux.HandleFunc("/", ...) makes this return 404.
func TestJSONErrorsPreserves405(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("POST /api/v1/jobs", func(w http.ResponseWriter, r *http.Request) {})

	h := Chain(mux, RequestID(), JSONErrors())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/jobs", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, "POST") {
		t.Errorf("Allow = %q, want it to list POST", allow)
	}
	if !strings.Contains(rec.Body.String(), "method_not_allowed") {
		t.Errorf("body = %s, want the method_not_allowed envelope", rec.Body.String())
	}
}

// TestJSONErrorsLeavesHandlerJSONAlone makes sure a genuine application 404
// (job not found) passes through untouched.
func TestJSONErrorsLeavesHandlerJSONAlone(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(r.Context(), w, http.StatusNotFound, httpx.CodeNotFound, "job not found")
	})

	h := Chain(mux, RequestID(), JSONErrors())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/abc", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "job not found") {
		t.Errorf("the handler's own message was replaced: %s", rec.Body.String())
	}
}

func TestLoggerRecordsTheFinalStatus(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, nil))

	h := Chain(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte("short and stout"))
		}),
		RequestID(),
		Logger(log),
	)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/brew", nil))

	out := buf.String()
	for _, want := range []string{"status=418", "bytes=15", "path=/brew", "request_id="} {
		if !strings.Contains(out, want) {
			t.Errorf("log line %q is missing %q", out, want)
		}
	}
}
