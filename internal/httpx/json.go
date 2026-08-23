package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ContentTypeJSON is the media type this API speaks.
const ContentTypeJSON = "application/json; charset=utf-8"

// WriteJSON serialises v and writes it with the given status code.
//
// It marshals into a buffer BEFORE touching the ResponseWriter. Encoding
// straight to the writer would emit the status line and half a JSON document
// before hitting a marshalling error, leaving the client with a 200 and a
// truncated body that no error handling can undo.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		// The value we were handed is unserialisable, which is a programming
		// error. Report a clean 500 rather than a corrupt 200.
		w.Header().Set("Content-Type", ContentTypeJSON)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"failed to encode response"}}`))
		return
	}

	w.Header().Set("Content-Type", ContentTypeJSON)
	// Content-Length lets clients and proxies avoid chunked encoding for what
	// are almost always small bodies.
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	// Header mutations after WriteHeader are silently dropped, so every header
	// must be set before this line.
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// WriteNoContent writes a 204 with no body.
func WriteNoContent(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }

// DecodeError describes why a request body could not be decoded, carrying the
// HTTP status the caller should return.
type DecodeError struct {
	Status  int
	Message string
}

func (e *DecodeError) Error() string { return e.Message }

// DecodeJSON reads exactly one JSON value from the request body into dst.
//
// The default encoding/json behaviour is too permissive for an API:
//   - an unbounded body lets one client exhaust server memory
//   - unknown fields are silently ignored, so a client typing "priorty" gets
//     the default priority and no indication anything went wrong
//   - trailing content after the JSON value is accepted
//
// This function closes all three, and turns the driver's rather cryptic errors
// into messages a client can act on.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) error {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		if mediaType := strings.TrimSpace(strings.Split(ct, ";")[0]); mediaType != "application/json" {
			return &DecodeError{
				Status:  http.StatusUnsupportedMediaType,
				Message: fmt.Sprintf("Content-Type must be application/json, got %q", mediaType),
			}
		}
	}

	// MaxBytesReader caps the body AND signals the server to close the
	// connection, so a client streaming an endless body cannot hold a
	// goroutine forever.
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return decodeError(err, maxBytes)
	}

	// A second Decode must report EOF. Anything else means the body held more
	// than one JSON value, e.g. `{"a":1}{"b":2}`.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return &DecodeError{
			Status:  http.StatusBadRequest,
			Message: "body must contain exactly one JSON object",
		}
	}

	return nil
}

func decodeError(err error, maxBytes int64) error {
	var (
		syntaxErr      *json.SyntaxError
		typeErr        *json.UnmarshalTypeError
		maxBytesErr    *http.MaxBytesError
		invalidDstErr  *json.InvalidUnmarshalError
		unknownFieldRe = "json: unknown field "
	)

	switch {
	case errors.As(err, &syntaxErr):
		return &DecodeError{
			Status:  http.StatusBadRequest,
			Message: fmt.Sprintf("body contains malformed JSON at byte %d", syntaxErr.Offset),
		}

	case errors.Is(err, io.ErrUnexpectedEOF):
		return &DecodeError{
			Status:  http.StatusBadRequest,
			Message: "body contains malformed JSON",
		}

	case errors.As(err, &typeErr):
		if typeErr.Field != "" {
			return &DecodeError{
				Status:  http.StatusBadRequest,
				Message: fmt.Sprintf("field %q must be of type %s", typeErr.Field, typeErr.Type),
			}
		}
		return &DecodeError{
			Status:  http.StatusBadRequest,
			Message: fmt.Sprintf("body contains a value of the wrong type at byte %d", typeErr.Offset),
		}

	case errors.Is(err, io.EOF):
		return &DecodeError{
			Status:  http.StatusBadRequest,
			Message: "body must not be empty",
		}

	case errors.As(err, &maxBytesErr):
		return &DecodeError{
			Status:  http.StatusRequestEntityTooLarge,
			Message: fmt.Sprintf("body must not exceed %d bytes", maxBytes),
		}

	case strings.HasPrefix(err.Error(), unknownFieldRe):
		// encoding/json offers no typed error for this one, so a string prefix
		// is the only option available.
		field := strings.TrimPrefix(err.Error(), unknownFieldRe)
		return &DecodeError{
			Status:  http.StatusBadRequest,
			Message: fmt.Sprintf("body contains unknown field %s", field),
		}

	case errors.As(err, &invalidDstErr):
		// We passed a non-pointer to Decode: our bug, not the client's.
		return fmt.Errorf("decode into invalid destination: %w", err)

	default:
		return &DecodeError{
			Status:  http.StatusBadRequest,
			Message: "body could not be decoded",
		}
	}
}
