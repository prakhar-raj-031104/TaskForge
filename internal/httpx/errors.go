package httpx

import (
	"context"
	"net/http"
)

// Machine-readable error codes. Clients branch on these; the human-readable
// message is free to change without breaking anyone.
const (
	CodeValidationFailed  = "validation_failed"
	CodeBadRequest        = "bad_request"
	CodeNotFound          = "not_found"
	CodeConflict          = "conflict"
	CodeMethodNotAllowed  = "method_not_allowed"
	CodeUnsupportedMedia  = "unsupported_media_type"
	CodePayloadTooLarge   = "payload_too_large"
	CodeTimeout           = "timeout"
	CodeInternalError     = "internal_error"
	CodeServiceUnavailabe = "service_unavailable"
)

// FieldError reports a problem with one input field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ErrorBody is the error envelope every non-2xx response uses.
//
// One shape for every error means a client writes one error handler. The
// request id is echoed so a user reporting "it failed" hands you the exact
// string needed to find the request in the logs.
type ErrorBody struct {
	Code      string       `json:"code"`
	Message   string       `json:"message"`
	Fields    []FieldError `json:"fields,omitempty"`
	RequestID string       `json:"request_id,omitempty"`
}

// ErrorResponse wraps ErrorBody so the payload is {"error": {...}} rather than
// a bare object, leaving room to add sibling keys later without a breaking
// change.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// WriteError writes a JSON error response.
//
// message is shown to the client, so it must never contain internal detail:
// no SQL, no stack traces, no connection strings. Those belong in the log line,
// correlated by request id.
func WriteError(ctx context.Context, w http.ResponseWriter, status int, code, message string) {
	WriteJSON(w, status, ErrorResponse{Error: ErrorBody{
		Code:      code,
		Message:   message,
		RequestID: RequestID(ctx),
	}})
}

// WriteValidationError writes a 422 listing every field that failed.
//
// 422 rather than 400: the request was syntactically valid JSON that we
// understood, but its contents violate the rules. 400 is reserved here for
// bodies we could not parse at all, which is a genuinely different fix for the
// client.
func WriteValidationError(ctx context.Context, w http.ResponseWriter, fields []FieldError) {
	WriteJSON(w, http.StatusUnprocessableEntity, ErrorResponse{Error: ErrorBody{
		Code:      CodeValidationFailed,
		Message:   "the request failed validation",
		Fields:    fields,
		RequestID: RequestID(ctx),
	}})
}

// WriteInternalError writes a deliberately vague 500.
//
// The caller is expected to have logged the real error with the request id
// already. Leaking the underlying message here is how database schemas and
// file paths end up in bug reports.
func WriteInternalError(ctx context.Context, w http.ResponseWriter) {
	WriteError(ctx, w, http.StatusInternalServerError, CodeInternalError,
		"an internal error occurred")
}
