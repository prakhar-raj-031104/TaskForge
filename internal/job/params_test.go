package job

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// fixedNow keeps scheduling assertions independent of when the test runs.
var fixedNow = time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

// fieldNames extracts the fields a ValidationError complained about, so tests
// assert on structure rather than on message wording.
func fieldNames(t *testing.T, err error) []string {
	t.Helper()

	var v *ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("error %v is not a *ValidationError", err)
	}
	names := make([]string, 0, len(v.Fields))
	for _, f := range v.Fields {
		names = append(names, f.Field)
	}
	return names
}

func TestCreateParamsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params CreateParams
	}{
		{"bare minimum", CreateParams{Type: "send_email"}},
		{"dotted type", CreateParams{Type: "email.welcome.v2"}},
		{"hyphenated type", CreateParams{Type: "send-email"}},
		{"digits in type", CreateParams{Type: "resize_image_2x"}},
		{"full request", CreateParams{
			Type:           "send_email",
			Payload:        json.RawMessage(`{"to":"a@b.com"}`),
			Priority:       ptr(100),
			MaxRetries:     ptr(0),
			ScheduledAt:    ptr(fixedNow.Add(time.Hour)),
			IdempotencyKey: ptr("order-123"),
		}},
		{"lowest priority", CreateParams{Type: "sleep", Priority: ptr(MinPriority)}},
		{"highest priority", CreateParams{Type: "sleep", Priority: ptr(MaxPriority)}},
		{"scheduled in the past is allowed", CreateParams{
			Type:        "sleep",
			ScheduledAt: ptr(fixedNow.Add(-24 * time.Hour)),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.params.Validate(fixedNow); err != nil {
				t.Errorf("Validate returned %v, want nil", err)
			}
		})
	}
}

func TestCreateParamsInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		params     CreateParams
		wantFields []string
	}{
		{"missing type", CreateParams{}, []string{"type"}},
		{"blank type", CreateParams{Type: "   "}, []string{"type"}},
		{"uppercase type", CreateParams{Type: "SendEmail"}, []string{"type"}},
		{"type with spaces", CreateParams{Type: "send email"}, []string{"type"}},
		{"type starting with a digit", CreateParams{Type: "2fast"}, []string{"type"}},
		{"type too long", CreateParams{Type: "a" + strings.Repeat("b", MaxTypeLength)}, []string{"type"}},

		{"payload is an array", CreateParams{
			Type: "sleep", Payload: json.RawMessage(`[1,2,3]`),
		}, []string{"payload"}},
		{"payload is a scalar", CreateParams{
			Type: "sleep", Payload: json.RawMessage(`42`),
		}, []string{"payload"}},
		{"payload is malformed", CreateParams{
			Type: "sleep", Payload: json.RawMessage(`{"a":`),
		}, []string{"payload"}},

		{"priority below range", CreateParams{Type: "sleep", Priority: ptr(0)}, []string{"priority"}},
		{"priority above range", CreateParams{Type: "sleep", Priority: ptr(101)}, []string{"priority"}},
		{"negative priority", CreateParams{Type: "sleep", Priority: ptr(-5)}, []string{"priority"}},

		{"negative retries", CreateParams{Type: "sleep", MaxRetries: ptr(-1)}, []string{"max_retries"}},
		{"retries above cap", CreateParams{Type: "sleep", MaxRetries: ptr(MaxMaxRetries + 1)}, []string{"max_retries"}},

		{"scheduled too far ahead", CreateParams{
			Type: "sleep", ScheduledAt: ptr(fixedNow.Add(MaxScheduleAhead + time.Hour)),
		}, []string{"scheduled_at"}},

		{"blank idempotency key", CreateParams{
			Type: "sleep", IdempotencyKey: ptr("   "),
		}, []string{"idempotency_key"}},
		{"idempotency key too long", CreateParams{
			Type: "sleep", IdempotencyKey: ptr(strings.Repeat("k", MaxIdempotencyKeyLength+1)),
		}, []string{"idempotency_key"}},

		// The important one: every problem is reported in a single response.
		{"several problems at once", CreateParams{
			Type:       "Bad Type",
			Payload:    json.RawMessage(`[]`),
			Priority:   ptr(500),
			MaxRetries: ptr(-3),
		}, []string{"type", "payload", "priority", "max_retries"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.params.Validate(fixedNow)
			if err == nil {
				t.Fatalf("Validate returned nil, want errors on %v", tt.wantFields)
			}

			got := fieldNames(t, err)
			if len(got) != len(tt.wantFields) {
				t.Fatalf("reported fields %v, want %v", got, tt.wantFields)
			}
			for i, want := range tt.wantFields {
				if got[i] != want {
					t.Errorf("field[%d] = %q, want %q", i, got[i], want)
				}
			}
		})
	}
}

func TestListFilterValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    ListFilter
		wantField string
	}{
		{"empty is valid", ListFilter{}, ""},
		{"known statuses", ListFilter{Statuses: []Status{StatusPending, StatusRunning}}, ""},
		{"at the limit cap", ListFilter{Limit: MaxListLimit}, ""},
		{"unknown status", ListFilter{Statuses: []Status{"exploded"}}, "status"},
		{"limit above cap", ListFilter{Limit: MaxListLimit + 1}, "limit"},
		{"negative limit", ListFilter{Limit: -1}, "limit"},
		{"offset above cap", ListFilter{Offset: MaxListOffset + 1}, "offset"},
		{"negative offset", ListFilter{Offset: -1}, "offset"},
		{"invalid type", ListFilter{Type: "Not A Type"}, "type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.filter.Validate()
			if tt.wantField == "" {
				if err != nil {
					t.Fatalf("Validate returned %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate returned nil, want an error on %q", tt.wantField)
			}
			if got := fieldNames(t, err); got[0] != tt.wantField {
				t.Errorf("field = %q, want %q", got[0], tt.wantField)
			}
		})
	}
}

func TestListFilterNormalize(t *testing.T) {
	t.Parallel()

	if got := (ListFilter{}).Normalize(); got.Limit != DefaultListLimit {
		t.Errorf("limit = %d, want the default %d", got.Limit, DefaultListLimit)
	}
	if got := (ListFilter{Limit: 7}).Normalize(); got.Limit != 7 {
		t.Errorf("limit = %d, want the caller's 7", got.Limit)
	}
}
