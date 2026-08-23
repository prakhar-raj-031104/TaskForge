package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
)

// TypeSendEmail is the registered job type for SendEmail.
const TypeSendEmail = "send_email"

// emailPayload is the expected job payload:
//
//	{"to": "user@example.com", "subject": "Hello", "body": "..."}
type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// Sender delivers an email.
//
// Declaring this interface here, in the consuming package, means the handler
// can be tested with a two-line fake and that swapping SMTP for SES or Postmark
// touches one type. It is one method because that is all the handler needs.
type Sender interface {
	Send(ctx context.Context, to, subject, body string) error
}

// SendEmail delivers email jobs through a Sender.
type SendEmail struct {
	sender Sender
}

// NewSendEmail builds the handler.
func NewSendEmail(sender Sender) *SendEmail {
	return &SendEmail{sender: sender}
}

// Handle implements job.Handler.
func (h *SendEmail) Handle(ctx context.Context, j *job.Job) error {
	var p emailPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return job.Permanent(fmt.Errorf("invalid send_email payload: %w", err))
	}

	// Validate before calling out. An address that is malformed now will be
	// malformed on every retry, so it is a permanent failure, and catching it
	// here avoids four pointless round trips to the mail provider.
	if strings.TrimSpace(p.To) == "" {
		return job.Permanent(fmt.Errorf("field \"to\" is required"))
	}
	if _, err := mail.ParseAddress(p.To); err != nil {
		return job.Permanent(fmt.Errorf("field \"to\" is not a valid email address: %w", err))
	}
	if strings.TrimSpace(p.Subject) == "" {
		return job.Permanent(fmt.Errorf("field \"subject\" is required"))
	}

	if err := h.sender.Send(ctx, p.To, p.Subject, p.Body); err != nil {
		// Transport failures keep their retries: the provider may simply be
		// having a bad minute.
		return fmt.Errorf("sending email: %w", err)
	}

	return nil
}

// LogSender is a Sender that logs instead of sending.
//
// This project is about queue mechanics, not SMTP. Wiring a real provider would
// add credentials, a vendor SDK and a delivery sandbox without teaching anything
// about job processing. The interface above is the seam where a real
// implementation drops in.
type LogSender struct {
	// Latency simulates provider round-trip time so the worker's concurrency
	// and duration metrics show realistic numbers.
	Latency time.Duration
}

// Send implements Sender.
func (s LogSender) Send(ctx context.Context, to, subject, _ string) error {
	if s.Latency > 0 {
		timer := time.NewTimer(s.Latency)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	logging.FromContext(ctx).InfoContext(ctx, "email sent (simulated)",
		// The recipient is masked. Logs are the least access-controlled surface
		// in most systems, and an aggregated log of every recipient address is
		// a personal-data problem waiting to happen.
		"to", maskEmail(to),
		"subject_length", len(subject),
	)
	return nil
}

// maskEmail turns user@example.com into u***@example.com, keeping the log
// useful for debugging without storing the full address.
func maskEmail(addr string) string {
	at := strings.LastIndex(addr, "@")
	if at <= 0 {
		return "***"
	}
	local, domain := addr[:at], addr[at:]
	if len(local) <= 1 {
		return "*" + domain
	}
	return local[:1] + "***" + domain
}
