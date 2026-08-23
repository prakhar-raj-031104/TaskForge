package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
)

// TypeWebhook is the registered job type for Webhook.
const TypeWebhook = "webhook"

// maxResponseBytes bounds what we read back from the target.
//
// A webhook target that streams an endless response would otherwise consume
// memory until the worker dies. We only need enough of the body to put a useful
// excerpt in last_error.
const maxResponseBytes = 4 << 10 // 4 KiB

// webhookPayload is the expected job payload:
//
//	{"url": "https://...", "method": "POST", "body": {...}, "headers": {...}}
type webhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Body    json.RawMessage   `json:"body"`
	Headers map[string]string `json:"headers"`
}

// WebhookOptions configures the handler.
type WebhookOptions struct {
	// Timeout bounds a single HTTP attempt. It must be shorter than the
	// worker's JobTimeout so the handler returns a useful error rather than
	// being killed by its own deadline.
	Timeout time.Duration

	// AllowPrivateTargets permits requests to loopback, link-local and RFC 1918
	// addresses.
	//
	// Defaulting this to false matters. A webhook handler takes a URL from
	// user-supplied job payload and fetches it from inside your network, which
	// is textbook server-side request forgery: a job with
	// "url": "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
	// turns your queue into a credential exfiltration tool on any cloud VM.
	// Development needs localhost targets, so it is configurable rather than
	// absolute, but production should leave it off.
	AllowPrivateTargets bool
}

// Webhook performs an outbound HTTP request.
type Webhook struct {
	client *http.Client
	opts   WebhookOptions
}

// NewWebhook builds the handler with a shared, correctly configured client.
//
// One client for the whole handler, not one per job: http.Client is safe for
// concurrent use and holds the connection pool. Creating one per request throws
// away keep-alives and leaks file descriptors under load, which is one of the
// most common performance bugs in Go services.
func NewWebhook(opts WebhookOptions) *Webhook {
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if !opts.AllowPrivateTargets {
		// Control runs after DNS resolution and before the connection is made,
		// so it sees the address actually being dialled. Validating the
		// hostname instead would be bypassable by a DNS name that resolves to
		// 127.0.0.1, and by DNS rebinding between the check and the dial.
		dialer.Control = blockPrivateAddresses
	}

	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}

	return &Webhook{
		client: &http.Client{
			Transport: transport,
			Timeout:   opts.Timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// Redirects are followed but bounded, and each hop is dialled
				// through the same Control hook, so a redirect to an internal
				// address is still refused.
				if len(via) >= 3 {
					return fmt.Errorf("stopped after %d redirects", len(via))
				}
				return nil
			},
		},
		opts: opts,
	}
}

// Handle implements job.Handler.
func (h *Webhook) Handle(ctx context.Context, j *job.Job) error {
	var p webhookPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return job.Permanent(fmt.Errorf("invalid webhook payload: %w", err))
	}

	target, err := url.Parse(p.URL)
	if err != nil {
		return job.Permanent(fmt.Errorf("invalid url: %w", err))
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return job.Permanent(fmt.Errorf("url scheme must be http or https, got %q", target.Scheme))
	}

	method := strings.ToUpper(p.Method)
	if method == "" {
		method = http.MethodPost
	}

	var body io.Reader
	if len(p.Body) > 0 {
		body = bytes.NewReader(p.Body)
	}

	req, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return job.Permanent(fmt.Errorf("building request: %w", err))
	}
	if len(p.Body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		// Connection refused, DNS failure, timeout: all plausibly transient, so
		// these keep their retries. The one exception is our own SSRF guard,
		// which will never start succeeding.
		if isBlockedTarget(err) {
			return job.Permanent(err)
		}
		return fmt.Errorf("%s %s: %w", method, target.Redacted(), err)
	}
	defer func() {
		// Drain before closing so the connection can go back to the keep-alive
		// pool instead of being torn down. Closing an undrained body is a
		// silent, steady performance regression.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		_ = resp.Body.Close()
	}()

	excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))

	logging.FromContext(ctx).DebugContext(ctx, "webhook delivered",
		"status", resp.StatusCode, "host", target.Host)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil

	case resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode >= 500:
		// The target is overloaded or broken. Retrying with backoff is exactly
		// the right response.
		return fmt.Errorf("%s %s: status %d: %s",
			method, target.Redacted(), resp.StatusCode, strings.TrimSpace(string(excerpt)))

	default:
		// Any other 4xx means the request itself is wrong: bad auth, bad
		// payload, missing resource. Sending it again unchanged cannot help, so
		// dead-letter now rather than burning the retry budget and hammering
		// somebody else's API.
		return job.Permanent(fmt.Errorf("%s %s: status %d: %s",
			method, target.Redacted(), resp.StatusCode, strings.TrimSpace(string(excerpt))))
	}
}

// errBlockedTarget marks an address rejected by the SSRF guard.
var errBlockedTarget = fmt.Errorf("target address is not permitted")

func isBlockedTarget(err error) bool {
	return err != nil && strings.Contains(err.Error(), errBlockedTarget.Error())
}

// blockPrivateAddresses refuses connections to addresses that are not routable
// on the public internet.
//
// It is installed as net.Dialer.Control, which runs with the resolved address
// immediately before connect(2). Checking here rather than on the hostname is
// what makes it robust against a public DNS name that resolves to an internal
// IP.
func blockPrivateAddresses(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: cannot parse address %q", errBlockedTarget, address)
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("%w: %q is not an IP address", errBlockedTarget, host)
	}

	switch {
	case ip.IsLoopback(),
		ip.IsPrivate(),
		ip.IsLinkLocalUnicast(),
		ip.IsLinkLocalMulticast(),
		ip.IsUnspecified(),
		ip.IsMulticast():
		return fmt.Errorf("%w: %s is a private or reserved address", errBlockedTarget, ip)
	}

	return nil
}
