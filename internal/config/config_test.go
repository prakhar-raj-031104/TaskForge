package config

import (
	"bytes"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"
)

// mapLookup adapts a map to LookupFunc so tests never touch real process
// environment variables. Real env vars are global mutable state shared by every
// test in the binary; injecting the lookup keeps these tests parallel-safe.
func mapLookup(m map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := load(mapLookup(nil))
	if err != nil {
		t.Fatalf("load with empty environment: unexpected error: %v", err)
	}

	if cfg.Env != EnvDevelopment {
		t.Errorf("Env = %q, want %q", cfg.Env, EnvDevelopment)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q, want %q", cfg.HTTP.Addr, ":8080")
	}
	if cfg.HTTP.MaxBodyBytes != 64<<10 {
		t.Errorf("HTTP.MaxBodyBytes = %d, want %d", cfg.HTTP.MaxBodyBytes, 64<<10)
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("Database.Port = %d, want 5432", cfg.Database.Port)
	}
	if cfg.Worker.Concurrency != 5 {
		t.Errorf("Worker.Concurrency = %d, want 5", cfg.Worker.Concurrency)
	}
	if cfg.Worker.PollInterval != time.Second {
		t.Errorf("Worker.PollInterval = %s, want 1s", cfg.Worker.PollInterval)
	}
	if cfg.IsProduction() {
		t.Error("IsProduction() = true, want false for the default environment")
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Parallel()

	cfg, err := load(mapLookup(map[string]string{
		"APP_ENV":              "production",
		"LOG_LEVEL":            "debug",
		"LOG_FORMAT":           "json",
		"HTTP_ADDR":            "127.0.0.1:9000",
		"HTTP_READ_TIMEOUT":    "2s",
		"HTTP_MAX_BODY_BYTES":  "1048576",
		"DB_HOST":              "postgres",
		"DB_PORT":              "6543",
		"DB_MAX_CONNS":         "25",
		"DB_MIN_CONNS":         "5",
		"WORKER_CONCURRENCY":   "16",
		"WORKER_POLL_INTERVAL": "250ms",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !cfg.IsProduction() {
		t.Error("IsProduction() = false, want true")
	}
	if cfg.Log.Format != "json" {
		t.Errorf("Log.Format = %q, want json", cfg.Log.Format)
	}
	if cfg.HTTP.Addr != "127.0.0.1:9000" {
		t.Errorf("HTTP.Addr = %q", cfg.HTTP.Addr)
	}
	if cfg.HTTP.ReadTimeout != 2*time.Second {
		t.Errorf("HTTP.ReadTimeout = %s, want 2s", cfg.HTTP.ReadTimeout)
	}
	if cfg.HTTP.MaxBodyBytes != 1<<20 {
		t.Errorf("HTTP.MaxBodyBytes = %d, want %d", cfg.HTTP.MaxBodyBytes, 1<<20)
	}
	if cfg.Database.Port != 6543 {
		t.Errorf("Database.Port = %d, want 6543", cfg.Database.Port)
	}
	if cfg.Worker.PollInterval != 250*time.Millisecond {
		t.Errorf("Worker.PollInterval = %s, want 250ms", cfg.Worker.PollInterval)
	}
}

// TestLoadEmptyValueFallsBackToDefault documents a deliberate decision: a
// variable set to the empty string is treated as unset.
func TestLoadEmptyValueFallsBackToDefault(t *testing.T) {
	t.Parallel()

	cfg, err := load(mapLookup(map[string]string{"DB_HOST": "   "}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Database.Host != "localhost" {
		t.Errorf("Database.Host = %q, want the default localhost", cfg.Database.Host)
	}
}

func TestLoadInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		wantKey string
	}{
		{"non-numeric port", map[string]string{"DB_PORT": "not-a-number"}, "DB_PORT"},
		{"port out of range", map[string]string{"DB_PORT": "70000"}, "DB_PORT"},
		{"bad duration", map[string]string{"HTTP_READ_TIMEOUT": "5 seconds"}, "HTTP_READ_TIMEOUT"},
		{"zero duration", map[string]string{"WORKER_POLL_INTERVAL": "0s"}, "WORKER_POLL_INTERVAL"},
		{"unknown log level", map[string]string{"LOG_LEVEL": "verbose"}, "LOG_LEVEL"},
		{"unknown environment", map[string]string{"APP_ENV": "prod"}, "APP_ENV"},
		{"unknown sslmode", map[string]string{"DB_SSLMODE": "maybe"}, "DB_SSLMODE"},
		{"zero max conns", map[string]string{"DB_MAX_CONNS": "0"}, "DB_MAX_CONNS"},
		{"min above max", map[string]string{"DB_MIN_CONNS": "50", "DB_MAX_CONNS": "10"}, "DB_MIN_CONNS"},
		{"zero concurrency", map[string]string{"WORKER_CONCURRENCY": "0"}, "WORKER_CONCURRENCY"},
		{"negative body limit", map[string]string{"HTTP_MAX_BODY_BYTES": "-1"}, "HTTP_MAX_BODY_BYTES"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := load(mapLookup(tt.env))
			if err == nil {
				t.Fatalf("load(%v) = nil error, want an error mentioning %s", tt.env, tt.wantKey)
			}
			if !strings.Contains(err.Error(), tt.wantKey) {
				t.Errorf("error %q does not mention %s", err, tt.wantKey)
			}
		})
	}
}

// TestLoadReportsEveryError checks that we accumulate errors instead of
// bailing out on the first one.
func TestLoadReportsEveryError(t *testing.T) {
	t.Parallel()

	_, err := load(mapLookup(map[string]string{
		"DB_PORT":   "abc",
		"LOG_LEVEL": "verbose",
	}))
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	for _, key := range []string{"DB_PORT", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not mention %s; errors are not being accumulated", err, key)
		}
	}
}

func TestDatabaseDSNRoundTrips(t *testing.T) {
	t.Parallel()

	const password = `p@ss w/ord:#1`

	db := Database{
		Host:           "db.internal",
		Port:           6543,
		User:           "taskforge",
		Password:       password,
		Name:           "taskforge",
		SSLMode:        "require",
		ConnectTimeout: 7 * time.Second,
	}

	u, err := url.Parse(db.DSN())
	if err != nil {
		t.Fatalf("DSN is not a parseable URL: %v", err)
	}
	if u.Scheme != "postgres" {
		t.Errorf("scheme = %q, want postgres", u.Scheme)
	}
	if u.Host != "db.internal:6543" {
		t.Errorf("host = %q, want db.internal:6543", u.Host)
	}
	if u.Path != "/taskforge" {
		t.Errorf("path = %q, want /taskforge", u.Path)
	}
	if got := u.User.Username(); got != "taskforge" {
		t.Errorf("username = %q, want taskforge", got)
	}
	// The password contains characters that must be percent-encoded. If we
	// built the DSN by string concatenation this assertion would fail.
	if got, _ := u.User.Password(); got != password {
		t.Errorf("password did not survive encoding: got %q, want %q", got, password)
	}
	if got := u.Query().Get("sslmode"); got != "require" {
		t.Errorf("sslmode = %q, want require", got)
	}
	if got := u.Query().Get("connect_timeout"); got != "7" {
		t.Errorf("connect_timeout = %q, want 7", got)
	}
}

func TestDatabaseRedactedDSNHidesPassword(t *testing.T) {
	t.Parallel()

	db := Database{
		Host: "localhost", Port: 5432,
		User: "taskforge", Password: "super-secret-value",
		Name: "taskforge", SSLMode: "disable",
	}

	redacted := db.RedactedDSN()
	if strings.Contains(redacted, "super-secret-value") {
		t.Fatalf("RedactedDSN leaked the password: %s", redacted)
	}
	if !strings.Contains(redacted, "taskforge") {
		t.Errorf("RedactedDSN dropped the username: %s", redacted)
	}
}

// TestLogValueNeverLeaksPassword is the test that matters most in this file: it
// asserts that logging the entire Config cannot print the database password.
func TestLogValueNeverLeaksPassword(t *testing.T) {
	t.Parallel()

	cfg, err := load(mapLookup(map[string]string{"DB_PASSWORD": "super-secret-value"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("startup", "config", cfg)

	if strings.Contains(buf.String(), "super-secret-value") {
		t.Fatalf("the password reached the log output: %s", buf.String())
	}
	if !strings.Contains(buf.String(), `"port":5432`) {
		t.Errorf("expected database fields in the log output, got: %s", buf.String())
	}
}
