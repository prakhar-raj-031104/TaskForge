package httpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// ServerConfig describes the timeouts a server should enforce.
type ServerConfig struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

// NewServer builds an *http.Server with every timeout populated.
//
// Go's zero-value http.Server has NO timeouts at all. A single client that
// opens a connection and never sends a byte occupies a goroutine and a file
// descriptor indefinitely; a few thousand of them is a denial of service that
// costs the attacker nothing. This is the most commonly skipped line in Go web
// tutorials and the most commonly regretted one in production.
func NewServer(cfg ServerConfig, h http.Handler, log *slog.Logger) *http.Server {
	return &http.Server{
		Addr:    cfg.Addr,
		Handler: h,

		// Time to read the request headers. Specifically defends against
		// Slowloris, where a client dribbles headers out one byte at a time.
		ReadHeaderTimeout: cfg.ReadTimeout,
		// Time to read the entire request, headers plus body.
		ReadTimeout: cfg.ReadTimeout,
		// Time to write the response, measured from the end of header read.
		WriteTimeout: cfg.WriteTimeout,
		// How long an idle keep-alive connection is kept around.
		IdleTimeout: cfg.IdleTimeout,

		// Route the server's own errors through slog instead of the standard
		// logger, so they land in the same structured stream as everything
		// else rather than as unformatted text on stderr.
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelError),
	}
}

// Serve runs srv until ctx is cancelled, then shuts it down gracefully.
//
// The shape here — start the blocking call in a goroutine, then select on
// either its result or context cancellation — is the standard Go answer to
// "wait for whichever happens first". ListenAndServe blocks forever, so it
// cannot be the thing that also watches for a signal.
func Serve(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration, log *slog.Logger) error {
	// Buffered with capacity 1 so the goroutine can always send and exit, even
	// if nobody is left reading. With an unbuffered channel the goroutine would
	// block forever on send after a shutdown, leaking for the life of the
	// process.
	serveErr := make(chan error, 1)

	go func() {
		log.Info("http server listening", "addr", srv.Addr)
		err := srv.ListenAndServe()
		// Shutdown makes ListenAndServe return ErrServerClosed. That is the
		// success path, not a failure.
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	select {
	case err := <-serveErr:
		// The server stopped on its own, which at this point means it never
		// started: the port was already in use, or binding was refused.
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil

	case <-ctx.Done():
		log.Info("shutdown signal received, draining connections",
			"timeout", shutdownTimeout)
	}

	// A fresh context, deliberately NOT derived from ctx. ctx is already
	// cancelled — that is why we are here — and a shutdown context derived from
	// a cancelled parent is born cancelled, turning "drain gracefully" into
	// "drop every in-flight request".
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Shutdown stops accepting new connections, then waits for active requests
	// to finish. It does not interrupt a handler mid-flight, which is why
	// handlers must respect their own context deadlines.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown exceeded its deadline, closing forcibly",
			"error", err)
		if closeErr := srv.Close(); closeErr != nil {
			return fmt.Errorf("force close after failed shutdown: %w", closeErr)
		}
		return fmt.Errorf("http server shutdown: %w", err)
	}

	// Shutdown has returned, so ListenAndServe has returned too; collect its
	// result so the goroutine is definitively finished before we report success.
	if err := <-serveErr; err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	log.Info("http server stopped cleanly")
	return nil
}
