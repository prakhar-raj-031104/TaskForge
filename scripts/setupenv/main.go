// Command setupenv copies .env.example to .env when .env does not already
// exist. It is a five-line shell command on Unix, but `cp` does not exist on
// Windows cmd.exe — writing it in Go keeps `make setup` identical on every
// machine, which is the same reason the whole project ships as containers.
package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

const (
	source = ".env.example"
	target = ".env"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "setupenv: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Never clobber an existing .env: it holds local credentials the developer
	// may have edited.
	if _, err := os.Stat(target); err == nil {
		fmt.Printf("%s already exists, leaving it untouched\n", target)
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("checking %s: %w", target, err)
	}

	data, err := os.ReadFile(source)
	if err != nil {
		return fmt.Errorf("reading %s: %w", source, err)
	}
	if err := os.WriteFile(target, data, 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", target, err)
	}

	fmt.Printf("created %s from %s\n", target, source)
	return nil
}
