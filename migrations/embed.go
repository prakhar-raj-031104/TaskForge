// Package migrations embeds the .sql migration files into the binary.
//
// This lives here, next to the .sql files, rather than under internal/,
// because go:embed patterns cannot reach outside the directory of the file
// that declares them — there is no "../" in an embed pattern.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
